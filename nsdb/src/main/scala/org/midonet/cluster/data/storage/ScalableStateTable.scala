/*
 * Copyright 2016 Midokura SARL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.midonet.cluster.data.storage

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicBoolean

import scala.util.control.NonFatal

import com.typesafe.scalalogging.Logger

import org.apache.curator.framework.state.{ConnectionState => StorageConnectionState}
import org.apache.zookeeper.KeeperException.{Code, ConnectionLossException}
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent, Watcher => KeeperWatcher}
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

import rx.{Observable, Subscriber}
import rx.Observable.OnSubscribe
import rx.observers.SafeSubscriber
import rx.subscriptions.Subscriptions

import org.midonet.cluster.backend.{Directory, DirectoryCallback}
import org.midonet.cluster.data.storage.ScalableStateTable._
import org.midonet.cluster.data.storage.StateTable.{Key, Update}
import org.midonet.cluster.rpc.State.ProxyResponse.Notify
import org.midonet.cluster.services.state.client.{StateSubscriptionKey, StateTableClient}
import org.midonet.cluster.services.state.client.StateTableClient.ConnectionState.{ConnectionState => ProxyConnectionState}
import org.midonet.util.reactivex.SubscriptionList
import org.midonet.util.functors.makeAction0

object ScalableStateTable {

    final val Log =
        Logger(LoggerFactory.getLogger("org.midonet.nsdb.state-table"))

    final val PersistentVersion = Int.MaxValue

    /**
      * A state table entry.
      *
      * @param key The entry key.
      * @param value The entry value.
      * @param version The entry version corresponding to the ephemeral sequential
      *                number.
      */
    private case class TableEntry[K, V](key: K, value: V, version: Int)

    /**
      * A key-value pair.
      */
    private case class KeyValue[K, V](key: K, value: V)

    /**
      * A subscriber that handles any exceptions thrown by a [[SafeSubscriber]].
      */
    private class ProtectedSubscriber[K, V](tableKey: Key,
                                            child: Subscriber[_ >: Update[K, V]])
        extends SafeSubscriber[Update[K, V]](child) {

        override def onCompleted(): Unit = {
            try super.onCompleted()
            catch {
                case NonFatal(e) =>
                    Log.debug(s"[$tableKey] Exception during onCompleted", e)
            }
        }

        override def onError(e: Throwable): Unit = {
            try super.onError(e)
            catch {
                case NonFatal(e2) =>
                    Log.debug(s"[$tableKey] Exception during onError: $e", e2)
            }
        }

        def contains(inner: Subscriber[_ >: Update[K, V]]): Boolean = {
            child eq inner
        }
    }

    /**
      * Provides a [[Subscriber]] singleton that does nothing and it is by
      * default unsubscribed.
      */
    private object EmptySubscriber extends Subscriber[Notify.Update] {
        unsubscribe()

        override def onNext(update: Notify.Update): Unit = { }

        override def onCompleted(): Unit = { }

        override def onError(e: Throwable): Unit = { }
    }

}

/**
  * A trait for a [[StateTable]] that provides dual backend support for both
  * [[Directory]] read-write operations and [[StateTableClient]] read
  * operations.
  */
trait ScalableStateTable[K, V] extends StateTable[K, V] with StateTableEncoder[K, V] {

    /**
      * Implements the [[OnSubscribe]] interface for subscriptions to updates
      * from this state table. For every new subscription, we start the state
      * table, and subscribe to the table via the underlying state proxy client
      * with fallback on the directory.
      */
    private class OnTableSubscribe extends OnSubscribe[Update[K, V]] {

        override def call(child: Subscriber[_ >: Update[K, V]]): Unit = {
            sync.synchronized {
                child.add(Subscriptions.create(makeAction0 {
                    stopInternal(0, child)
                }))
                if (!child.isUnsubscribed) {
                    startInternal(0).call(child)
                }
            }
        }
    }

    /**
      * Completes the addition of an entry to the state table.
      */
    private class AddCallback(state: State) extends DirectoryCallback[String] {

        override def onSuccess(path: String, stat: Stat, context: Object): Unit = {
            processAddCallback(state, path, context)
        }

        override def onError(e: KeeperException, context: Object): Unit = {
            processAddError(state, e, context)
        }
    }

    /**
      * Completes the listing of the current table entries.
      */
    private class GetCallback(state: State)
        extends DirectoryCallback[util.Collection[String]] {

        override def onSuccess(entries: util.Collection[String], stat: Stat,
                               context: Object): Unit = {
            processGetCallback(state, entries, stat, context)
        }

        override def onError(e: KeeperException, context: Object): Unit = {
            processGetError(state, e, context)
        }
    }

    /**
      * Completes the deletion of an existing entry.
      */
    private class DeleteCallback(state: State) extends DirectoryCallback[Void] {

        override def onSuccess(arg: Void, stat: Stat, context: Object): Unit = {
            processDeleteCallback(state, context)
        }

        override def onError(e: KeeperException, context: Object): Unit = {
            processDeleteError(state, e, context)
        }
    }

    /**
      * Handles [[KeeperWatcher]] notifications when the storage directory for
      * this state table has changed.
      */
    private class Watcher(state: State) extends KeeperWatcher {
        override def process(event: WatchedEvent): Unit = {
            processWatcher(state, event)
        }
    }

    /**
      * Handles changes to the storage connection state.
      */
    private class StorageConnectionSubscriber(state: State)
        extends Subscriber[StorageConnectionState] {

        override def onNext(connectionState: StorageConnectionState): Unit = {
            processStorageConnection(state, connectionState)
        }

        override def onError(e: Throwable): Unit = {
            Log.error(s"[$tableKey] Unexpected error ${e.getMessage} on the " +
                      "storage connection observable", e)
            close(e)
        }

        override def onCompleted(): Unit = {
            Log.warn(s"[$tableKey] Unexpected completion of the storage " +
                     s"connection observable: closing table")
            close(e = null)
        }
    }

    /**
      * Handles changes to the state proxy connection state.
      */
    private class ProxyConnectionSubscriber(state: State)
        extends Subscriber[ProxyConnectionState] {

        override def onNext(connectionState: ProxyConnectionState): Unit = {
	    Log.info(s"[$tableKey] onNext Connection state = $connectionState")
            processProxyConnection(state, connectionState)
        }

        override def onError(e: Throwable): Unit = {
            Log.error(s"[$tableKey] Unexpected error ${e.getMessage} on the " +
                      "proxy connection observable", e)
            close(e)
        }

        override def onCompleted(): Unit = {
            Log.warn(s"[$tableKey] Unexpected completion of the proxy " +
                     "connection observable: closing table")
            close(e = null)
        }
    }

    /**
      * The subscriber to the state proxy client for the current state table.
      */
    private class ProxySubscriber(state: State) extends Subscriber[Notify.Update] {

        override def onNext(update: Notify.Update): Unit = {
            processProxyUpdate(state, update)
        }

        override def onError(e: Throwable): Unit = {
            processProxyError(state, e)
        }

        override def onCompleted(): Unit = {
            processProxyCompletion(state)
        }
    }

    /**
      * Represents the state for this state table, encapsulating the storage
      * callback, watcher and connection subscriber, and the proxy subscriber.
      */
    private class State extends SubscriptionList[Update[K, V]] {

        private val addCallback = new AddCallback(this)
        private val getCallback = new GetCallback(this)
        private val deleteCallback = new DeleteCallback(this)
        private val watcher = new Watcher(this)

        private val storageConnectionSubscriber = new StorageConnectionSubscriber(this)
        private val proxyConnectionSubscriber = new ProxyConnectionSubscriber(this)

        @volatile private var proxySubscriber: Subscriber[Notify.Update] =
            EmptySubscriber

        private val storageConnectedFlag = new AtomicBoolean(true)
        private val proxyConnectedFlag = new AtomicBoolean(false)

        private val cache = new ConcurrentHashMap[K, TableEntry[K, V]]
        private var version = -1L
        private val failures = new ConcurrentLinkedQueue[TableEntry[K, V]]()

        private val updateCache = new util.HashMap[K, TableEntry[K, V]](64)
        private val updates = new util.ArrayList[Update[K, V]](64)
        private val removals = new ThreadLocal[util.List[TableEntry[K, V]]] {
            override def initialValue = new util.ArrayList[TableEntry[K, V]](16)
        }

        private val adding = new util.HashSet[KeyValue[K, V]](4)
        private val removing = new util.HashSet[KeyValue[K, V]](4)
        private val owned = new util.HashSet[Int]()

        private var snapshotInProgress = false

        /**
          * Starts the current state by monitoring the connection for the
          * underlying storage and state-proxy client.
          */
        @throws[IllegalStateException]
        def start(): Unit = {
            Log.debug(s"[$tableKey] - state.start called")
            if (get().terminated) {
                Log.debug(s"[$tableKey] - state.start terminated exception")
                throw new IllegalStateException(s"[$tableKey] State closed")
            }

            Log.debug(s"[$tableKey] - state.start with proxy='$proxy'")
            proxy.connection.subscribe(proxyConnectionSubscriber)
            connection.subscribe(storageConnectionSubscriber)
            refresh()
        }

        /**
          * Stops the current state. If there are any subscribers, their
          * notification stream is completed.
          */
        def stop(e: Throwable): Unit = {
            if (get().terminated) {
                return
            }

            // Complete all subscribers.
            val subs = terminate()
            var index = 0
            while (index < subs.length) {
                if (e ne null) {
                    subs(index).onError(e)
                } else {
                    subs(index).onCompleted()
                }
                index += 1
            }

            proxySubscriber.unsubscribe()
            proxyConnectionSubscriber.unsubscribe()
            storageConnectionSubscriber.unsubscribe()
            proxySubscriber = null
            cache.clear()
            failures.clear()
            version = Long.MaxValue
        }

        /**
          * Begins an asynchronous add operation of a new entry to the state
          * table. The method verifies that an entry for the same key-value
          * pair does not already exists or is in the process of being added.
          */
        def add(key: K, value: V): Unit = {
            Log trace s"[$tableKey] Add $key -> $value"

            val path = encodeEntryPrefix(key, value)
            val keyValue = KeyValue(key, value)
            if (storageConnectedFlag.get()) {
                this.synchronized {
                    // Do not add the key-value pair if the same key-value is:
                    // (i) in the process of being added, or (ii)
                    if (adding.contains(keyValue)) {
                        Log debug s"[$tableKey] Already adding $key -> $value"
                        return
                    }
                    val entry = cache.get(key)
                    if ((entry ne null) && entry.value == value &&
                         owned.contains(entry.version) &&
                         !removing.contains(keyValue)) {
                        Log debug s"[$tableKey] Entry $key -> $value exists"
                        return
                    }
                    adding.add(keyValue)
                }
                directory.asyncAdd(path, null, CreateMode.EPHEMERAL_SEQUENTIAL,
                                   addCallback, keyValue)
            } else {
                Log warn s"[$tableKey] Add $key -> $value failed: not connected"
            }
        }

        /**
          * Completes an asynchronous add operation of a new entry to the state
          * table cache. The method puts the added entry to the local cache if
          * it can overwrite any existing entry for the same key. It also clears
          * the entry from the adding map. This requires a lock to synchronize
          * the cache modification.
          */
        def addComplete(entry: TableEntry[K, V], keyValue: KeyValue[K, V]): Unit = {
            Log trace s"[$tableKey] Add ${entry.key} -> ${entry.value} " +
                      s"completed"

            val removeEntry = this.synchronized {
                val oldEntry = cache.get(entry.key)
                val removeEntry = if (oldEntry eq null) {
                    cache.put(entry.key, entry)
                    publish(Update(entry.key, nullValue, entry.value))

                    Log trace s"[$tableKey] Added ${entry.key} -> ${entry.value} " +
                              s"version:${entry.version}"
                    null
                } else if (oldEntry.version == PersistentVersion ||
                           oldEntry.version < entry.version) {
                    cache.put(entry.key, entry)
                    publish(Update(entry.key, oldEntry.value, entry.value))

                    Log trace s"[$tableKey] Updated ${entry.key} -> ${entry.value} " +
                              s"from version:${oldEntry.version} to " +
                              s"version:${entry.version}"

                    if (owned.contains(oldEntry.version)) {
                        oldEntry
                    } else {
                        null
                    }
                } else {
                    Log trace s"[$tableKey] Ignore key:${entry.key} " +
                              s"value:${entry.value}"
                    null
                }

                owned.add(entry.version)
                adding.remove(keyValue)
                removeEntry
            }
            delete(removeEntry)
        }

        /**
          * Completes an asynchronous add operation that finished with an error.
          */
        def addError(e: KeeperException, keyValue: KeyValue[K, V]): Unit = {
            // If an add operation fails, we cannot retry since we cannot
            // guarantee the order with respect to subsequent operations.
            Log.warn(s"Add ${e.getPath} failed code: ${e.code.intValue()}", e)

            this.synchronized {
                adding.remove(keyValue)
            }
        }

        /**
          * Begins an asynchronous remove operation for an entry in the state
          * table cache. Removal is allowed only if the current table has
          * previously added the value, and if the value matches the argument
          * value.
          */
        def remove(key: K, value: V): V = this.synchronized {
            Log trace s"[$tableKey] Remove $key -> $value"

            val entry = cache.get(key)
            if (entry eq null) {
                nullValue
            } else if (entry.version < 0) {
                nullValue
            } else if (value != nullValue && value != entry.value) {
                nullValue
            } else if (!owned.contains(entry.version)) {
                nullValue
            } else {
                val keyValue = KeyValue(key, entry.value)
                adding.remove(keyValue)
                removing.add(keyValue)
                delete(entry)
                entry.value
            }
        }

        /**
          * Completes the removal of the specified entry.
          */
        def removeComplete(entry: TableEntry[K, V]): Unit = this.synchronized {
            Log trace s"[$tableKey] Remove ${entry.key} -> ${entry.value} " +
                      s"version:${entry.version} completed"

            if (cache.remove(entry.key, entry)) {
                publish(Update(entry.key, entry.value, nullValue))
            }
            owned.remove(entry.version)
            removing.remove(KeyValue(entry.key, entry.version))
        }

        /**
          * @return True if the table contains the specified key.
          */
        def containsKey(key: K): Boolean = {
            cache.containsKey(key)
        }

        /**
          * @return True if the table contains the specified key-value entry.
          */
        def contains(key: K, value: V): Boolean = {
            val entry = cache.get(key)
            (entry ne null) && entry.value == value
        }

        /**
          * @return The value for the specified key, or [[nullValue]] if the
          *         key does not exist.
          */
        def get(key: K): V = {
            val entry = cache.get(key)
            if (entry ne null) {
                entry.value
            } else {
                nullValue
            }
        }

        /**
          * @return The set of keys for the specified value.
          */
        def getByValue(value: V): Set[K] = {
            val iterator = cache.entrySet().iterator()
            val set = Set.newBuilder[K]
            while (iterator.hasNext) {
                val entry = iterator.next()
                if (entry.getValue.value == value) {
                    set += entry.getKey
                }
            }
            set.result()
        }

        /**
          * Updates the state table cache with the given list of entries. The
          * method computes the difference with respect to the current snapshot
          * and (i) updates the cache with the new values, (ii) deletes the
          * remove value added by this table that are no longer part of this
          * map, and (iii) notifies all changes to the table subscribers.
          */
        def update(entries: util.Collection[String], ver: Long): Unit = {
            if (get().terminated) {
                return
            }

            Log trace s"[$tableKey] Entries updated with version:$ver"

            // Ignore updates that are older than the current cache version.
            if (ver < version) {
                Log warn s"[$tableKey] Ignore storage update version:$ver " +
                         s"previous to version:$version"
                return
            }

            val rem = removals.get
            rem.clear()

            this.synchronized {

                version = ver

                updateCache.clear()
                updates.clear()

                // A storage update invalidates any in-progress snapshot: this
                // will discard all subsequent notifications to ensure we do
                // not load an incomplete table.
                snapshotInProgress = false

                val entryIterator = entries.iterator()
                while (entryIterator.hasNext) {
                    val nextEntry = decodeEntry(entryIterator.next())
                    // Ignore entries that cannot be decoded.
                    if (nextEntry != null) {
                        // Verify if there are multiple entries for the same key
                        // and if so select the greatest version learned entry.
                        val prevEntry = updateCache.get(nextEntry.key)
                        if ((prevEntry eq null) ||
                            prevEntry.version == PersistentVersion ||
                            (prevEntry.version < nextEntry.version &&
                             nextEntry.version != PersistentVersion)) {

                            updateCache.put(nextEntry.key, nextEntry)
                        }
                    }
                }

                // Compute the added entries.
                computeAddedEntries(rem)

                // Compute the removed entries.
                computeRemovedEntries(rem)

                // Publish updates.
                publishUpdates()
            }

            deleteEntries(rem)
        }

        /**
          * @return A snapshot of this state table.
          */
        def snapshot: Map[K, V] = {
            val iterator = cache.entrySet().iterator()
            val map = Map.newBuilder[K, V]
            while (iterator.hasNext) {
                val entry = iterator.next()
                map += entry.getKey -> entry.getValue.value
            }
            map.result()
        }

        /**
          * Called when the backend storage is connected.
          */
        def storageConnected(): Unit = {
            storageConnectedFlag set true
        }

        /**
          * Called when the backend storage is disconnected.
          */
        def storageDisconnected(): Unit = {
            storageConnectedFlag set false
        }

        /**
          * Called when the backend storage is reconnected. The method
          * refreshes the state table cache and retries any previously failed
          * operations.
          */
        def storageReconnected(): Unit = {
            if (storageConnectedFlag.compareAndSet(false, true)) {
                refresh()
                retry()
            }
        }

        /**
          * Called when the state proxy is connected. This will create a new
          * subscriber to the state proxy client for the current table and
          * table version number.
          */
        def proxyConnected(): Unit = {
            if (proxyConnectedFlag.compareAndSet(false, true)) {
                if (proxySubscriber.isUnsubscribed) {
                    proxySubscriber = new ProxySubscriber(this)
                    proxy.observable(StateSubscriptionKey(tableKey, Some(version)))
                         .subscribe(proxySubscriber)
                }
            }
        }

        /**
          * Called when the state proxy is disconnected. At this point the state
          * table falls-back and begins synchronizing with the storage backend.
          */
        def proxyDisconnected(): Unit = {
            proxyConnectedFlag set false
            proxySubscriber.unsubscribe()

            if (storageConnectedFlag.get()) {
                refresh()
            }
        }

        /**
          * Processes a state table update received from the state proxy server,
          * either a snapshot or a relative update.
          */
        def proxyUpdate(update: Notify.Update): Unit = {
            // Validate the message.
            if (!update.hasType) {
                Log info s"[$tableKey] State proxy update missing type"
            }
            if (!update.hasCurrentVersion) {
                Log info s"[$tableKey] State proxy update missing version"
            }

            val rem = removals.get
            rem.clear()

            this.synchronized {
                // Drop all messages that are previous to the current version.
                if (update.getCurrentVersion < version) {
                    Log info s"[$tableKey] Ignoring state proxy update version " +
                             s"${update.getCurrentVersion} previous to " +
                             s"$version"
                    return
                }

                update.getType match {
                    case Notify.Update.Type.SNAPSHOT =>
                        proxySnapshot(update, rem)
                    case Notify.Update.Type.RELATIVE =>
                        proxyRelative(update, rem)
                    case _ => // Ignore
                }
            }

            deleteEntries(rem)
        }

        /**
          * Processes an error from the state proxy client.
          */
        def proxyError(e: Throwable): Unit = {
            // TODO: Must update with more relevant exceptions.
            Log debug s"[$tableKey] Proxy error: $e"
            proxyDisconnected()
        }

        /**
          * Refreshes the state table cache using data from storage. The method
          * does nothing if the storage is not connected or the table is
          * being synchronized using the state proxy client.
          */
        def refresh(): Unit = {
            if (!storageConnectedFlag.get() || get().terminated) {
                return
            }
            if (proxyConnectedFlag.get() && !proxySubscriber.isUnsubscribed) {
                return
            }

            val context = Long.box(System.currentTimeMillis())
            directory.asyncGetChildren("", getCallback, watcher, context)
        }

        /**
          * Handles the failure for the given context: adds the context to the
          * failures queue and if connected calls retry.
          */
        def failure(entry: TableEntry[K, V]): Unit = {
            failures.offer(entry)
            if (storageConnectedFlag.get()) {
                retry()
            }
        }

        override def call(child: Subscriber[_ >: Update[K, V]]): Unit = {
            val subscriber = new ProtectedSubscriber[K, V](tableKey, child)
            this.synchronized {
                super.call(subscriber)
            }
        }

        /**
          * @see [[SubscriptionList.start()]]
          */
        protected override def start(child: Subscriber[_ >: Update[K, V]])
        : Unit = {
            // Do nothing.
        }

        /**
          * @see [[SubscriptionList.added()]]
          */
        protected override def added(child: Subscriber[_ >: Update[K, V]])
        : Unit = {
            // The call of this method is synchronized with any update sent to
            // subscribers. Send the initial state of this state table to this
            // subscriber.

            val iterator = cache.elements()
            while (iterator.hasMoreElements && !child.isUnsubscribed) {
                val entry = iterator.nextElement()
                child onNext Update(entry.key, nullValue, entry.value)
            }
        }

        /**
          * @see [[SubscriptionList.terminated()]]
          */
        protected override def terminated(child: Subscriber[_ >: Update[K, V]])
        : Unit = {
            child onError new IllegalStateException(s"Table $tableKey stopped")
        }

        /**
          * Deletes from storage an obsolete table entry that has been added by
          * this state table. The method ignores null values, or entries without
          * a version.
          */
        private def delete(entry: TableEntry[K, V]): Unit = {
            if (entry eq null) {
                return
            }
            Log trace s"[$tableKey] Delete ${entry.key} -> ${entry.value} " +
                      s"version:${entry.version} (table version:$version)"

            if (storageConnectedFlag.get()) {
                directory.asyncDelete(encodeEntryWithVersion(entry),
                                      -1, deleteCallback, entry)
            } else {
                failure(entry)
            }
        }

        /**
          * Computes the added entries by comparing a new snapshot in the
          * [[updateCache]] with the current [[cache]], and adding all missing
          * or updating entries. Replaced entries are added to the removal
          * argument list for deletion.
          *
          * The call of this method must be synchronized.
          */
        private def computeAddedEntries(rem: util.List[TableEntry[K, V]])
        : Unit = {
            // Compute the added entries.
            val addIterator = updateCache.values().iterator()
            while (addIterator.hasNext) {
                val newEntry = addIterator.next()
                val oldEntry = cache.get(newEntry.key)
                if (oldEntry eq null) {
                    cache.put(newEntry.key, newEntry)
                    updates.add(Update(newEntry.key, nullValue,
                                       newEntry.value))
                } else if (oldEntry.version != newEntry.version) {
                    cache.put(newEntry.key, newEntry)
                    updates.add(Update(newEntry.key, oldEntry.value,
                                       newEntry.value))
                    // Remove owned replaced entry.
                    if (owned.contains(oldEntry.version)) {
                        rem.add(oldEntry)
                    }
                }
            }
        }

        /**
          * Computes the removed entries by comparing a new snapshot in the
          * [[updateCache]] with the current [[cache]], and removing all
          * entries that are no longer in the snapshot.
          *
          * The call of this method must be synchronized.
          */
        private def computeRemovedEntries(rem: util.List[TableEntry[K, V]])
        : Unit = {
            val removeIterator = cache.entrySet().iterator()
            while (removeIterator.hasNext) {
                val oldEntry = removeIterator.next()
                if (!updateCache.containsKey(oldEntry.getKey)) {
                    removeIterator.remove()
                    updates.add(Update(oldEntry.getKey,
                                       oldEntry.getValue.value, nullValue))
                    // Remove owned deleted entry.
                    if (owned.contains(oldEntry.getValue.version)) {
                        rem.add(oldEntry.getValue)
                    }
                }
            }
        }

        /**
          * Deletes from storage the list of entries.
          */
        private def deleteEntries(rem: util.List[TableEntry[K, V]]): Unit = {
            // Delete the removed entries.
            Log trace s"[$tableKey] Deleting ${rem.size()} obsolete entries"
            var index = 0
            while (index < rem.size()) {
                delete(rem.get(index))
                index += 1
            }
        }

        /**
          * Retries the previously failed operations.
          */
        private def retry(): Unit = {
            var entry: TableEntry[K, V] = null
            do {
                entry = failures.poll()
                if (entry ne null) {
                    delete(entry)
                }
            } while ((entry ne null) && storageConnectedFlag.get())
        }

        /**
          * Publishes an update to all subscribers.
          */
        private def publish(update: Update[K, V]): Unit = {
            val sub = subscribers
            var index = 0
            while (index < sub.length) {
                sub(index) onNext update
                index += 1
            }
        }

        /**
          * Publishes all updates from the [[updates]] list to the current
          * subscribers.
          *
          * The call of this method must be synchronized.
          */
        private def publishUpdates(): Unit = {
            Log trace s"[$tableKey] Update with ${updates.size()} changes"

            // Publish updates to all subscribers.
            val subs = subscribers
            var updateIndex = 0
            while (updateIndex < updates.size()) {
                var subIndex = 0
                while (subIndex < subs.length) {
                    subs(subIndex) onNext updates.get(updateIndex)
                    subIndex += 1
                }
                updateIndex += 1
            }
        }

        /**
          * Processes a state proxy snapshot notification. This method must be
          * synchronized.
          */
        private def proxySnapshot(update: Notify.Update,
                                  rem: util.List[TableEntry[K, V]]): Unit = {
            Log trace s"[$tableKey] Snapshot begin:${update.getBegin} " +
                      s"end:${update.getEnd} entries:${update.getEntriesCount}"

            // If this is the beginning of a snapshot, clear the update cache,
            // otherwise verify that a snapshot notification sequence is in
            // progress.
            if (update.getBegin) {
                updateCache.clear()
                updates.clear()
                snapshotInProgress = true
            } else if (!snapshotInProgress) {
                Log debug s"[$tableKey] Notification sequence interrupted: " +
                          "discarding update"
                return
            }

            // Add all entries to the update cache: the entries are added
            // without any processing, since this is performed at the server.
            var index = 0
            while (index < update.getEntriesCount) {
                val entry = decodeEntry(update.getEntries(index))
                if (entry ne null) {
                    updateCache.put(entry.key, entry)
                }
                index += 1
            }

            // If this is the end of a snapshot, update the table and notify
            // the changes.
            if (update.getEnd) {
                snapshotInProgress = false

                // Compute the added entries.
                computeAddedEntries(rem)

                // Compute the removed entries.
                computeRemovedEntries(rem)

                // Publish updates.
                publishUpdates()
            }
        }

        /**
          * Processes a state proxy relative notification. This method must be
          * synchronized.
          */
        private def proxyRelative(update: Notify.Update,
                                  rem: util.List[TableEntry[K, V]]): Unit = {
            Log trace s"[$tableKey] Diff begin:${update.getBegin} " +
                      s"end:${update.getEnd} entries:${update.getEntriesCount}"

            updates.clear()

            // Apply all differential updates to the current cache: all updates
            // must be applied and notify (a sanity check is performed).
            var index = 0
            while (index < update.getEntriesCount) {
                val entry = update.getEntries(index)
                if (entry.hasValue) {
                    // This entry is added or updated.
                    val newEntry = decodeEntry(entry)
                    val oldEntry = cache.put(newEntry.key, newEntry)
                    if (oldEntry eq null) {
                        updates.add(Update(newEntry.key, nullValue,
                                           newEntry.value))
                    } else {
                        updates.add(Update(newEntry.key, oldEntry.value,
                                           newEntry.value))
                        // Remove owned deleted entry.
                        if (owned.contains(oldEntry.version)) {
                            rem.add(oldEntry)
                        }
                    }
                } else {
                    // This entry is removed.
                    val oldKey = decodeKey(entry.getKey)
                    val oldEntry = cache.remove(oldKey)
                    if (oldEntry eq null) {
                        Log warn s"$tableKey Inconsistent diff: removing " +
                                 s"$oldKey not found"
                    } else {
                        updates.add(Update(oldEntry.key, oldEntry.value,
                                           nullValue))
                        // Remove owned deleted entry.
                        if (owned.contains(oldEntry.version)) {
                            rem.add(oldEntry)
                        }
                    }
                }
                index += 1
            }

            // Publish updates.
            publishUpdates()
        }
    }

    protected def tableKey: Key

    protected def directory: Directory
    protected def connection: Observable[StorageConnectionState]
    protected def proxy: StateTableClient

    protected def nullValue: V

    @volatile private var state: State = null

    private var subscriptions = 0L
    private val onSubscribe = new OnTableSubscribe
    private val sync = new Object

    /**
      * @see [[StateTable.start()]]
      */
    @inline
    override def start(): Unit = sync.synchronized {
        startInternal(1)
    }

    /**
      * @see [[StateTable.stop()]]
      */
    @inline
    override def stop(): Unit = sync.synchronized {
        stopInternal(1, child = null)
    }

    /**
      * @see [[StateTable.add()]]
      */
    override def add(key: K, value: V): Unit = {
        val currentState = state
        if (currentState ne null) {
            currentState.add(key, value)
        } else {
            Log warn s"[$tableKey] Adding $key -> $value with table stopped"
        }
    }

    /**
      * @see [[StateTable.remove()]]
      */
    override def remove(key: K): V = {
        val currentState = state
        if (currentState ne null) {
            currentState.remove(key, nullValue)
        } else {
            Log warn s"[$tableKey] Removing $key with table stopped"
            nullValue
        }
    }

    /**
      * @see [[StateTable.remove()]]
      */
    override def remove(key: K, value: V): Boolean = {
        val currentState = state
        if (currentState ne null) {
            currentState.remove(key, value) != nullValue
        } else {
            Log warn s"[$tableKey] Removing $key -> $value with table stopped"
            false
        }
    }

    /**
      * @see [[StateTable.containsLocal()]]
      */
    override def containsLocal(key: K): Boolean = {
        val currentState = state
        if (currentState ne null) {
            currentState.containsKey(key)
        } else {
            false
        }
    }

    /**
      * @see [[StateTable.containsLocal()]]
      */
    override def containsLocal(key: K, value: V): Boolean = {
        val currentState = state
        if (currentState ne null) {
            currentState.contains(key, value)
        } else {
            false
        }
    }

    /**
      * @see [[StateTable.getLocal()]]
      */
    override def getLocal(key: K): V = {
        val currentState = state
        if (currentState ne null) {
            currentState.get(key)
        } else {
            nullValue
        }
    }

    /**
      * @see [[StateTable.getLocalByValue()]]
      */
    override def getLocalByValue(value: V): Set[K] = {
        val currentState = state
        if (currentState ne null) {
            currentState.getByValue(value)
        } else {
            Set.empty
        }
    }

    /**
      * @see [[StateTable.localSnapshot]]
      */
    override def localSnapshot: Map[K, V] = {
        val currentState = state
        if (currentState ne null) {
            currentState.snapshot
        } else {
            Map.empty
        }
    }

    /**
      * @see [[StateTable.observable]]
      */
    @inline
    override val observable: Observable[Update[K, V]] = {
        Observable.create(onSubscribe)
    }

    /**
      * @return True if the table is stopped.
      */
    def isStopped: Boolean = state eq null

    /**
      * Starts the synchronization of this state table with the backend storage
      * and state proxy servers.
      */
    private def startInternal(inc: Int): State = {
        subscriptions += inc
        if (state eq null) {
            Log debug s"[$tableKey] Starting state table"
            state = new State
            try {
                state.start()
            } catch {
                case t: Throwable =>
                    Log error s"[$tableKey] exception caught in start: $t"
                    throw t
            }
        }
        state
    }

    /**
      * Stops the synchronization of this state state with the backend storage
      * and state proxy servers.
      */
    private def stopInternal(dec: Int, child: Subscriber[_ >: Update[K, V]])
    : Unit = {

        def hasNoneOrOnlySubscriber(child: Subscriber[_ >: Update[K, V]])
        : Boolean = {
            val subscribers = state.get().subscribers
            subscribers.length == 0 ||
                (subscribers.length == 1 &&
                 subscribers(0).asInstanceOf[ProtectedSubscriber[K, V]]
                     .contains(child))
        }

        if (subscriptions > 0) {
            subscriptions -= dec
        }
        if (subscriptions == 0 && (state ne null) &&
            hasNoneOrOnlySubscriber(child)) {
            Log debug s"[$tableKey] Stopping state table"
            state.stop(null)
            state = null
        }
    }

    /**
      * Closes immediately the current state table, regardless of the current
      * subscribers.
      */
    private def close(e: Throwable): Unit = sync.synchronized {
        if (e ne null)
            Log warn s"[$tableKey] State table closed with exception: $e"
        else
            Log warn s"[$tableKey] State table closed"
        subscriptions = 0
        if (state ne null) {
            state.stop(e)
            state = null
        }
    }

    /**
      * Handles changes to the storage connection state.
      */
    private def processStorageConnection(s: State,
                                         connectionState: StorageConnectionState)
    : Unit = {
        if (state ne s) {
            return
        }
        connectionState match {
            case StorageConnectionState.CONNECTED =>
                Log debug s"[$tableKey] Storage connected"
                s.storageConnected()
            case StorageConnectionState.SUSPENDED =>
                Log debug s"[$tableKey] Storage connection suspended"
                s.storageDisconnected()
            case StorageConnectionState.RECONNECTED =>
                Log debug s"[$tableKey] Storage reconnected"
                s.storageReconnected()
            case StorageConnectionState.LOST =>
                Log warn s"[$tableKey] Storage connection lost"
                s.storageDisconnected()
                close(new ConnectionLossException())
            case StorageConnectionState.READ_ONLY =>
                Log warn s"[$tableKey] Storage connection is read-only"
                s.storageDisconnected()
        }
    }

    private def processProxyConnection(s: State,
                                       connectionState: ProxyConnectionState)
    : Unit = {
        if (state ne s) {
            return
        }
        if (connectionState.isConnected) {
            Log debug s"[$tableKey] State proxy connected"
            s.proxyConnected()
        } else {
            Log debug s"[$tableKey] State proxy disconnected"
            s.proxyDisconnected()
        }
    }

    /**
      * Processes the completion of adding a new entry to this state table. The
      * method updates to the current state the map of owned versions, that is
      * entries that have been added through this table.
      */
    private def processAddCallback(callbackState: State, path: String,
                                   context: Object): Unit = {
        // Ignore, if the callback state does not match the current table state.
        if (state ne callbackState) {
            return
        }
        callbackState.addComplete(decodeEntry(path),
                                  context.asInstanceOf[KeyValue[K, V]])
    }

    /**
      * Processes errors during add.
      */
    private def processAddError(errorState: State, e: KeeperException,
                                context: Object): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne errorState) {
            return
        }

        errorState.addError(e, context.asInstanceOf[KeyValue[K, V]])
    }

    /**
      * Processes the list of entries received from storage for this state
      * table. This entries set always overrides the
      */
    private def processGetCallback(callbackState: State,
                                   entries: util.Collection[String],
                                   stat: Stat, context: Object): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne callbackState) {
            return
        }

        Log trace s"[$tableKey] Read ${entries.size()} entries in " +
                  s"${latency(context)} ms"
        callbackState.update(entries, stat.getPzxid)
    }

    /**
      * Processes errors during get.
      */
    private def processGetError(errorState: State, e: KeeperException,
                                context: Object): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne errorState) {
            return
        }

        e.code() match {
            case Code.NONODE =>
                Log debug s"[$tableKey] State table does not exist or deleted"
                close(e = null)
            case Code.CONNECTIONLOSS =>
                Log warn s"[$tableKey] Storage connection lost"
                close(e)
            case _ =>
                Log warn s"[$tableKey] Refreshing state table failed ${e.code()}"
                close(e)
        }
    }

    /**
      * Processes delete completions.
      */
    private def processDeleteCallback(callbackState: State,
                                      context: Object): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne callbackState) {
            return
        }

        callbackState.removeComplete(context.asInstanceOf[TableEntry[K, V]])
    }

    /**
      * Processes errors during delete. If the error is retriable, the method
      * enqueues the failed context to retry later.
      */
    private def processDeleteError(errorState: State, e: KeeperException,
                                   context: Object): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne errorState) {
            return
        }

        val entry = context.asInstanceOf[TableEntry[K, V]]

        e.code() match {
            case Code.CONNECTIONLOSS | Code.OPERATIONTIMEOUT |
                 Code.SESSIONEXPIRED | Code.SESSIONMOVED =>
                Log.info(s"[$tableKey] Delete ${e.getPath} failed code:" +
                         s"${e.code().intValue()} retrying")
                errorState.failure(entry)
            case Code.NONODE =>
                errorState.removeComplete(entry)
            case _ =>
                Log.warn(s"[$tableKey] Delete ${e.getPath} failed code:" +
                         s"${e.code().intValue()}", e)
                errorState.removeComplete(entry)
        }
    }

    /**
      * Processes a watcher event for the current state table.
      */
    private def processWatcher(watcherState: State, event: WatchedEvent): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne watcherState) {
            return
        }

        Log trace s"[$tableKey] Table data changed: refreshing"
        watcherState.refresh()
    }

    /**
      * Processes an update from the state proxy client.
      */
    private def processProxyUpdate(proxyState: State, update: Notify.Update): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne proxyState) {
            return
        }

        proxyState.proxyUpdate(update)
    }

    /**
      * Processes an error from the state proxy client.
      */
    private def processProxyError(proxyState: State, e: Throwable): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne proxyState) {
            return
        }

        proxyState.proxyError(e)
    }

    /**
      * Processes an observable completion from the state proxy client.
      */
    private def processProxyCompletion(proxyState: State): Unit = {
        // Ignore, if the error state does not match the current table state.
        if (state ne proxyState) {
            return
        }

        Log debug s"[$tableKey] State proxy notification stream completed"
        proxyState.proxyDisconnected()
    }

    /**
      * @return The encoded table entry path with the version suffix.
      */
    private def encodeEntryWithVersion(entry: TableEntry[K, V]): String = {

        encodePath(entry.key, entry.value, entry.version)
    }

    /**
      * @return The encoded table entry path without the version suffix.
      */
    private def encodeEntryPrefix(key: K, value: V): String = {
        s"/${encodeKey(key)},${encodeValue(value)},"
    }

    /**
      * Decodes the table path and returns a [[TableEntry]].
      */
    private def decodeEntry(path: String): TableEntry[K, V] = {
        val string = if (path.startsWith("/")) path.substring(1)
        else path
        val tokens = string.split(",")
        if (tokens.length != 3)
            return null
        try {
            TableEntry(decodeKey(tokens(0)), decodeValue(tokens(1)),
                       Integer.parseInt(tokens(2)))
        } catch {
            case NonFatal(_) => null
        }
    }

    /**
      * Decodes a state proxy [[Notify.Entry]] and returns a [[TableEntry]].
      */
    private def decodeEntry(entry: Notify.Entry): TableEntry[K, V] = {
        try {
            TableEntry(decodeKey(entry.getKey), decodeValue(entry.getValue),
                       entry.getVersion)
        } catch {
            case NonFatal(_) => null
        }
    }

    /**
      * Computes the latency of a state table operation assuming that the
      * context includes the start timestamp. Returns -1 otherwise.
      */
    private def latency(context: AnyRef): Long = {
        context match {
            case startTime: java.lang.Long =>
                System.currentTimeMillis() - startTime
            case _ => -1L
        }
    }

}
