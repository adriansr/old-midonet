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

package org.midonet.benchmark

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, StandardOpenOption}
import java.util
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}
import scala.reflect.ClassTag

import com.codahale.metrics.MetricRegistry
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.KeeperException
import org.rogach.scallop.{ScallopConf, Subcommand}

import rx.Observer

import org.midonet.benchmark.Common._
import org.midonet.benchmark.tables._
import org.midonet.cluster.data.storage.StateTable.Update
import org.midonet.cluster.data.storage.model.ArpEntry
import org.midonet.cluster.data.storage.{StateTable, StateTableStorage, ZookeeperObjectMapper}
import org.midonet.cluster.models.Topology.{Network, Port, Router}
import org.midonet.cluster.services.{MidonetBackend, MidonetBackendService}
import org.midonet.cluster.storage.MidonetBackendConfig
import org.midonet.cluster.util.UUIDUtil._
import org.midonet.conf.HostIdGenerator.PropertiesFileNotWritableException
import org.midonet.conf.MidoNodeConfigurator
import org.midonet.packets.{IPv4Addr, MAC}
import org.midonet.util.concurrent._
import org.midonet.util.functors.makeRunnable

object StateBenchmarkRunner {

    private val Random = new scala.util.Random()

    private val SuccessCode = 0
    private val NoArgumentsErrorCode = 1
    private val PropertiesFileErrorCode = 2
    private val ZooKeeperErrorCode = 3
    private val OtherErrorCode = 4
    private val BenchmarkFailedErrorCode = 5

    private val WriteAddOp = 0.toByte
    private val WriteUpdateOp = 1.toByte
    private val WriteDeleteOp = 2.toByte
    private val ReadAddOp = 3.toByte
    private val ReadUpdateOp = 4.toByte
    private val ReadDeleteOp = 5.toByte

    private class OutputWriter(output: String) {
        val path = FileSystems.getDefault.getPath(output)
        val channel = FileChannel.open(
            path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)
        val executor = Executors.newSingleThreadExecutor()

        def append(op: TableOp): Unit = {
            executor.submit(makeRunnable {
                val buffer = ByteBuffer.allocate(40)
                buffer.putInt(op.table)
                buffer.putLong(op.key)
                buffer.putLong(op.oldValue)
                buffer.putLong(op.newValue)
                buffer.putLong(op.timestamp)
                buffer.putInt(op.op)
                buffer.rewind()
                channel.write(buffer)
            })
        }

        def close(): Unit = {
            executor.shutdown()
            while (!executor.awaitTermination(600000, TimeUnit.MILLISECONDS)) {
                System.err.println("[bm-agent] Closing the output writer timed " +
                                   "out after 10 minutes")
            }
            channel.close()
        }
    }

    private abstract class TableInfo[K,V](val objectClass: Class[_],
                                          val keyClass: Class[K],
                                          val valueClass: Class[V],
                                          val name: String,
                                          val tableClass: Class[_ <: StateTable[K,V]]) {
        def newObject(id: UUID): AnyRef
        def randomKey(): Long
        def randomValue(): Long
        def encodeKey(key: Long): K
        def decodeKey(key: K): Long
        def encodeValue(value: Long): V
        def decodeValue(value: V): Long
    }

    private case class TableOp(table: Int, key: Long, oldValue: Long,
                               newValue: Long, timestamp: Long, op: Byte)

    private case class Entry(key: Long, value: Long,
                             encodedKey: Any, encodedValue: Any,
                             timestamp: Long)

    private class Table(info: TableInfo[Any,Any], index: Int,
                        writer: OutputWriter, inner: StateTable[Any, Any])
        extends Observer[Update[Any, Any]] {

        private val entries = new util.HashMap[Long, Entry]

        inner.start()
        val subscription = inner.observable.subscribe(this)

        override def onNext(update: Update[Any, Any]): Unit = update match {
            case Update(k, null, v) =>
                val key = info.decodeKey(k)
                val value = info.decodeValue(v)
                writer.append(TableOp(index, key, oldValue = 0L, value,
                                      System.currentTimeMillis(), ReadAddOp))
            case Update(k, v, null) =>
                val key = info.decodeKey(k)
                val value = info.decodeValue(v)
                writer.append(TableOp(index, key, value, newValue = 0L,
                                      System.currentTimeMillis(), ReadDeleteOp))
            case Update(k, ov, nv) =>
                val key = info.decodeKey(k)
                val oldValue = info.decodeValue(ov)
                val newValue = info.decodeValue(nv)
                writer.append(TableOp(index, key, oldValue, newValue,
                                      System.currentTimeMillis(), ReadUpdateOp))
        }

        override def onError(e: Throwable): Unit = {
            System.err.println(s"[bm-agent] Table $index observable failed: " +
                               e.getMessage)
        }

        override def onCompleted(): Unit = {
            System.err.println(s"[bm-agent] Table $index observable completed")
        }

        def close(): Unit = {
            subscription.unsubscribe()
            inner.stop()
        }

        def count = entries.size()

        def add(): Unit = {
            while (true) {
                val key = info.randomKey()
                val value = info.randomValue()
                val entry = Entry(key, value, info.encodeKey(key),
                                  info.encodeValue(value),
                                  System.currentTimeMillis())
                if (!inner.containsLocal(entry.encodedKey)) {
                    entries.put(key, entry)
                    inner.add(entry.encodedKey, entry.encodedValue)
                    writer.append(TableOp(index, key, oldValue = 0L, value,
                                          entry.timestamp, WriteAddOp))
                    return
                }
            }
        }

        def update(): Unit = {
            val oldEntry = randomEntry()
            if (oldEntry eq null) {
                return
            }
            val value = info.randomValue()
            val newEntry = Entry(oldEntry.key, value,
                                 info.encodeKey(oldEntry.key),
                                 info.encodeValue(value),
                                 System.currentTimeMillis())
            entries.put(newEntry.key, newEntry)
            inner.add(newEntry.encodedKey, newEntry.encodedValue)
            writer.append(TableOp(index, newEntry.key, oldEntry.value, value,
                                  newEntry.timestamp, WriteUpdateOp))
        }

        def remove(): Unit = {
            val entry = randomEntry()
            if (entry eq null) {
                return
            }
            entries.remove(entry.key)
            inner.remove(entry.encodedKey, entry.encodedValue)
            writer.append(TableOp(index, entry.key, entry.value, newValue = 0L,
                                  entry.timestamp, WriteDeleteOp))
        }

        private def randomEntry(): Entry = {
            val random = Random.nextInt(entries.size())
            val iterator = entries.values().iterator()
            var index = 0
            while (iterator.hasNext) {
                val entry = iterator.next()
                if (index >= random) {
                    return entry
                }
                index += 1
            }
            null
        }
    }

    private val Tables = Map(
        "bridge-mac" -> new TableInfo(classOf[Network], classOf[MAC],
                                      classOf[UUID], MidonetBackend.MacTable,
                                      classOf[MacIdStateTable]) {
            override def newObject(id: UUID): AnyRef = {
                Network.newBuilder().setId(id.asProto).build()
            }
            override def randomKey(): Long = {
                Random.nextLong() & 0xFFFFFFFFFFFFL
            }
            override def randomValue(): Long = {
                Random.nextLong()
            }
            override def encodeKey(key: Long): MAC = {
                new MAC(key)
            }
            override def decodeKey(key: MAC): Long = {
                key.asLong()
            }
            override def encodeValue(value: Long): UUID = {
                new UUID(0L, value)
            }
            override def decodeValue(value: UUID): Long = {
                value.getLeastSignificantBits
            }
        },
        "bridge-arp" -> new TableInfo(classOf[Network], classOf[IPv4Addr],
                                      classOf[MAC], MidonetBackend.Ip4MacTable,
                                      classOf[Ip4MacStateTable]) {
            override def newObject(id: UUID): AnyRef = {
                Network.newBuilder().setId(id.asProto).build()
            }
            override def randomKey(): Long = {
                Random.nextLong() & 0xFFFFFFFFL
            }
            override def randomValue(): Long = {
                Random.nextLong() & 0xFFFFFFFFFFFFL
            }
            override def encodeKey(key: Long): IPv4Addr = {
                new IPv4Addr(key.toInt)
            }
            override def decodeKey(key: IPv4Addr): Long = {
                key.addr
            }
            override def encodeValue(value: Long): MAC = {
                new MAC(value)
            }
            override def decodeValue(value: MAC): Long = {
                value.asLong()
            }
        },
        "router-arp" -> new TableInfo(classOf[Router], classOf[IPv4Addr],
                                      classOf[ArpEntry], MidonetBackend.ArpTable,
                                      classOf[ArpStateTable]) {
            override def newObject(id: UUID): AnyRef = {
                Router.newBuilder().setId(id.asProto).build()
            }
            override def randomKey(): Long = {
                Random.nextLong() & 0xFFFFFFFFL
            }
            override def randomValue(): Long = {
                Random.nextLong()
            }
            override def encodeKey(key: Long): IPv4Addr = {
                new IPv4Addr(key.toInt)
            }
            override def decodeKey(key: IPv4Addr): Long = {
                key.addr
            }
            override def encodeValue(value: Long): ArpEntry = {
                new ArpEntry(new MAC(value), value, value, value)
            }
            override def decodeValue(value: ArpEntry): Long = {
                value.lastArp
            }
        },
        "router-peer" -> new TableInfo(classOf[Port], classOf[MAC],
                                       classOf[IPv4Addr], MidonetBackend.PeeringTable,
                                       classOf[MacIp4StateTable]) {
            override def newObject(id: UUID): AnyRef = {
                Port.newBuilder().setId(id.asProto).build()
            }
            override def randomKey(): Long = {
                Random.nextLong() & 0xFFFFFFFFFFFFL
            }
            override def randomValue(): Long = {
                Random.nextLong() & 0xFFFFFFFFL
            }
            override def encodeKey(key: Long): MAC = {
                new MAC(key)
            }
            override def decodeKey(key: MAC): Long = {
                key.asLong()
            }
            override def encodeValue(value: Long): IPv4Addr = {
                new IPv4Addr(value.toInt)
            }
            override def decodeValue(value: IPv4Addr): Long = {
                value.addr
            }
        })

    trait BenchmarkCommand {
        def run(configurator: MidoNodeConfigurator): Int
    }
}

class StateBenchmarkRunner extends BenchmarkRunner {

    import StateBenchmarkRunner._

    /*object Config extends Subcommand("config") with BenchmarkCommand {
        descr("Prints the current configuration")

        val renderOptions = ConfigRenderOptions.defaults()
            .setOriginComments(false)
            .setComments(true)
            .setJson(true)
            .setFormatted(true)

        override def run(configurator: MidoNodeConfigurator): Int = {
            println(configurator.dropSchema(configurator.runtimeConfig,
                                            showPasswords = true)
                        .root().render(renderOptions))
            SuccessCode
        }
    }*/

    object Simple extends Subcommand("simple") with BenchmarkCommand {
        descr("Simple benchmark where the benchmark writes at a given average " +
              "rate to a number of state tables, and reads the updates from " +
              "all tables. The write operations follow an exponential " +
              "distribution. The benchmark begins with a warm-up interval " +
              "during which the test adds an initial number of entries to " +
              "the table. Following the warm-up the test enters a steady state " +
              "interval during which the benchmark randomly chooses one of " +
              "the following operations: (i) updating an entry, (ii) removing " +
              "and adding a new entry. The specified benchmark duration " +
              "refers to the steady-state interval.")

        val table =
            opt[String]("table", short = 't', default = Some("bridge-mac"), descr =
                "The state table class, it can be one of the following: " +
                "bridge-mac, bridge-arp, router-arp, router-peer")
        val duration =
            opt[Int]("duration", short = 'd', default = Some(600), descr =
                "The test duration in seconds")
        val tableCount =
            opt[Int]("table-count", short = 'n', default = Some(10), descr =
                "The number of tables to which the benchmark writes")
        val entryCount =
            opt[Int]("entry-count", short = 'e', default = Some(100), descr =
                "The initial number of entries added by the benchmark")
        val writeRate =
            opt[Int]("write-rate", short = 'w', default = Some(60), descr =
                "The number of writes per minute to a table.")
        val dump =
            opt[String]("dump", short = 'u', default = Some("benchmark-dump.out"), descr =
                "The output dump data file.")
        val stat =
            opt[String]("stat", short = 's', default = Some("benchmark-stat.out"), descr =
                "The output statistics data file.")

        private var tables: Array[Table] = null

        override def run(configurator: MidoNodeConfigurator): Int = {
            println("Starting simple benchmark...")

            val config = new MidonetBackendConfig(configurator.runtimeConfig)
            val curator = CuratorFrameworkFactory.newClient(
                config.hosts,
                new ExponentialBackoffRetry(config.retryMs.toInt, config.maxRetries))
            val registry = new MetricRegistry
            val backend = new MidonetBackendService(config, curator, curator,
                                                    registry, None) {
                protected override def setup(storage: StateTableStorage): Unit = {
                    for (info <- Tables.values) {
                        storage.registerTable(
                            info.objectClass,
                            info.keyClass.asInstanceOf[Class[Object]],
                            info.valueClass.asInstanceOf[Class[Object]],
                            info.name,
                            info.tableClass.asInstanceOf[Class[StateTable[Object, Object]]])
                    }
                }
            }

            val writer = new OutputWriter(dump.get.get)
            StateTableMetrics.writer = new BenchmarkWriter(stat.get.get)

            try {
                backend.startAsync().awaitRunning()

                val zoom = backend.store.asInstanceOf[ZookeeperObjectMapper]

                pre(config, backend, zoom, writer)
                warmUp(config, backend)
                steadyState(config, backend)
                post(config, backend)

                println("Simple benchmark completed successfully")
                SuccessCode
            } catch {
                case NonFatal(e) =>
                    System.err.println("[bm-agent] Simple benchmark failed: " +
                                       e.getMessage)
                    BenchmarkFailedErrorCode
            } finally {
                StateTableMetrics.writer.close()
                writer.close()
                backend.stopAsync().awaitTerminated()
                curator.close()
            }
        }

        private def pre(config: MidonetBackendConfig,
                        backend: MidonetBackendService,
                        zoom: ZookeeperObjectMapper,
                        writer: OutputWriter): Unit = {
            println("[Step 1 of 4] Creating objects and tables...")

            val count = tableCount.get.get
            val info = Tables.getOrElse(table.get.get,
                                        throw new IllegalArgumentException("No such table"))
                .asInstanceOf[TableInfo[Any, Any]]
            tables = new Array[Table](count)

            for (index <- 0 until count) {
                val id = new UUID(0L, index)
                if (!backend.store.exists(info.objectClass, id).await()) {
                    try backend.store.create(info.newObject(id))
                    catch { case NonFatal(e) => }
                }
                val table = backend.stateTableStore
                    .getTable(info.objectClass, new UUID(0L, index), info.name)(
                        ClassTag(info.keyClass), ClassTag(info.valueClass))
                tables(index) = new Table(info, index, writer, table.asInstanceOf[StateTable[Any, Any]])
            }
        }

        private def warmUp(config: MidonetBackendConfig,
                           backend: MidonetBackendService): Unit = {
            val count = entryCount.get.get
            println(s"[Step 2 of 4] Warming up by adding $count entries " +
                    s"to ${tables.length} tables...")

            for (table <- tables) {
                for (index <- 0 until count) {
                    table.add()
                }
            }

            println("[Step 2 of 4] Warming up completed")
        }

        private def steadyState(config: MidonetBackendConfig,
                                backend: MidonetBackendService): Unit = {
            println("[Step 3 of 4] Steady state benchmark for " +
                    s"${duration.get.get} seconds...")

            val startTime = System.currentTimeMillis()
            val finishTime = startTime + duration.get.get * 1000
            val sleepTime = 60000 / (writeRate.get.get * tables.length)

            println(s"[Step 3 of 4] Average inter-op interval is $sleepTime " +
                    "milliseconds")
            while (System.currentTimeMillis() < finishTime) {
                Thread.sleep(sleepTime)
                tables(Random.nextInt(tables.length)).add()
            }

            println("[Step 3 of 4] Steady state completed")
        }

        private def post(config: MidonetBackendConfig,
                         backend: MidonetBackendService): Unit = {
            Thread.sleep(10000)
            println("[Step 4 of 4] Cleaning up...")

            val info = Tables.getOrElse(table.get.get,
                                        throw new IllegalArgumentException("No such table"))
                .asInstanceOf[TableInfo[Any, Any]]

            for (index <- tables.indices) {
                tables(index).close()
                try backend.store.delete(info.objectClass, new UUID(0L, index))
                catch { case NonFatal(e) => }
            }
        }

    }

    def start(session: TestRun): Unit = {

        val bootstrapConfig =
            ConfigFactory.parseString("zookeeper.bootstrap_timeout : 1s")

        Simple.run(MidoNodeConfigurator(bootstrapConfig))
    }

    def stop(): Unit = {

    }
}
