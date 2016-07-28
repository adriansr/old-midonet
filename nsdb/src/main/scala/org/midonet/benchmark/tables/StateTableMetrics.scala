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

package org.midonet.benchmark.tables

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import org.midonet.benchmark.tables.StateTableMetrics.{Entry, Stat}
import org.midonet.cluster.data.storage.model.ArpEntry
import org.midonet.packets.{IPv4Addr, MAC}

object StateTableMetrics {

    private case class Stat(init: Long) {
        var callback: Long = Long.MinValue
        var storage: Long = Long.MinValue
        var proxy: Long = Long.MinValue
    }

    private case class Entry[K, V](key: K, value: V)

    @volatile private[benchmark] var writer: BenchmarkWriter = null

    def encodeEntry(entry: Any): Long = {
        entry match {
            case mac: MAC => mac.asLong()
            case address: IPv4Addr => address.addr.toLong
            case uuid: UUID => uuid.getLeastSignificantBits
            case arp: ArpEntry => arp.expiry
            case _ => Long.MinValue
        }
    }

}

trait StateTableMetrics[K, V] {

    private val adding = new util.HashMap[Entry[K, V], Stat]()
    private val removing = new util.HashMap[Entry[K, V], Stat]()

    def statAdd(key: K, value: V): Unit = {
        adding.put(Entry(key, value), Stat(System.nanoTime()))
    }

    def statAddCallback(key: K, value: V): Unit = {
        val stat = adding.get(Entry(key, value))
        if (stat ne null) {
            stat.callback = System.nanoTime()
        }
    }

    def statAddStore(key: K, value: V): Unit = {
        val entry = Entry(key, value)
        val stat = adding.remove(entry)
        if (stat ne null) {
            stat.storage = System.nanoTime()
            updateAddStat(entry, stat)
        }
    }

    def statAddProxy(key: K, value: V): Unit = {
        val entry = Entry(key, value)
        val stat = adding.remove(entry)
        if (stat ne null) {
            stat.proxy = System.nanoTime()
            updateAddStat(entry, stat)
        }
    }

    def statRemove(key: K, value: V): Unit = {
        removing.put(Entry(key, value), Stat(System.nanoTime()))
    }

    def statRemoveCallback(key: K, value: V): Unit = {
        val stat = removing.get(Entry(key, value))
        if (stat ne null) {
            stat.callback = System.nanoTime()
        }
    }

    def statRemoveStore(key: K, value: V): Unit = {
        val entry = Entry(key, value)
        val stat = removing.remove(entry)
        if (stat ne null) {
            stat.storage = System.nanoTime()
            updateRemoveStat(entry, stat)
        }
    }

    def statRemoveProxy(key: K, value: V): Unit = {
        val entry = Entry(key, value)
        val stat = removing.remove(entry)
        if (stat ne null) {
            stat.proxy = System.nanoTime()
            updateRemoveStat(entry, stat)
        }
    }

    private def updateAddStat(entry: Entry[K, V], stat: Stat): Unit = {
        if (StateTableMetrics.writer ne null) {
            val buffer = ByteBuffer.allocate(44)
            buffer.putInt(1)
            buffer.putLong(StateTableMetrics.encodeEntry(entry.key))
            buffer.putLong(StateTableMetrics.encodeEntry(entry.value))
            if (stat.callback > Long.MinValue) {
                buffer.putLong(stat.callback - stat.init)
            }
            if (stat.storage > Long.MinValue) {
                buffer.putLong(stat.storage - stat.init)
            }
            if (stat.proxy > Long.MinValue) {
                buffer.putLong(stat.proxy - stat.init)
            }
            buffer.rewind()
            StateTableMetrics.writer.append(buffer)
        }
    }

    private def updateRemoveStat(entry: Entry[K, V], stat: Stat): Unit = {
        if (StateTableMetrics.writer ne null) {
            val buffer = ByteBuffer.allocate(44)
            buffer.putInt(2)
            buffer.putLong(StateTableMetrics.encodeEntry(entry.key))
            buffer.putLong(StateTableMetrics.encodeEntry(entry.value))
            if (stat.callback > Long.MinValue) {
                buffer.putLong(stat.callback - stat.init)
            }
            if (stat.storage > Long.MinValue) {
                buffer.putLong(stat.storage - stat.init)
            }
            if (stat.proxy > Long.MinValue) {
                buffer.putLong(stat.proxy - stat.init)
            }
            buffer.rewind()
            StateTableMetrics.writer.append(buffer)
        }
    }

}
