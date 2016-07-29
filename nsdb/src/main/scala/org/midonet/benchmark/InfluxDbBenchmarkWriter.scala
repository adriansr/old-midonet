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

import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal

import org.influxdb.InfluxDBFactory
import org.influxdb.dto.Point

import org.midonet.benchmark.BenchmarkWriter.StateTableMetric
import org.midonet.util.functors._

class InfluxDbBenchmarkWriter(url: String, user: String, password: String,
                              database: String)
    extends BenchmarkWriter {

    val db = InfluxDBFactory.connect(url, user, password)
    db.enableBatch(1000, 100, TimeUnit.MILLISECONDS)

    val executor = Executors.newSingleThreadExecutor()

    override def append(metric: StateTableMetric): Unit = {
        executor.submit(makeRunnable {
            val point = Point.measurement("latency")
                .time(metric.time, TimeUnit.MILLISECONDS)
                .addField("callback", metric.callbackLatency)
                .addField("storage", metric.storageLatency)
                .addField("proxy", metric.proxyLatency)
                .build()
            try db.write(database, "default", point)
            catch { case NonFatal(_) => }
        })
    }

    override def close(): Unit = {
        executor.shutdown()
        while (!executor.awaitTermination(600000, TimeUnit.MILLISECONDS)) {
            System.err.println(s"[bm-writer] Closing the InfluxDB writer " +
                               s"$url timed out after 10 minutes")
        }
    }
}
