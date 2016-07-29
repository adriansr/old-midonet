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

import org.midonet.benchmark.BenchmarkWriter.StateTableMetric

object BenchmarkWriter {

    case class StateTableMetric(time: Long,
                                operation: Int,
                                key: Long,
                                value: Long,
                                callbackLatency: Long,
                                storageLatency: Long,
                                proxyLatency: Long)

}

trait BenchmarkWriter {

    def append(metric: StateTableMetric): Unit

    def close(): Unit

}
