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

import java.util.concurrent.{Callable, ScheduledExecutorService}


import org.apache.zookeeper.{KeeperException, WatchedEvent, Watcher}
import org.slf4j.LoggerFactory

import org.midonet.cluster.backend.zookeeper.{StateAccessException, ZkConnection, ZkConnectionAwareWatcher, ZkDirectory}

class StateBenchmarkRunner(server: String,
                           timeout: Int,
                           executor: ScheduledExecutorService) {

    private val log = LoggerFactory.getLogger(classOf[StateBenchmarkRunner])

    val connWatcher = new Watcher {
        override def process(event: WatchedEvent): Unit = {
            processConnectionEvent(event)
        }
    }

    val zkConnection = new ZkConnection(server,
                                        timeout,
                                        connWatcher)

    zkConnection.open()

    private def processConnectionEvent(event: WatchedEvent): Unit = {
        log debug s"Connection event: $event"
    }

    log debug "Started"
}
