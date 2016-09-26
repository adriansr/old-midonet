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

package org.midonet.midolman

import scala.concurrent.{ExecutionContext, Future}

import org.midonet.midolman.io.UpcallDatapathConnectionManager
import org.midonet.netlink.rtnetlink.LinkOps
import org.midonet.util.concurrent.{Task, TaskSequence}

class VppSetup(uplinkInterface: String,
               upcallConnManager: UpcallDatapathConnectionManager)
              (implicit ec: ExecutionContext)
    extends TaskSequence("VPP setup") {

    class VethPairSetup(override val name: String,
                        devName: String,
                        peerName: String) extends Task {

        var macAddress: Option[Array[Byte]] = None

        @throws[Exception]
        override def execute(): Future[Any] = Future{
            val veth = LinkOps.createVethPair(devName, peerName, true)
            macAddress = Some(veth.dev.mac.getAddress)
        }

        @throws[Exception]
        override def rollback(): Future[Any] = Future {
            LinkOps.deleteLink(devName)
        }
    }

    class VppDevice(override val name: String,
                    deviceName: String) extends Task {

        @throws[Exception]
        override def execute(): Future[Any] = {

        }

        @throws[Exception]
        override def rollback(): Future[Any] = {

        }
    }

    val uplinkVeth = new VethPairSetup("uplink veth pair",
                                       "uplink-vpp-" + uplinkInterface,
                                       "uplink-ovs-" + uplinkInterface)

    add(uplinkVethSetup)
}
