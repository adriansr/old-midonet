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
import scala.util.Success

import org.midonet.midolman.io.UpcallDatapathConnectionManager
import org.midonet.midolman.vpp.VppApi
import org.midonet.netlink.rtnetlink.LinkOps
import org.midonet.odp.ports.NetDevPort
import org.midonet.util.concurrent.{Task, TaskSequence}
import org.midonet.util.logging.Logger

object VppSetup {

    trait MacSource {
        def getMac: Option[Array[Byte]]
    }

    class VethPairSetup(override val name: String,
                        devName: String,
                        peerName: String)
                       (implicit ec: ExecutionContext)
        extends Task with MacSource {

        var macAddress: Option[Array[Byte]] = None

        override def getMac: Option[Array[Byte]] = macAddress

        @throws[Exception]
        override def execute(): Future[Any] = Future {
            val veth = LinkOps.createVethPair(devName, peerName, up=true)
            macAddress = Some(veth.dev.mac.getAddress)
        }

        @throws[Exception]
        override def rollback(): Future[Any] = Future {
            LinkOps.deleteLink(devName)
        }
    }

    class VppDevice(override val name: String,
                    deviceName: String,
                    vppApi: VppApi,
                    macSource: MacSource)
                   (implicit ec: ExecutionContext)
        extends Task {

        var interfaceIndex: Option[Int] = None

        @throws[Exception]
        override def execute(): Future[Any] = {
            vppApi.createDevice(deviceName, macSource.getMac)
                .flatMap[Int] { result =>
                    vppApi.setDeviceAdminState(result.swIfIndex,
                                               isUp = true)
                        .map( _ => result.swIfIndex)
                } andThen {
                    case Success(index) => interfaceIndex = Some(index)
                }
        }

        @throws[Exception]
        override def rollback(): Future[Any] = {
            vppApi.deleteDevice(deviceName)
        }
    }

    /* TODO
    class OvsDatapath(override val name: String,

                      sourceDevice: String,
                      targetDevice: String,
                      upcallConnManager: UpcallDatapathConnectionManager,
                      datapathState: DatapathState) extends Task {

        @throws[Exception]
        override def execute(): Future[Any] = Future {
            upcallConnManager.createAndHookDpPort(datapathState.datapath,
                                                  new NetDevPort(targetDevice),
                                                  sourceDevice)
        }

        @throws[Exception]
        override def rollback(): Future[Any] = Future {

        }
    }*/
}

class VppSetup(uplinkInterface: String,
               upcallConnManager: UpcallDatapathConnectionManager,
               datapathState: DatapathState,
               log: Logger)
              (implicit ec: ExecutionContext)
    extends TaskSequence("VPP setup", log) {

    import VppSetup._

    val vppApi = new VppApi("midolman")

    val uplinkVppName = "uplink-vpp-" + uplinkInterface
    val uplinkOvsName = "uplink-ovs-" + uplinkInterface

    val uplinkVeth = new VethPairSetup("uplink veth pair",
                                       uplinkVppName,
                                       uplinkOvsName)

    val uplinkVpp = new VppDevice("uplink device at vpp",
                                  uplinkVppName,
                                  vppApi,
                                  uplinkVeth)

    // val uplinkOvs = new OvsDatapath("uplink flow at ovs",...)

    /*
     * setup the tasks, in execution order
     */
    add(uplinkVeth)
    add(uplinkVpp)
    // add(uplinkOvs)
}
