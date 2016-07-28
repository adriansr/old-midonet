//
// Copyright 2016 Midokura SARL
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
package org.midonet.benchmark.controller.client

import org.midonet.benchmark.Protocol._
import org.midonet.benchmark.controller.Common._

sealed trait ProtocolHandler {

    def onAcknowledgeReceived(rid: RequestId, msg: Acknowledge): Boolean

    def onBootstrapReceived(rid: RequestId, msg: Bootstrap): Boolean

    def onStartReceived(rid: RequestId, msg: Start): Boolean

    def onStopReceived(rid: RequestId, msg: Stop): Boolean

    def onTerminateReceived(rid: RequestId, msg: Terminate): Boolean
}

private[controller] sealed abstract
class ProtocolHandlerImpl(client: ClientInterface) extends ProtocolHandler {

    def onAcknowledgeReceived(rid: RequestId, msg: Acknowledge): Boolean = {
        client.logger error s"Received spurious acknowledge: $rid"
        true
    }

    def onBootstrapReceived(rid: RequestId, msg: Bootstrap): Boolean = {
        client.logger error s"Received out of place bootstrap: $rid"
        false
    }

    def onStartReceived(rid: RequestId, msg: Start): Boolean = {
        client.logger error s"Received out of place start: $rid"
        false
    }

    def onStopReceived(rid: RequestId, msg: Stop): Boolean = {
        client.logger error s"Received out of place stop: $rid"
        false
    }

    def onTerminateReceived(rid: RequestId, msg: Terminate): Boolean = {
        client.logger info "Received terminate from controller"
        false
    }
}

private[controller]
class DisconnectedProtocolHandler(client: ClientInterface) extends ProtocolHandlerImpl(client)

class IdleProtocolHandler(requestId: RequestId,
                          client: ClientInterface) extends ProtocolHandlerImpl(client) {
    override def onAcknowledgeReceived(rid: RequestId, msg: Acknowledge): Boolean = {
        if (rid == requestId) {
            client.logger debug "Registered to server"
        } else {
            client.logger warn s"Received spurious acknowledge"
        }
        true
    }

    override def onBootstrapReceived(rid: RequestId, msg: Bootstrap): Boolean = {
        client.become(this, new ReadyProtocolHandler(msg.toSession,client)) &&
            client.acknowledge(rid)
    }
}

class ReadyProtocolHandler(session: Session,
                           client: ClientInterface) extends ProtocolHandlerImpl(client) {

    override def onBootstrapReceived(rid: RequestId, msg: Bootstrap): Boolean = {
        client.become(this, new ReadyProtocolHandler(msg.toSession, client)) &&
            client.acknowledge(rid)
    }

    override def onStopReceived(rid: RequestId, msg: Stop): Boolean = {
        client.logger warn "Received spurious stop"
        true
    }

    override def onStartReceived(rid: RequestId, msg: Start): Boolean = {
        client.become(this, new RunningProtocolHandler(session, client)) &&
        client.acknowledge(rid) && client.startBenchmark(session)
    }
}

class RunningProtocolHandler(session: Session,
                             client: ClientInterface) extends ProtocolHandlerImpl(client) {

    override def onStopReceived(rid: RequestId, msg: Stop): Boolean = {
        val sid = msg.getSessionId
        if (sid == session.id) {
            client.stopBenchmark()
            client.become(this, new ReadyProtocolHandler(session, client)) &&
                client.acknowledge(rid)
        } else {
            client.logger warn s"Ignoring stop for wrong session $sid"
            true
        }
    }
}



