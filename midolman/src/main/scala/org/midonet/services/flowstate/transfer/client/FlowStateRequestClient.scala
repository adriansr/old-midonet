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

package org.midonet.services.flowstate.transfer.client

import java.io.{Closeable, DataInputStream, IOException}
import java.net.Socket
import java.util.UUID

import org.midonet.cluster.flowstate.FlowStateTransfer.{StateRequest, StateResponse}
import org.midonet.midolman.config.FlowStateConfig
import org.midonet.packets.SbeEncoder
import org.midonet.services.flowstate.transfer.StateTransferProtocolBuilder._
import org.midonet.services.flowstate.transfer.StateTransferProtocolParser._
import org.midonet.services.flowstate.transfer.internal._
import org.midonet.util.io.stream.ByteBufferBlockWriter
import org.midonet.util.ClosingRetriable

trait FlowStateRequestClient extends ClosingRetriable {

    private val Retries = 3

    def flowStateConfig: FlowStateConfig

    protected def initSocket(clientHost: String = "0.0.0.0") = {
        val socket = new Socket(clientHost, flowStateConfig.port)
        socket.setSoTimeout(flowStateConfig.connectionTimeout)
        socket
    }

    protected def sendRequest(socket: Socket, dis: DataInputStream,
                            stateRequest: StateRequest) = {
        try {
            socket.getOutputStream().write(stateRequest.toByteArray)

            val protobufSize = dis.readInt()

            val rawResponse = readBytes(dis, protobufSize)
            val response = StateResponse.parseFrom(rawResponse)

            parseStateResponse(response)
        } catch {
            case e: IOException =>
                log warn (s"Unable to get requested flow from " +
                    s"${socket.getInetAddress.getHostName}", e)
                throw e
        }
    }

    protected def readBytes(dis: DataInputStream, size: Int): Array[Byte] = {
        val buffer = new Array[Byte](size)
        dis.readFully(buffer)
        buffer
    }

    protected def retryClosing(closeable: Closeable) (retriable: => Unit): Unit =
        retryClosing(Retries)(closeable)(retriable)

}

/*
 * TCP client used for remote communications between a MidoNet Agent and the
 * flow state minion running in the host
 */
class FlowStateInternalClient(override val flowStateConfig: FlowStateConfig)
     extends FlowStateRequestClient {

    def remoteFlowStateFrom(host: String, portId: UUID) = {
        val aggregator = new FlowStateAggregator
        var socket: Socket = null

        retryClosing(socket) {
            socket = initSocket()
            val dis = new DataInputStream(socket.getInputStream)

            val request = buildStateRequestRemote(portId, host)
            val response = sendRequest(socket, dis, request)

            response match {
                case StateAck(port) =>
                    log debug s"Remote Ack received from previous owner of $port"
                    pipelinedReadTranslatedState(dis, aggregator)
                case StateError(code, description) =>
                    log warn s"Ignoring response: $code error received from" +
                        s" previous owner: $description"
                case _ =>
                    log warn "Ignoring response: received a malformed/illegal" +
                        s" response from $host"
            }
        }

        aggregator.batch()
    }

    def internalFlowStateFrom(portId: UUID) = {
        val aggregator = new FlowStateAggregator
        var socket: Socket = null

        retryClosing(socket) {
            socket = initSocket()
            val dis = new DataInputStream(socket.getInputStream)

            val request = buildStateRequestInternal(portId)
            val response = sendRequest(socket, dis, request)

            response match {
                case StateAck(port) =>
                    log debug s"Internal Ack received from previous owner of $port"
                    pipelinedReadTranslatedState(dis, aggregator)
                case StateError(code, description) =>
                    log warn s"Ignoring response: $code error received from" +
                        s" previous owner: $description"
                case _ =>
                    log warn "Ignoring response: received a malformed/illegal" +
                        " response"
            }
        }

        aggregator.batch()
    }

    private def pipelinedReadTranslatedState(dis: DataInputStream,
                                             aggregator: FlowStateAggregator): Unit = {
        var next = dis.readInt()
        while (next > 0) {
            val rawEncoder = readBytes(dis, next)
            val sbe = decodeFromBytes(rawEncoder)
            aggregator.push(sbe)
            next = dis.readInt()
        }
    }

    private def decodeFromBytes(bytes: Array[Byte]) = {
        val encoder = new SbeEncoder
        encoder.decodeFrom(bytes)
        encoder
    }

}

/*
 * TCP client used for remote communications between flow state minions in
 * different MidoNet Agents
 */
class FlowStateRemoteClient(override val flowStateConfig: FlowStateConfig)
    extends FlowStateRequestClient {

    def rawPipelinedFlowStateFrom(host: String, portId: UUID,
                                  writer: ByteBufferBlockWriter[_]): Unit = {
        var socket: Socket = null

        retryClosing(socket) {
            socket = initSocket(host)
            val dis = new DataInputStream(socket.getInputStream)

            val request = buildStateRequestRaw(portId)
            val response = sendRequest(socket, dis, request)

            response match {
                case StateAck(port) =>
                    log debug s"Raw Ack received from previous owner of $port"
                    pipelinedReadWriteRawState(dis, writer)
                case StateError(code, description) =>
                    log warn s"Ignoring response: $code error received from" +
                        s" previous owner: $description"
                case _ =>
                    log warn "Ignoring response: received a malformed/illegal" +
                        s" response from $host"
            }
        }
    }

    private def pipelinedReadWriteRawState(dis: DataInputStream,
                                           writer: ByteBufferBlockWriter[_]): Unit = {
        var next = dis.readInt()
        while (next > 0) {
            val buffer = readBytes(dis, next)
            writer.write(buffer)
            next = dis.readInt()
        }
    }

}
