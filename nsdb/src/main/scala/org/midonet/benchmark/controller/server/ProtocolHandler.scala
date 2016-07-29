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

package org.midonet.benchmark.controller.server

import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}

import org.midonet.benchmark.Protocol._

class ProtocolHandler(manager: WorkerManager)
    extends ChannelInboundHandlerAdapter {

    override def exceptionCaught(ctx: ChannelHandlerContext,
                                 cause: Throwable): Unit = {

    }

    override def channelRead(ctx: ChannelHandlerContext,
                             msg: AnyRef): Unit = {
        val result = msg match {
            case wmsg: WorkerMessage if wmsg.hasRequestId =>
                handleKnownMessage(ctx,wmsg)
            case _ =>
                handleUnknownMessage(ctx)
        }
        if (!result) {
            ctx.close()
        }
    }

    override def channelUnregistered(ctx: ChannelHandlerContext): Unit = {
        manager.disconnected(ctx.channel.remoteAddress)
    }

    override def channelRegistered(ctx: ChannelHandlerContext): Unit = {
        manager.connected(ctx.channel.remoteAddress, ctx.channel)
    }

    private def handleKnownMessage(ctx: ChannelHandlerContext,
                                   msg: WorkerMessage): Boolean = {

        val client = ctx.channel.remoteAddress()
        val rid = msg.getRequestId

        msg.getContentCase match {
            case WorkerMessage.ContentCase.ACKNOWLEDGE =>
                manager.ackReceived(client, rid)

            case WorkerMessage.ContentCase.DATA =>
                manager.dataReceived(client, rid, msg.getData)

            case WorkerMessage.ContentCase.ERROR =>
                manager.errorReceived(client, rid, msg.getError)

            case WorkerMessage.ContentCase.REGISTER =>
                manager.registerReceived(client, rid)

            case WorkerMessage.ContentCase.UNREGISTER =>
                manager.unregisterReceived(client, rid)

            case WorkerMessage.ContentCase.STOP =>
                manager.stopReceived(client, rid)

            case _ => handleUnknownMessage(ctx)
        }
    }

    private def handleUnknownMessage(ctx: ChannelHandlerContext): Boolean = {
        ctx.writeAndFlush(buildTerminateMsg(1,"Unknown message"))
        false
    }

    private def buildTerminateMsg(requestId: Long, reason: String): ControllerMessage =
        ControllerMessage.newBuilder()
            .setRequestId(requestId)
            .setTerminate(Terminate.newBuilder()
                              .setError(Error.newBuilder().setReason(reason)))
            .build()

}
