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

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success}

import com.typesafe.scalalogging.Logger

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.protobuf.{ProtobufDecoder, ProtobufEncoder, ProtobufVarint32FrameDecoder, ProtobufVarint32LengthFieldPrepender}
import io.netty.handler.logging.{LogLevel, LoggingHandler}

import org.slf4j.LoggerFactory

import org.midonet.benchmark.Protocol
import org.midonet.benchmark.controller.Common._

object StateBenchmarkControlServer {
    val NumServerThreads = 1
    val NumWorkerThreads = 2
    val Backlog = 16
    val LogName = "benchmark-controller"
}

class StateBenchmarkControlServer(port: Int)(implicit ec: ExecutionContext) {

    import StateBenchmarkControlServer._

    private val log = Logger(LoggerFactory.getLogger(LogName))

    private val bossGroup = new NioEventLoopGroup(NumServerThreads)
    private val workerGroup = new NioEventLoopGroup(NumWorkerThreads)

    private val bootstrap = new ServerBootstrap

    val manager = new WorkerManager

    val serverChannelPromise = Promise[Channel]()

    bootstrap.group(bossGroup, workerGroup)

    // Options for the parent channel.
    bootstrap.option(ChannelOption.SO_REUSEADDR, Boolean.box(true))
    bootstrap.option(ChannelOption.SO_BACKLOG, Int.box(Backlog))

    // Options for the child channels: disable Nagle's algorithm for TCP to
    // improve latency, and sockets are closed asynchronously.
    bootstrap.childOption(ChannelOption.TCP_NODELAY, Boolean.box(true))
    bootstrap.childOption(ChannelOption.SO_LINGER, Int.box(-1))
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, Boolean.box(true))

    // Set logging.
    bootstrap.handler(new LoggingHandler(LogLevel.DEBUG))

    // Set the channel class.
    bootstrap.channel(classOf[NioServerSocketChannel])

    // Set the child handler.
    bootstrap.childHandler(new ChannelInitializer[SocketChannel] {
        @throws[Exception]
        override def initChannel(channel: SocketChannel): Unit = {
            channel.pipeline()
                .addLast("frameDecoder", new ProtobufVarint32FrameDecoder)
                .addLast("messageDecoder", new ProtobufDecoder(
                    Protocol.WorkerMessage.getDefaultInstance
                ))
                .addLast("frameEncoder",
                         new ProtobufVarint32LengthFieldPrepender)
                .addLast("messageEncoder", new ProtobufEncoder)
                .addLast("protocolHandler", new ProtocolHandler(manager))
        }
    })

    bootstrap.validate()

    bind(port)

    def bind(port: Int): Unit = {
        bootstrap.bind(port).asScala.onComplete {
            case Success(channel) =>
                log debug "Bind succeeded"
                serverChannelPromise trySuccess channel
            case Failure(err) =>
                log debug s"Bind failed: $err"
                serverChannelPromise tryFailure err
        }
    }

    /**
      * This method is synchronous
      */
    @throws[Exception]
    def close(): Unit = {
        serverChannelPromise.tryFailure(new Exception("Bind cancelled"))
        serverChannelPromise.future onComplete {
            case Success(channel) => channel.close()
            case _ =>
        }
        // TODO: etc..
    }

}
