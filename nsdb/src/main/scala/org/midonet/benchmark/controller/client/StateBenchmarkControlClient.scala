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

import java.util.{Timer, TimerTask}
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import com.typesafe.scalalogging.Logger

import io.netty.channel.nio.NioEventLoopGroup

import org.midonet.benchmark.Protocol._
import org.midonet.benchmark.controller.Common._
import org.midonet.cluster.services.discovery.MidonetServiceHostAndPort
import org.midonet.cluster.services.state.client.PersistentConnection

object StateBenchmarkControlClient {
    val DefaultReconnectionDelay = 3 seconds
}

class StateBenchmarkControlClient(remote: MidonetServiceHostAndPort,
                                  executor: ScheduledExecutorService,
                                  eventLoopGroup: NioEventLoopGroup)
                                 (implicit ec: ExecutionContext)
    extends PersistentConnection[WorkerMessage,
                                 ControllerMessage]("benchmark-client",
                                                    executor)(ec,eventLoopGroup)
    with ClientInterface {

    import StateBenchmarkControlClient._

    val requestId = new AtomicInteger(0)

    var handler = new AtomicReference[ProtocolHandler](new DisconnectedProtocolHandler(this))

    /** Implement this method to provide the prototype message for decoding.
      *
      * @return The prototype message
      */
    override protected def getMessagePrototype: ControllerMessage =
        ControllerMessage.getDefaultInstance

    /** This method allows to provide the target host and port to connect to.
      * Will be called on every connect() so different values can be returned
      * i.e. from service discovery
      *
      * @return host and port
      */
    override protected def getRemoteAddress: Option[MidonetServiceHostAndPort] =
        Some(remote)

    /**
      * Implement this method to add custom behavior when the connection
      * is established (for the first time or after a connection retry)
      */
    override protected def onConnect(): Unit = {
        val rid = requestId.getAndIncrement()
        handler.get match {
            case h: DisconnectedProtocolHandler
                if become(h, new IdleProtocolHandler(rid,this)) =>
                log debug "Connected to controller"
                // send register message
                if (write(WorkerMessage.newBuilder()
                              .setRequestId(rid)
                              .setRegister(Register.getDefaultInstance).build())) {
                    log debug s"Sent register id:$rid"
                } else {
                    log warn "Failed to send register"
                    val prev = handler.getAndSet(new DisconnectedProtocolHandler(this))
                }
            case h: ProtocolHandler =>
                val msg = s"Unexpected state on connect: $h"
                log warn msg
                throw new IllegalStateException(msg)
        }
    }

    /**
      * Implement this method to add custom behavior when the connection
      * is closed (due to stop(), remote close(), or an error)
      */
    override protected def onDisconnect(cause: Throwable): Unit = {
        log debug "onDisconnect"
        val prev = handler.getAndSet(new DisconnectedProtocolHandler(this))
        prev match {
            case h: RunningProtocolHandler =>
                stopBenchmark()
            case _ =>
        }
    }

    /**
      * Implement this method to react to connection failures
      */
    override protected def onFailedConnection(cause: Throwable): Unit = {
        log warn s"Connection failed: ${cause.getMessage}"
    }

    /**
      * Implement this method to provide the reconnection delay
      */
    override protected def reconnectionDelay: Duration = DefaultReconnectionDelay

    override def onNext(msg: ControllerMessage): Unit = {

        val rid = msg.getRequestId
        log debug s"Received message: $msg"
        val h = handler.get
        val success = msg.getContentCase match {
            case ControllerMessage.ContentCase.ACKNOWLEDGE =>
                h.onAcknowledgeReceived(rid,msg.getAcknowledge)

            case ControllerMessage.ContentCase.BOOTSTRAP =>
                h.onBootstrapReceived(rid,msg.getBootstrap)

            case ControllerMessage.ContentCase.START =>
                h.onStartReceived(rid,msg.getStart)

            case ControllerMessage.ContentCase.STOP =>
                h.onStopReceived(rid,msg.getStop)

            case ControllerMessage.ContentCase.TERMINATE =>
                h.onTerminateReceived(rid,msg.getTerminate)

            case ControllerMessage.ContentCase.CONTENT_NOT_SET =>
                log warn "Ignoring message without content"
                false

            case _ =>
                log warn "Ignoring unexpected message"
                false
        }
        if (!success) stop()
    }

    override def logger: Logger = log

    override def become(oldHandler: ProtocolHandler,
                        newHandler: ProtocolHandler): Boolean = {
        val result = handler.compareAndSet(oldHandler,newHandler)
        assert(result)
        result
    }

    override def acknowledge(rid: RequestId): Boolean = {
        val result = write(WorkerMessage.newBuilder()
                               .setRequestId(rid)
                                .setAcknowledge(Acknowledge.getDefaultInstance)
                               .build())
        if (result) {
            log debug s"Sent acknowledge $rid"
        } else {
            log warn s"Failed sending acknowledge $rid"
        }

        result
    }

    trait BenchmarkRunner {
        def start(session: Session): Unit
        def stop(): Unit
    }

    object MockBenchmarkRunner extends BenchmarkRunner {

        var timer: Timer = null

        def start(session: Session): Unit = {
            assert(timer == null)
            timer = new Timer(true)
            timer.schedule(new TimerTask {
                def run(): Unit = {
                    val msg = WorkerMessage.newBuilder()
                        .setRequestId(requestId.incrementAndGet())
                        .setData(Data.newBuilder()
                            .setSessionId(session.id))
                        .build()
                    write(msg)
                }
            }, 300, 300)
        }

        def stop(): Unit = {
            timer.cancel()
            timer.purge()
            timer = null
        }
    }

    var benchmark = null
    override def startBenchmark(session: Session): Boolean = {
        log info s"Starting benchmark ${session.id}"
        MockBenchmarkRunner.start(session)
        true
    }

    override def stopBenchmark(): Unit = {
        log info "Stopping benchmark"
        MockBenchmarkRunner.stop()
    }
}
