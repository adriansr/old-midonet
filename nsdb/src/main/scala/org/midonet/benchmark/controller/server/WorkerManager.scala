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

import java.net.SocketAddress
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.Future

import com.typesafe.scalalogging.Logger

import io.netty.channel.Channel

import org.slf4j.LoggerFactory

import org.midonet.benchmark.Protocol._
import org.midonet.benchmark.Common._

object WorkerManager {

    case class Stats(numConnections: Int,
                     numRegistered: Int,
                     numConfigured: Int,
                     numRunning: Int,
                     numDataMessages: Int)

    type WorkerKey = SocketAddress
    private class Worker(channel: Channel) {

        val key = channel.remoteAddress()

        var isRegistered = false
        var sessionId: Option[Long] = None
        var isRunning = false

        var ackCallback: (Worker, RequestId) => Unit = null

        def close(): Future[AnyRef] = {
            channel.close().asScala
        }

        def send(msg: ControllerMessage): Future[AnyRef] = {
            channel.writeAndFlush(msg).asScala
        }
    }
}

class WorkerManager {

    import WorkerManager._

    private val log = Logger(LoggerFactory.getLogger("worker-manager"))

    private val clients = MutableMap.empty[WorkerKey, Worker]

    private val currentSession = new AtomicReference[TestRun]()
    private val requestId = new AtomicLong(0L)
    private val dataCounter = new AtomicInteger(0)

    def connected(key: WorkerKey, channel: Channel): Unit = {
        log debug s"Connected $key"
        clients synchronized {
            val worker = new Worker(channel)
            worker.ackCallback = spuriousAckHandler
            clients += (key -> new Worker(channel))
        }
    }

    def disconnected(key: WorkerKey): Boolean = {
        log debug s"Disconnected $key"
        // hold a lock to the map and to the entry while removing
        // so there is no race with getWorker()
        clients synchronized {
            clients.get(key) match {
                case Some(worker) => worker synchronized {
                    clients.remove(key).isDefined
                }
                case  None => false
            }
        }
    }

    private def spuriousAckHandler(worker: Worker, rid: RequestId): Unit = {
        log debug s"Spurious ack:$rid from worker:${worker.key}"
    }

    def ackReceived(key: WorkerKey, rid: RequestId): Boolean =
        getWorker(key) { worker =>
            if (worker.ackCallback != null) {
                worker.ackCallback(worker, rid)
                worker.ackCallback = spuriousAckHandler
            } else {
                spuriousAckHandler(worker, rid)
            }
        }

    def dataReceived(key: WorkerKey, rid: RequestId, data: Data): Boolean = {
        val sid = data.getSessionId
        getWorker(key) { worker =>
            if (worker.isRunning && worker.sessionId == Some(sid)) {
                //log debug s"Received data from $key"
                dataCounter.incrementAndGet()
            } else {
                log debug s"Ignored data from $key for old session:$sid"
            }
        }
    }

    def errorReceived(key: WorkerKey, rid: RequestId, error: Error): Boolean =
        getWorker(key) { worker =>
            val msg = if (error.hasReason) error.getReason else "(no reason)"
            log error s"Error from client:$key reason:$msg"
            worker.close()
        }

    def registerReceived(key: WorkerKey, rid: RequestId): Boolean =
        getWorker(key) { worker =>
            if (!worker.isRegistered) {
                worker.isRegistered = true
                worker.send(acknowledge(rid))
                log debug s"Registered client $key"
            } else {
                worker.send(terminate(rid, Some("Already registered")))
                worker.close()
            }
        }

    def unregisterReceived(key: WorkerKey, rid: RequestId): Boolean =
        getWorker(key) { worker =>
            if (worker.isRegistered) {
                worker.isRegistered = false
                worker.send(acknowledge(rid))
            } else {
                worker.send(terminate(rid, Some("Not registered")))
                worker.close()
            }
        }

    def stopReceived(key: WorkerKey, rid: RequestId): Boolean =
        getWorker(key) { worker =>
            if (worker.isRunning) {
                worker.isRunning = false
                worker.send(acknowledge(rid))
            } else {
                worker.send(terminate(rid, Some("Not running")))
                worker.close()
            }
        }

    private def getWorker(key: WorkerKey)(body: Worker => Any): Boolean = {
        def fetch(key: WorkerKey) = clients synchronized {
            clients.get(key)
        }

        fetch(key) match {
        case Some(worker) =>
            worker synchronized { body(worker) }
            true
        case None =>
            log error s"No such worker: $key"
            false
        }
    }

    private def acknowledge(rid: RequestId): ControllerMessage =
        ControllerMessage.newBuilder()
            .setRequestId(rid)
            .setAcknowledge(Acknowledge.getDefaultInstance)
            .build

    private def start(rid: RequestId, sid: SessionId): ControllerMessage =
        ControllerMessage.newBuilder()
            .setRequestId(rid)
            .setStart(Start.newBuilder().setSessionId(sid))
            .build

    private def stop(rid: RequestId, sid: SessionId): ControllerMessage =
        ControllerMessage.newBuilder()
            .setRequestId(rid)
            .setStop(Stop.newBuilder().setSessionId(sid))
            .build

    private def terminate(rid: RequestId,
                          reason: Option[String]): ControllerMessage = {
        val term = reason match {
            case Some(str) => Terminate.newBuilder()
                .setError(Error.newBuilder().setReason(str))
                .build()
            case None => Terminate.getDefaultInstance
        }
        ControllerMessage.newBuilder()
            .setRequestId(rid)
            .setTerminate(term)
            .build()
    }

    private def bootstrap(rid: RequestId,
                          session: TestRun): ControllerMessage = {
        val b = Bootstrap.newBuilder()
            .setSessionId(session.id)
        if (session.controller.isDefined) {
            b.setController(session.controller.get)
        }
        for (s <- session.zkServers) b.addZookeeperServers(s)
        for (s <- session.clusterServers) b.addClusterServers(s)

        ControllerMessage.newBuilder()
            .setRequestId(rid)
            .setBootstrap(b)
            .build()
    }

    private def clientSnapshot() = clients synchronized { clients.clone() }

    def stats: Stats = {
        val workers = clientSnapshot()
        val session = currentSession.get
        val target: Option[SessionId] = if (session!=null) Some(session.id) else None
        var numReg = 0
        var numConf = 0
        var numRunning = 0

        for (worker <- workers.values) worker synchronized {
            if (worker.isRegistered) {
                numReg += 1
                if (worker.sessionId == target) {
                    numConf += 1
                    if (worker.isRunning) {
                        numRunning += 1
                    }
                }
            }
        }

        Stats(workers.size, numReg, numConf, numRunning, dataCounter.get())
    }

    @tailrec
    final def configure(session: TestRun): Unit = {
        val prev = currentSession.get
        if (currentSession.compareAndSet(prev, session)) {
            dataCounter.set(0)
            val workers = clientSnapshot()

            // TODO: detect running
            val target = Some(session.id)
            val rid = requestId.incrementAndGet()
            val msg = bootstrap(rid, session)

            var sent = 0
            for (worker <- workers.values) worker synchronized {
                if (worker.isRegistered
                    && !worker.isRunning
                    && worker.sessionId != target) {

                    worker.ackCallback = (worker, recvRid) => {
                        if (recvRid == rid) {
                            worker.ackCallback = spuriousAckHandler
                            worker.sessionId = target
                        } else {
                            spuriousAckHandler(worker, recvRid)
                        }
                    }

                    worker.send(msg)
                    sent += 1
                }
            }
            log debug s"Sent $sent bootstrap requests"
        } else {
            configure(session)
        }
    }

    def startWorkers(num: Int): Unit = {
        val workers = clientSnapshot()

        val session = currentSession.get
        val target = Some(session.id)
        val rid = requestId.incrementAndGet()
        val msg = start(rid, session.id)

        var running = 0
        for (worker <- workers.values if running < num) worker synchronized {
            if (worker.isRegistered
                && worker.sessionId == target) {

                if (worker.isRunning) {
                    running += 1
                } else {
                    worker.ackCallback = (worker, recvRid) => {
                        if (recvRid == rid) {
                            worker.ackCallback = spuriousAckHandler
                            worker.isRunning = true
                        } else {
                            spuriousAckHandler(worker, recvRid)
                        }
                    }
                    worker.send(msg)
                    running += 1
                }
            }
        }
    }

    def stopWorkers(): Unit = {
        val workers = clientSnapshot()

        val session = currentSession.get
        val target = Some(session.id)
        val rid = requestId.incrementAndGet()
        val msg = stop(rid, session.id)

        for (worker <- workers.values) worker synchronized {
            if (worker.isRegistered
                && worker.sessionId == target
                && worker.isRunning) {

                worker.ackCallback = (worker, recvRid) => {
                    if (recvRid == rid) {
                        worker.ackCallback = spuriousAckHandler
                        worker.isRunning = false
                    } else {
                        spuriousAckHandler(worker, recvRid)
                    }
                }
                worker.send(msg)
            }
        }
    }
}
