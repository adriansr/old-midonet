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

package org.midonet.benchmark

import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

import io.netty.channel.{Channel, ChannelFuture, ChannelFutureListener}

import org.midonet.benchmark.Protocol.Bootstrap

object Common {
    type RequestId = Long
    type SessionId = Long

    val DefaultPort = 10000
    val DefaultReconnectionDelay = 3 seconds

    case class TestRun(id: SessionId,
                       controller: Option[String],
                       zkServers: Seq[String],
                       clusterServers: Seq[String],
                       duration: Option[Int],
                       table: Option[String],
                       tableCount: Option[Int],
                       entryCount: Option[Int],
                       writeRate: Option[Int],
                       dumpFile: Option[String])

    implicit def toWrapper(msg: Bootstrap): BootstrapWrapper = {
        new BootstrapWrapper(msg)
    }

    class BootstrapWrapper(val msg: Bootstrap) extends AnyVal {

        def toTestRun: TestRun = {
            def toSeq[T](n: Int, fn: Int => T): Seq[T] =
                for (i <- 0 until n) yield fn(i)
            def toOpt[T](checker: => Boolean,
                         getter: => T): Option[T] = {
                if (checker) Some(getter) else None
            }

            TestRun(msg.getSessionId,
                    toOpt(msg.hasController, msg.getController),
                    toSeq(msg.getZookeeperServersCount,
                          msg.getZookeeperServers),
                    toSeq(msg.getClusterServersCount, msg.getClusterServers),
                    toOpt(msg.hasDuration, msg.getDuration),
                    toOpt(msg.hasTable, msg.getTable),
                    toOpt(msg.hasTableCount, msg.getTableCount),
                    toOpt(msg.hasEntryCount, msg.getEntryCount),
                    toOpt(msg.hasWriteRate, msg.getWriteRate),
                    toOpt(msg.hasDumpFile, msg.getDumpFile))
        }
    }

    implicit def toWrapper(f: ChannelFuture): ChannelFutureWrapper = {
        new ChannelFutureWrapper(f)
    }

    class ChannelFutureWrapper(val f: ChannelFuture) extends AnyVal {

        /**
          * @return Wraps the current [[ChannelFuture]] as a Scala [[Future]]
          *         that completes with the [[Channel]].
          */
        def asScala: Future[Channel] = {
            val promise = Promise[Channel]()
            f.addListener(new ChannelFutureListener {
                override def operationComplete(future: ChannelFuture): Unit = {
                    if (future.isSuccess) promise trySuccess future.channel()
                    else promise tryFailure future.cause()
                }
            })
            promise.future
        }
    }

    trait BenchmarkRunner {
        def start(session: TestRun): Unit
        def stop(): Unit
    }
}
