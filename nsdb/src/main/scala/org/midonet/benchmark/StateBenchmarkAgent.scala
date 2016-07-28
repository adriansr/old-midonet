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

import java.util.concurrent.{Callable, Executor, ScheduledThreadPoolExecutor}

import scala.concurrent.ExecutionContext.Implicits.global

import io.netty.channel.nio.NioEventLoopGroup

import org.rogach.scallop.ScallopConf

import org.midonet.benchmark.controller.client.StateBenchmarkControlClient
import org.midonet.benchmark.controller.Common._
import org.midonet.cluster.services.discovery.MidonetServiceHostAndPort

object StateBenchmarkAgent extends App {

    //val DefaultZkTimeoutMillis = 10000

    val opts = new ScallopConf(args) {
        val server = opt[String]("server", short = 's', default = Option("localhost"),
                               descr = "Controller host")
        val port = opt[Int]("port", short = 'p', default = Option(DefaultPort),
                            descr = "Controller port")
    }

    val NumThreads = 2
    val executor = new ScheduledThreadPoolExecutor(NumThreads)
    val eventLoopGroup = new NioEventLoopGroup(1)

    executor.submit(new Runnable {
        override def run(): Unit = {
            for (i <- 1 to 2) {
                val client = new StateBenchmarkControlClient(
                    MidonetServiceHostAndPort(opts.server.get.get,
                                              opts.port.get.get),
                    executor,
                    eventLoopGroup)
                client.start()
            }
        }
    })
}
