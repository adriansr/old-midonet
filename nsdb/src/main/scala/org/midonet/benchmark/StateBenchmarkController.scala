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

import java.util.{Timer, TimerTask}

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

import org.rogach.scallop.{ScallopConf, ScallopOption}

import org.midonet.benchmark.controller.Common._
import org.midonet.benchmark.controller.server.{StateBenchmarkControlServer, WorkerManager}

object StateBenchmarkController extends App {

    /*type Run = PartialFunction[Array[String], Unit]
    trait Command {
        def run: Run
        def help: String = "Unknown command arguments"
    }

    private val CommandSeparators = Array(' ', '\t', '\n')*/
    val opts = new ScallopConf(args) {
        val port = opt[Int]("port", short = 'p', default = Option(DefaultPort),
                            descr = "Controller port",
                            validate = _ > 0)
        val num = opt[Int]("num", short = 'n', default = Some(0),
                             descr = "Number of agents",
                           validate = _ > 0)
        val controller = opt[String]("controller", short = 'c', default = None,
                                     descr = "Controller server")
        val zkServers = opt[String]("zkservers", short = 'z', default = None,
                                    descr = "Comma-separated list of Zk servers")
        val clusters = opt[String]("clusters", short = 's', default = None,
                                   descr = "Comma-separated list of cluster servers")
        val verbose = opt[Boolean]("verbose", short = 'v', default = Some(false),
                                   descr = "Verbose mode")
    }

    def splitopt(opt: ScallopOption[String]): Seq[String] = {
        opt.get match {
            case Some(str) => str.split(',')
            case None => Seq()
        }
    }

    val session = Session(1,
                          opts.controller.get,
                          splitopt(opts.zkServers),
                          splitopt(opts.clusters))

    println(s"Using session = $session")

    if (!opts.verbose.get.get) {
        System.setProperty("logback.configurationFile", "logback-disabled.xml")
    }

    Runtime.getRuntime.addShutdownHook(new Thread("shutdown") {
        override def run(): Unit = {
            shutdown()
        }
    })

    val server = new StateBenchmarkControlServer(opts.port.get.get)
    server.serverChannelPromise.future onComplete {
        case Success(_) =>
        case Failure(err) =>
            println(s"Bind failed: $err")
            Runtime.getRuntime.exit(3)
    }

    abstract class UiTimer(delayMsecs: Int,
                           field: WorkerManager.Stats => Int,
                           target: Int,
                           name: String,
                           repeat: Boolean = false)
        extends TimerTask {

        private var started = false
        private val timer = new Timer(true)

        @throws[Exception]
        def start(): Unit = {
            if (started) throw new Exception("Timer already started")
            started = true
            if (repeat) {
                timer.schedule(this, delayMsecs, delayMsecs)
            } else {
                timer.schedule(this, delayMsecs)
            }
        }

        override def run(): Unit = {
            val count = field(server.manager.stats)
            if (count < target) {
                print(s"\rWaiting for $name ... [ $count/$target ] ")
                Console.out.flush()
            } else {
                stop()
                println(s"\rGot $target $name")
                complete()
            }
        }

        def complete(): Unit

        def stop(): Unit = {
            timer.cancel()
            timer.purge()
        }
    }

    val numAgents = opts.num.get.get
    val UiDelay = 500

    val stopTimer = new UiTimer(UiDelay,
                                - _.numRunning,
                                0,
                                "still running",
                                repeat = true) {
        override def complete(): Unit = {
            println("\nDone.")
            System.exit(0)
        }
    }

    val testTimer = new UiTimer(UiDelay,
                                _.numDataMessages,
                                100*numAgents,
                                "data packets",
                                repeat = true) {
        override def complete(): Unit = {
            server.manager.stopWorkers()
            println("\n *** Finished ***")
            stopTimer.start()
        }
    }

    val runTimer = new UiTimer(UiDelay,
                               _.numRunning,
                               numAgents,
                               "running agents",
                               repeat = true) {
        override def complete(): Unit = {
            println("\n *** Running ***")
            testTimer.start()
        }
    }

    val configureTimer = new UiTimer(UiDelay,
                                     _.numConfigured,
                                     numAgents,
                                     "configured agents",
                                     repeat = true) {
        override def complete(): Unit = {
            server.manager.startWorkers(numAgents)
            runTimer.start()
        }
    }

    val registeredTimer = new UiTimer(UiDelay,
                                      _.numRegistered,
                                      numAgents,
                                      "registered agents",
                                      repeat = true) {
        override def complete(): Unit = {
            server.manager.configure(session)
            configureTimer.start()
        }
    }

    registeredTimer.start()

    /*
        val options = new ScallopConf(args) {

            val run = RunCommand

            printedName = "mn-sbs"
            footer("Copyright (c) 2016 Midokura SARL, All Rights Reserved.")
        }

        def invalidEx =
            new Exception("invalid arguments, run with --help for usage information")

        val code = options.subcommand map {
            case subcommand: BenchmarkCommand =>
                Try(subcommand.run())
            case _ =>
                Failure(invalidEx)
        } getOrElse Failure(invalidEx) match {
            case Success(returnCode) =>
                returnCode
            case Failure(e) => e match {
                case _ if args.length == 0 =>
                    options.printHelp()
                case _ =>
                    System.err.println("[mn-sbs] Failed: " + e.getMessage)
            }
        }*/

    private def shutdown(): Unit = {

    }
}
