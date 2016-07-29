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

package org.midonet.benchmark.tables

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{FileSystems, StandardOpenOption}
import java.util.concurrent.{Executors, TimeUnit}

import scala.util.control.NonFatal

import org.midonet.util.functors._

trait BenchmarkWriter {
    def append(buffer: ByteBuffer): Unit
    def close(): Unit
}

/*
class BenchmarkWriter(fileName: String) {

    val path = FileSystems.getDefault.getPath(fileName)
    val channel = FileChannel.open(
        path, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING)
    val executor = Executors.newSingleThreadExecutor()

    def append(buffer: ByteBuffer): Unit = {
        executor.submit(makeRunnable {
            try channel.write(buffer)
            catch { case NonFatal(_) => }
        })
    }

    def close(): Unit = {
        executor.shutdown()
        while (!executor.awaitTermination(600000, TimeUnit.MILLISECONDS)) {
            System.err.println(s"[bm-writer] Closing the output writer " +
                               s"$fileName timed out after 10 minutes")
        }
        channel.close()
    }
}
*/