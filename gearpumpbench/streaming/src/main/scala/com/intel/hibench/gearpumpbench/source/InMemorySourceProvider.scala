/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.hibench.gearpumpbench.source

import com.intel.hibench.gearpumpbench.source.InMemorySourceProvider.InMemorySourceTask
import com.intel.hibench.gearpumpbench.util.GearpumpConfig
import org.apache.gearpump.streaming.Processor
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.Task
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.Message
import org.apache.gearpump.streaming.task.StartTime

class InMemorySourceProvider extends SourceProvider {
  override def getSourceProcessor(conf: GearpumpConfig): Processor[_ <: Task] = {
    Processor[InMemorySourceTask](conf.parallelism)
  }
}

object InMemorySourceProvider {

  class InMemorySourceTask(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
    val TEXT_TO_SPLIT =
      """
        |   Licensed to the Apache Software Foundation (ASF) under one
        |   or more contributor license agreements.  See the NOTICE file
        |   distributed with this work for additional information
        |   regarding copyright ownership.  The ASF licenses this file
        |   to you under the Apache License, Version 2.0 (the
        |   "License"); you may not use this file except in compliance
        |   with the License.  You may obtain a copy of the License at
        |
        |       http://www.apache.org/licenses/LICENSE-2.0
        |
        |   Unless required by applicable law or agreed to in writing, software
        |   distributed under the License is distributed on an "AS IS" BASIS,
        |   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        |   See the License for the specific language governing permissions and
        |   limitations under the License.
      """.stripMargin

    override def onStart(startTime: StartTime): Unit = {
      self ! Message("start")
    }

    var times = 0
    val MAX = 1000 * 1000

    override def onNext(msg: Message): Unit = {
      if (times < MAX) {
        TEXT_TO_SPLIT.lines.foreach { line =>
          taskContext.output(Message(line, System.currentTimeMillis()))
        }
        times += 1
        self ! Message("continue")
      }

    }
  }

}
