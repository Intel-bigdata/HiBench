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
package com.intel.hibench.streambench.gearpump.task

import com.intel.hibench.streambench.gearpump.util.GearpumpConfig
import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.streaming.task.{Task, TaskContext}

class Numeric(taskContext: TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val benchConf = conf.getValue[GearpumpConfig](GearpumpConfig.BENCH_CONFIG).get
  private val fieldIndex = benchConf.fieldIndex
  private val separator = benchConf.separator
  private var max: Long = 0
  private var min: Long = Long.MaxValue

  override def onNext(msg: Message): Unit = {
    val fields = msg.msg.asInstanceOf[String].trim.split(separator)
    if (fields.length > fieldIndex) {
      val value = fields(fieldIndex).toLong
      if (value > max) max = value
      if (value < min) min = value
      taskContext.output(Message((max, min, value, 1L), System.currentTimeMillis()))
    }
  }
}
