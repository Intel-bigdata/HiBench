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

package com.intel.hibench.streambench.common

import org.slf4j.LoggerFactory

/**
  * @deprecated replace by LatencyReporter
  */
@deprecated
class Logger(platForm: Platform.Platform, testCase: TestCase.TestCase) {
  val prefix = s"${platForm}-${testCase}"
  private val logger = LoggerFactory.getLogger(classOf[Logger])

  def logThroughput(throughput: String) = {
    logger.info(s"${prefix}: Throughput: ${throughput} records/s")
  }

  def logAvgLatency(time: String) = {
    logger.info(s"${prefix}: Average Latency: ${time} ms")
  }

  def logLatency(recordNum: Long, time: String) = {
    logger.info(s"${prefix}: Latency: ${time} ms of ${recordNum} records")
  }

  def logConfig(name: String, value: String) = {
    logger.info(s"${prefix}: Config: ${name}=${value}")
  }

  //N.B: All log printed by logMsg will not be shown in fininal report
  def logMsg(msg: String) = logger.info(s"${prefix}: Message: ${msg}")
}
