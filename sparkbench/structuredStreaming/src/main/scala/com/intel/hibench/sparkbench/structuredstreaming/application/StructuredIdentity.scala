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

package com.intel.hibench.sparkbench.structuredstreaming.application

import com.intel.hibench.common.streaming.metrics.KafkaReporter
import com.intel.hibench.sparkbench.structuredstreaming.util.SparkBenchConfig
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class StructuredIdentity() extends StructuredBenchBase {

  override def process(ds: DataFrame, config: SparkBenchConfig) = {

    // Get the singleton instance of SparkSession
    val spark = SparkSession.builder.appName("structured " + config.benchName).getOrCreate()
    import spark.implicits._

    val query = ds.writeStream
      .foreach(new ForeachWriter[Row] {
        var reporter: KafkaReporter = _

        def open(partitionId: Long, version: Long): Boolean = {
          val reportTopic = config.reporterTopic
          val brokerList = config.brokerList
          reporter = new KafkaReporter(reportTopic, brokerList)
          true
        }

        def close(errorOrNull: Throwable): Unit = {}

        def process(record: Row): Unit = {
          val inTime = record(0).asInstanceOf[String].toLong
          val outTime = System.currentTimeMillis()
          reporter.report(inTime, outTime)
        }
      })
      .start()

    query.awaitTermination()
  }
}
