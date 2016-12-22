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

package com.intel.hibench.sparkbench.streaming.application

import com.intel.hibench.common.streaming.UserVisitParser
import com.intel.hibench.sparkbench.streaming.util.SparkBenchConfig
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class StructuredWordCount() extends StructuredBenchBase {

  override def process(ds: DataFrame, config: SparkBenchConfig) = {

    // Get the singleton instance of SparkSession
    val spark = SparkSession.builder.appName("structured " + config.benchName).getOrCreate()
    import spark.implicits._

    // Project Line to UserVisit
    val words = ds.map(row => {
      val userVisit = UserVisitParser.parse(row.getAs[String]("value"))
      userVisit.getIp
    })

    val wordCounts = words.groupBy("value").count().orderBy($"count".desc)
    
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
