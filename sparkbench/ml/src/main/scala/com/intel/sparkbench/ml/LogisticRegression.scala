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

// scalastyle:off println
package com.intel.hibench.sparkbench.ml

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.sql.SparkSession

object LogisticRegression {

  def main(args: Array[String]): Unit = {
    var inputPath = ""
    var numFeatures = 0
    var aggDepth = 0
    if (args.length == 3) {
      inputPath = args(0)
      numFeatures = args(1).toInt
      aggDepth = if (args(2).toInt < 2) 2 else args(2).toInt
    }
    val spark = SparkSession.builder.appName("LogisticRegression").getOrCreate()
    val df = spark.read.format("libsvm")
                       .option("numFeatures", numFeatures)
                       .load(inputPath)
    // Run training algorithm to build the model
    val model = new LogisticRegression()
      .setMaxIter(30)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)
      .setAggregationDepth(aggDepth)
      .fit(df)
    println(s"training complete!")
    spark.stop()
  }
}
// scalastyle:on println
