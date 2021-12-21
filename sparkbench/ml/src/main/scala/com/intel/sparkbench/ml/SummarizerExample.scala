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

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scopt.OptionParser


object SummarizerExample {

  case class Params( input: String = null )

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("Summarizer") {
      head("Summarizer: an example app for computing Summarizer.")
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"SummarizerExample with $params")
      .getOrCreate()

    val sc = spark.sparkContext
    val training:RDD[Vector] = sc.objectFile(params.input)

    val summary: MultivariateStatisticalSummary = Statistics.colStats(training)
    println(summary.mean)
    println(summary.variance)
    println(summary.max)
    println(summary.min)

    spark.stop()
  }
}
// scalastyle:on println
