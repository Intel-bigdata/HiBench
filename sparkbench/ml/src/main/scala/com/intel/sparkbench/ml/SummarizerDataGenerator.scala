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

package com.intel.sparkbench.ml

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * :: DeveloperApi ::
 * Generate test data for Correlation. This class chooses positive labels
 * with probability `probOne` and scales features for positive examples by `eps`.
 */
object SummarizerDataGenerator {

  /**
   * Generate an RDD containing test data for Correlation.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param nexamples Number of examples that will be contained in the RDD.
   * @param nfeatures Number of features to generate for each example.
   * @param eps Epsilon factor by which positive examples are scaled.
   * @param nparts Number of partitions of the generated RDD. Default value is 2.
   * @param probOne Probability that a label is 1 (and not 0). Default value is 0.5.
   */
  def generateDistributedRowMatrix (
                                    sc: SparkContext,
                                    m: Long,
                                    n: Int,
                                    numPartitions: Int,
                                    seed: Long = System.currentTimeMillis()): RDD[Vector] = {
    val data: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, m, n, numPartitions, seed)

    data
  }

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"SummarizerDataGenerator")
      .getOrCreate()
    val sc = spark.sparkContext

    var outputPath = ""
    var numExamples: Int = 200000
    var numFeatures: Int = 20
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt
    val eps = 3

    if (args.length == 3) {
      outputPath = args(0)
      numExamples = args(1).toInt
      numFeatures = args(2).toInt
      println(s"Output Path: $outputPath")
      println(s"Num of Examples: $numExamples")
      println(s"Num of Features: $numFeatures")
    } else {
      System.err.println(
        s"Usage: $SummarizerDataGenerator <OUTPUT_PATH> <NUM_EXAMPLES> <NUM_FEATURES>"
      )
      System.exit(1)
    }

    val data: RDD[Vector] = generateDistributedRowMatrix(sc, numExamples, numFeatures, numPartitions)

    data.saveAsObjectFile(outputPath)

    spark.stop()
  }
}
