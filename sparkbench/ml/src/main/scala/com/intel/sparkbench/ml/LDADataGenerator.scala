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

package com.intel.hibench.sparkbench.ml

import com.intel.hibench.sparkbench.common.IOCommon

import java.util.Random

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.{HashMap => MHashMap}
import org.apache.spark.rdd.RDD

/**
 * :: DeveloperApi ::
 * Generate test data for LDA (Latent Dirichlet Allocation).
 * Each document is an example with a docId and a vector.
 * The vector is of length vocabulary size and each entry
 * represents the number of the corresponding word appearing.
 */
object LDADataGenerator {

  /**
   * Generate an RDD containing test data for LDA.
   *
   * @param sc SparkContext to use for creating the RDD.
   * @param numDocs Number of documents that will be contained in the RDD.
   * @param numVocab Vocabulary size to generate for each document.
   * @param docLenMin Minimum document length.
   * @param docLenMax Maximum document length.
   * @param numParts Number of partitions of the generated RDD. Default value is 3.
   * @param seed Random seed for each partition.
   */
  def generateLDARDD(
    sc: SparkContext,
    numDocs: Long,
    numVocab: Int,
    docLenMin: Int,
    docLenMax: Int,
    numParts: Int = 3,
    seed: Long = System.currentTimeMillis()): RDD[(Long, Vector)] = {
    val data = sc.parallelize(0L until numDocs, numParts).mapPartitionsWithIndex { 
      (idx, part) =>
        val rng = new Random(seed ^ idx)
        part.map { case docIndex =>
          var currentSize = 0
          val entries = MHashMap[Int, Int]()
          val docLength = rng.nextInt(docLenMax - docLenMin + 1) + docLenMin
          while (currentSize < docLength) {
            val index = rng.nextInt(numVocab)
            entries(index) = entries.getOrElse(index, 0) + 1
            currentSize += 1
          }

          val iter = entries.toSeq.map(v => (v._1, v._2.toDouble))
          (docIndex, Vectors.sparse(numVocab, iter))
       }
    }
    data
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LDADataGenerator")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numDocs: Long = 500L
    var numVocab: Int = 1000
    var docLenMin: Int = 50
    var docLenMax: Int = 10000
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 5) {
      outputPath = args(0)
      numDocs = args(1).toInt
      numVocab = args(2).toInt
      docLenMin = args(3).toInt
      docLenMax = args(4).toInt
      println(s"Output Path: $outputPath")
      println(s"Num of Documents: $numDocs")
      println(s"Vocabulary size: $numVocab")
    } else {
      System.err.println(
        s"Usage: $LDADataGenerator <OUTPUT_PATH> <NUM_DOCUMENTS> <VOCABULARY_SIZE>"
      )
      System.exit(1)
    }

    val data = generateLDARDD(sc, numDocs, numVocab, docLenMin, docLenMax, numPartitions)

    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
