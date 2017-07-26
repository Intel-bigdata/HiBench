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

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object LDAExample {

  def main(args: Array[String]): Unit = {
    var inputPath = ""
    var outputPath = ""
    var numTopics: Int = 10

    if (args.length == 3) {
      inputPath = args(0)
      outputPath = args(1)
      numTopics = args(2).toInt
    }

    val conf = new SparkConf().setAppName("LDA Example")
    val sc = new SparkContext(conf)

    val corpus: RDD[(Long, Vector)] = sc.objectFile(inputPath)
    
    // Cluster the documents into numTopics topics using LDA
    val ldaModel = new LDA().setK(numTopics).run(corpus)

    // Save and load model.
    ldaModel.save(sc, outputPath)
    val sameModel = DistributedLDAModel.load(sc, outputPath)

    sc.stop()
  }
}
