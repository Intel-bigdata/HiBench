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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.random._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.mllib.linalg.{Vectors, Vector}

object RatingDataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RatingDataGeneration")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numUsers: Int = 100
    var numProducts: Int = 100
    var sparsity: Double = 0.05
    var implicitPrefs: Boolean = false
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 5) {
      outputPath = args(0)
      numUsers = args(1).toInt
      numProducts = args(2).toInt
      sparsity = args(3).toDouble
      implicitPrefs = args(4).toBoolean

      println(s"Output Path: $outputPath")
      println(s"Num of Users: $numUsers")
      println(s"Num of Products: $numProducts")
      println(s"Sparsity: $sparsity")
      println(s"Implicit Prefs: $implicitPrefs")
    } else {
      System.err.println(
        s"Usage: $RatingDataGenerator <OUTPUT_PATH> <NUM_USERS> <NUM_PRODUCTS> <SPARSITY>  <IMPLICITPREFS>"
      )
      System.exit(1)
    }

    val rawData: RDD[Vector] = RandomRDDs.normalVectorRDD(sc, numUsers, numProducts, numPartitions)
    val rng = new java.util.Random()
    val data = rawData.map{v =>
      val a = Array.fill[Double](v.size)(0.0)
      v.foreachActive{(i,vi) =>
         if(rng.nextDouble <= sparsity){
           a(i) = vi
         }
      }
      Vectors.dense(a).toSparse
   }


    data.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
