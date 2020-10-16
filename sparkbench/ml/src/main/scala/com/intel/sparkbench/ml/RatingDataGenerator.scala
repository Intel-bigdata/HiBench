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
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.mllib.linalg.{Vector => OldVector}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RatingDataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RatingDataGeneration")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numUsers: Int = 100
    var numProducts: Int = 100
    var numRatings: Int = 10
    var implicitPrefs: Boolean = false
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 5) {
      outputPath = args(0)
      numUsers = args(1).toInt
      numProducts = args(2).toInt
      numRatings = args(3).toInt
      implicitPrefs = args(4).toBoolean

      require(numRatings <= numUsers * numProducts,
        s"RatingDataGenerator: numRatings=$numRatings is too large, should be <= numUsers * numProducts"
      )

      println(s"Output Path: $outputPath")
      println(s"Num of Users: $numUsers")
      println(s"Num of Products: $numProducts")
      println(s"Num of Ratings: $numRatings")
      println(s"Implicit Prefs: $implicitPrefs")
    } else {
      System.err.println(
        s"Usage: $RatingDataGenerator <OUTPUT_PATH> <NUM_USERS> <NUM_PRODUCTS> <NUM_RATINGS> <IMPLICITPREFS>"
      )
      System.exit(1)
    }

    // ratingID from 1 to numRatings
    val ratingData = sc.parallelize(1 to numRatings, numPartitions)
      .mapPartitionsWithIndex { case (_, iter) =>
        val rng = new java.util.Random()
        // ratingIDs in current partition
        val ratingIDs = iter.toArray
        // generate unique rating location (user, product) for each ratingID
        val ratingLocations = mutable.HashSet.empty[(Int, Int)]
        var i = 0
        while (i < ratingIDs.length) {
          val user = rng.nextInt(numUsers)
          val product = rng.nextInt(numProducts)
          if (!ratingLocations.contains((user, product))) {
            ratingLocations.add((user, product))
            i = i + 1
          }
        }

        ratingLocations.map { location =>
          val user = location._1
          val product = location._2
          val rating = if (implicitPrefs)
             (rng.nextInt(5)+1).toFloat - 2.5f
            else
             (rng.nextInt(5)+1).toFloat
          Rating(user, product, rating)
        }.toIterator
      }

    ratingData.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
