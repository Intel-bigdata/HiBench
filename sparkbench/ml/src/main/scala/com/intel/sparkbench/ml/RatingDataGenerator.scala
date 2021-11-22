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
import org.apache.spark.{SparkConf, SparkContext}

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

      require(numRatings.toLong <= numUsers.toLong * numProducts.toLong,
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

    val ratingData = sc.parallelize(1 to numRatings, numPartitions)
      .mapPartitionsWithIndex { (index, p) =>
        val rng = new java.util.Random(index)
        p.map { _ =>
          // possible to generate duplicated (user, product), it does not affect the results
          val user = rng.nextInt(numUsers)
          val product = rng.nextInt(numProducts)
          // Use MovieLens ratings that are on a scale of 1-5
          // To map ratings to confidence scores, we use:
          //   5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5
          // See Spark example "MovieLensALS.scala" for details.
          val rating = if (implicitPrefs)
             (rng.nextInt(5)+1).toFloat - 2.5f
            else
             (rng.nextInt(5)+1).toFloat
          Rating(user, product, rating)
        }
      }

    ratingData.saveAsObjectFile(outputPath)

    sc.stop()
  }
}
