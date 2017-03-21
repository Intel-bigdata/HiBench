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

import scala.collection.mutable

object RatingDataGenerator {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RatingDataGeneration")
    val sc = new SparkContext(conf)

    var outputPath = ""
    var numUsers: Int = 100
    var numProducts: Int = 100
    var numRatings: Long = 100
    var implicitPrefs: Boolean = false
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)
    val numPartitions = IOCommon.getProperty("hibench.default.shuffle.parallelism")
      .getOrElse((parallel / 2).toString).toInt

    if (args.length == 5) {
      outputPath = args(0)
      numUsers = args(1).toInt
      numProducts = args(2).toInt
      numRatings = args(3).toLong
      implicitPrefs = args(4).toBoolean

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

    val data = generateRatings(sc, numUsers, numProducts,
      numRatings, implicitPrefs, numPartitions)

    data._1.saveAsObjectFile(outputPath+"/Train")
    data._2.saveAsObjectFile(outputPath+"/Test")

    sc.stop()
  }

  // Problems with having a userID or productID in the test set but not training set
  // leads to a lot of work...
  def generateRatings(
      sc: SparkContext,
      numUsers: Int,
      numProducts: Int,
      numRatings: Long,
      implicitPrefs: Boolean,
      numPartitions: Int,
      seed: Long = System.currentTimeMillis()): (RDD[Rating],RDD[Rating]) = {

    val train = RandomRDDs.randomRDD(sc,
      new RatingGenerator(numUsers, numProducts,implicitPrefs),
      numRatings, numPartitions, seed).cache()

    val test = RandomRDDs.randomRDD(sc,
      new RatingGenerator(numUsers, numProducts,implicitPrefs),
      math.ceil(numRatings * 0.25).toLong, numPartitions, seed + 24)

    // Now get rid of duplicate ratings and remove non-existant userID's
    // and prodID's from the test set
    val commons: PairRDDFunctions[(Int,Int),Rating] =
      new PairRDDFunctions(train.keyBy(rating => (rating.user, rating.product)).cache())

    val exact = commons.join(test.keyBy(rating => (rating.user, rating.product)))

    val trainPruned = commons.subtractByKey(exact).map(_._2).cache()

    // Now get rid of users that don't exist in the train set
    val trainUsers: RDD[(Int,Rating)] = trainPruned.keyBy(rating => rating.user)
    val testUsers: PairRDDFunctions[Int,Rating] =
      new PairRDDFunctions(test.keyBy(rating => rating.user))
    val testWithAdditionalUsers = testUsers.subtractByKey(trainUsers)

    val userPrunedTestProds: RDD[(Int,Rating)] =
      testUsers.subtractByKey(testWithAdditionalUsers).map(_._2).keyBy(rating => rating.product)

    val trainProds: RDD[(Int,Rating)] = trainPruned.keyBy(rating => rating.product)

    val testWithAdditionalProds =
      new PairRDDFunctions[Int, Rating](userPrunedTestProds).subtractByKey(trainProds)
    val finalTest =
      new PairRDDFunctions[Int, Rating](userPrunedTestProds).subtractByKey(testWithAdditionalProds)
        .map(_._2)

    (trainPruned, finalTest)
  }

}

class RatingGenerator(
    private val numUsers: Int,
    private val numProducts: Int,
    private val implicitPrefs: Boolean) extends RandomDataGenerator[Rating] {

  private val rng = new java.util.Random()

  private val observed = new mutable.HashMap[(Int, Int), Boolean]()

  override def nextValue(): Rating = {
    var tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    while (observed.getOrElse(tuple,false)){
      tuple = (rng.nextInt(numUsers),rng.nextInt(numProducts))
    }
    observed += (tuple -> true)

    val rating = if (implicitPrefs) rng.nextInt(2)*1.0 else rng.nextDouble()*5

    new Rating(tuple._1, tuple._2, rating)
  }

  override def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  override def copy(): RatingGenerator = new RatingGenerator(numUsers, numProducts, implicitPrefs)
}
