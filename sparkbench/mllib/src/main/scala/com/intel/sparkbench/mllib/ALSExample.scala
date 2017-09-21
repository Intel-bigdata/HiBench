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
package com.intel.hibench.sparkbench.mllib

import scala.collection.mutable

import org.apache.log4j.{Level, Logger}
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

object ALSTest {

  case class Params(
      dataPath: String = null,
      numUsers: Int = 0,
      numProducts: Int = 0,
      sparsity: Double = 0.01,
      kryo: Boolean = false,
      numIterations: Int = 20,
      lambda: Double = 1.0,
      rank: Int = 10,
      numUserBlocks: Int = -1,
      numProductBlocks: Int = -1,
      implicitPrefs: Boolean = false)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ALS") {
      head("ALS: an example app for ALS on User-Product data.")
      opt[Int]("numUsers")
        .text(s"numUsers, default: ${defaultParams.numUsers}")
        .action((x, c) => c.copy(numUsers = x))
      opt[Int]("numProducts")
        .text(s"numProducts, default: ${defaultParams.numProducts}")
        .action((x, c) => c.copy(numProducts = x))
      opt[Double]("sparsity")
        .text(s"sparsity, default: ${defaultParams.sparsity}")
        .action((x, c) => c.copy(sparsity = x))
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Boolean]("kryo")
        .text("Kryo serialization, default: ${defaultParams.kryo}")
        .action((x, c) => c.copy(kryo = x))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks} (auto)")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numProductBlocks} (auto)")
        .action((x, c) => c.copy(numProductBlocks = x))
      opt[Boolean]("implicitPrefs")
        .text("implicit preference, default: ${defaultParams.implicitPrefs}")
        .action((x, c) => c.copy(implicitPrefs = x))
      arg[String]("<dataPath>")
        .required()
        .text("Input paths to a User-Product dataset of ratings")
        .action((x, c) => c.copy(dataPath = x))	
    }  
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"ALS with $params")
    if (params.kryo) {
      conf.registerKryoClasses(Array(classOf[mutable.BitSet], classOf[Rating]))
        .set("spark.kryoserializer.buffer", "8m")
    }
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val numUsers = params.numUsers
    val numProducts = params.numProducts
    val implicitPrefs = params.implicitPrefs

    val rawdata: RDD[Vector] = sc.objectFile(params.dataPath)
    val data: RDD[Rating] = Vector2Rating(rawdata, numUsers, numProducts)
    val training: RDD[Rating] = data.randomSplit(Array(1-params.sparsity, params.sparsity))(0).cache()
    val testsparsity = 0.75 * (1-params.sparsity)
    val test: RDD[Rating] = data.randomSplit(Array(1-testsparsity, testsparsity))(0)

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    val model = new ALS()
      .setRank(params.rank)
      .setIterations(params.numIterations)
      .setLambda(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setUserBlocks(params.numUserBlocks)
      .setProductBlocks(params.numProductBlocks)
      .run(training)

    val rmse = computeRmse(model, test, params.implicitPrefs)

    println(s"Test RMSE = $rmse.")

    sc.stop()
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], implicitPrefs: Boolean)
    : Double = {

    def mapPredictedRating(r: Double): Double = {
      if (implicitPrefs) math.max(math.min(r, 1.0), 0.0) else r
    }

    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map{ x =>
      ((x.user, x.product), mapPredictedRating(x.rating))
    }.join(data.map(x => ((x.user, x.product), x.rating))).values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).mean())
  }

  def Vector2Rating(rawdata: RDD[Vector], m: Int, n: Int)
    : RDD[Rating] = {
    
    val Ratingdata: RDD[Rating] = rawdata.zipWithIndex().flatMap(v => {
      var arr = new Array[Rating](n)
      var i = 0
      for (i <- 0 until n){
        arr(i) = Rating(v._2.toInt, i, v._1.apply(i))
      }
      arr
    })
    Ratingdata
  }

}
// scalastyle:on println
