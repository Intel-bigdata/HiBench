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

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.recommendation.ALS.Rating
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object ALSExample {

  case class Params(
    dataPath: String = null,
    numIterations: Int = 10,
    lambda: Double = 0.1,
    rank: Int = 10,
    numUserBlocks: Int = 10,
    numItemBlocks: Int = 10,
    implicitPrefs: Boolean = false)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("ALS") {
      head("ALS: an example app for ALS on User-Item data.")
      opt[Int]("rank")
        .text(s"rank, default: ${defaultParams.rank}")
        .action((x, c) => c.copy(rank = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default: ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("lambda")
        .text(s"regularization parameter, default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      opt[Int]("numUserBlocks")
        .text(s"number of user blocks, default: ${defaultParams.numUserBlocks}")
        .action((x, c) => c.copy(numUserBlocks = x))
      opt[Int]("numProductBlocks")
        .text(s"number of product blocks, default: ${defaultParams.numItemBlocks}")
        .action((x, c) => c.copy(numItemBlocks = x))
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
    val spark = SparkSession
      .builder
      .appName(s"ALS with $params")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._

    val ratings = sc.objectFile[Rating[Int]](params.dataPath).toDF()

    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    // Build the recommendation model using ALS on the training data
    val als = new ALS()
      .setRank(params.rank)
      .setMaxIter(params.numIterations)
      .setRegParam(params.lambda)
      .setImplicitPrefs(params.implicitPrefs)
      .setNumUserBlocks(params.numUserBlocks)
      .setNumItemBlocks(params.numItemBlocks)
      .setUserCol("user")
      .setItemCol("item")
      .setRatingCol("rating")
    val model = als.fit(training)

    // Evaluate the model by computing the RMSE on the test data
    // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
    model.setColdStartStrategy("drop")
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)
    println(s"Root-mean-square error = $rmse")

    spark.stop()
  }
}
