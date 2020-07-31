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
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.feature.{LabeledPoint => NewLabeledPoint}
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object XGBoost {

  case class Params(
    numClasses: Int = 2,
    maxDepth: Int = 30,
    maxBins: Int = 32,
    numIterations: Int = 20,
    learningRate: Double = 0.1,
    dataPath: String = null
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("XGBoost"){
      head("XGBoost: use XGBoost for classification")
      opt[Int]("numClasses")
        .text(s"numClasses, default: ${defaultParams.numClasses}")
        .action((x,c) => c.copy(numClasses = x))
      opt[Int]("maxDepth")
        .text(s"maxDepth, default: ${defaultParams.maxDepth}")
        .action((x,c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"maxBins, default: ${defaultParams.maxBins}")
        .action((x,c) => c.copy(maxBins = x))
      opt[Int]("numIterations")
        .text(s"numIterations, default: ${defaultParams.numIterations}")
        .action((x,c) => c.copy(numIterations = x))
      opt[Double]("learningRate")
        .text(s"learningRate, default: ${defaultParams.learningRate}")
        .action((x,c) => c.copy(learningRate = x))
      arg[String]("<dataPath>")
        .required()
        .text("data path for XGBoost")
        .action((x,c) => c.copy(dataPath = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"XGBoost with $params")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val dataPath = params.dataPath
    val numClasses = params.numClasses
    val maxDepth = params.maxDepth
    val maxBins = params.maxBins
    val numIterations = params.numIterations
    val learningRate = params.learningRate

    // Load data file.
    val mllibRDD: RDD[LabeledPoint] = sc.objectFile(dataPath)
    // Convert to ML LabeledPoint and to DataFrame
    val mlRDD: RDD[NewLabeledPoint] = mllibRDD.map { p => NewLabeledPoint(p.label, p.features.asML) }
    val data = mlRDD.toDF

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    val numWorkers = sc.getConf.getInt("spark.executor.instances", -1)
    val numThreads = sc.getConf.getInt("spark.executor.cores", -1)
    val taskCPUs = sc.getConf.getInt("spark.task.cpus", -1)

    if (numWorkers == -1 || numThreads == -1 || taskCPUs == -1) {
      println("XGBoost error: should set spark.executor.instances, " +
        "spark.executor.cores and spark.task.cpus in Spark Config")
      sys.exit(1)
    }

    val xgbParam = Map("eta" -> learningRate,
      "num_round" -> numIterations,
      "eta" -> learningRate,
      "num_class" -> numClasses,
      "max_depth" -> maxDepth,
      "max_bin" -> maxBins,
      "objective" -> "multi:softprob",
      "num_workers" -> numWorkers,
      "nthread" -> numThreads
    )
    val xgbClassifier = new XGBoostClassifier(xgbParam).
      setFeaturesCol("features").
      setLabelCol("label")

    val model = xgbClassifier.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1.0 - accuracy}")

    sc.stop()
  }
}
