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

import scopt.OptionParser

object GradientBoostedTree {

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

    val parser = new OptionParser[Params]("GBT"){
      head("GBT: an example of Gradient Boosted Tree for classification")
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
        .text("data path for Gradient Boosted Tree")
        .action((x,c) => c.copy(dataPath = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"Gradient Boosted Tree with $params")
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numClasses = params.numClasses
    val maxDepth = params.maxDepth
    val maxBins = params.maxBins
    val numIterations = params.numIterations
    val learningRate = params.learningRate

    // Load  data file.
    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a GradientBoostedTrees model.
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")
    boostingStrategy.numIterations = numIterations
    boostingStrategy.learningRate = learningRate
    boostingStrategy.treeStrategy.numClasses = numClasses
    boostingStrategy.treeStrategy.maxDepth = maxDepth
    boostingStrategy.treeStrategy.maxBins = maxBins
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)

    sc.stop()
  }
}
