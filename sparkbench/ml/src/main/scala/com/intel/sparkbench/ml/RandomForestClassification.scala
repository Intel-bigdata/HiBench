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
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import scopt.OptionParser

object RandomForestClassification {
  case class Params(
    inputPath: String = null,
    numTrees: Int = 3,
    numClasses: Int = 2,
    featureSubsetStrategy: String = "auto",
    impurity: String = "gini",
    maxDepth: Int = 4,
    maxBins: Int = 32)
	
  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("RF") {
      head("RF: an example app.")
      opt[Int]("numTrees")
        .text(s"numTrees, default: ${defaultParams.numTrees}")
        .action((x, c) => c.copy(numTrees = x))
      opt[Int]("numClasses")
        .text(s"numClasses, default: ${defaultParams.numClasses}")
        .action((x, c) => c.copy(numClasses = x))
      opt[Int]("maxDepth")
        .text(s"maxDepth, default: ${defaultParams.maxDepth}")
        .action((x, c) => c.copy(maxDepth = x))
      opt[Int]("maxBins")
        .text(s"maxBins, default: ${defaultParams.maxBins}")
        .action((x, c) => c.copy(maxBins = x))
      opt[String]("featureSubsetStrategy")
        .text(s"featureSubsetStrategy, default: ${defaultParams.featureSubsetStrategy}")
        .action((x, c) => c.copy(featureSubsetStrategy = x))
      opt[String]("impurity")
        .text(s"impurity (smoothing constant), default: ${defaultParams.impurity}")
        .action((x, c) => c.copy(impurity = x))
      arg[String]("<inputPath>")
        .required()
        .text("Input path of dataset")
        .action((x, c) => c.copy(inputPath = x))	
    }  
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }
  
  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"RFC with $params")
    val sc = new SparkContext(conf)

    // $example on$
    // Load and parse the data file.
    val data: RDD[LabeledPoint] = sc.objectFile(params.inputPath)

    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.

    val categoricalFeaturesInfo = Map[Int, Int]()

    val model = RandomForest.trainClassifier(trainingData, params.numClasses, categoricalFeaturesInfo,
      params.numTrees, params.featureSubsetStrategy, params.impurity, params.maxDepth, params.maxBins)

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
