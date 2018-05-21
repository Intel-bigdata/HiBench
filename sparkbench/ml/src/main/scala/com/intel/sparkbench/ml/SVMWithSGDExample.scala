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
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint

import scopt.OptionParser

object SVMWithSGDExample {

   case class Params(
     numIterations: Int = 100,
     stepSize: Double = 1.0,
     regParam: Double = 0.01,
     dataPath: String = null
   )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SVM") {
      head("SVM: an example of SVM for classification.")
      opt[Int]("numIterations")
        .text(s"numIterations, default: ${defaultParams.numIterations}")
        .action((x,c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text(s"stepSize, default: ${defaultParams.stepSize}")
        .action((x,c) => c.copy(stepSize = x))
      opt[Double]("regParam")
        .text(s"regParam, default: ${defaultParams.regParam}")
        .action((x,c) => c.copy(regParam = x))
      arg[String]("<dataPath>")
        .required()
        .text("data path of SVM")
        .action((x, c) => c.copy(dataPath = x)) 
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {

    val conf = new SparkConf().setAppName(s"SVM with $params")
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numIterations = params.numIterations
    val stepSize = params.stepSize
    val regParam = params.regParam

    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = SVMWithSGD.train(training, numIterations, stepSize, regParam)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)

    sc.stop()
  }
}
