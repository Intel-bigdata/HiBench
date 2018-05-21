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
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.rdd.RDD

import scopt.OptionParser

object LinearRegression {

  case class Params(
      dataPath: String = null,
      numIterations: Int = 100,
      stepSize: Double = 0.00000001
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Linear"){
      head("Linear Regression: an example of linear regression with SGD optimizer")
      opt[Int]("numIterations")
        .text(s"numIterations, default: ${defaultParams.numIterations}")
        .action((x,c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text(s"stepSize, default: ${defaultParams.stepSize}")
        .action((x,c) => c.copy(stepSize = x))
      arg[String]("<dataPath>")
        .required()
        .text("Input path for data")
        .action((x,c) => c.copy(dataPath = x))
    }
    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }
   
  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"LinearRegressionWithSGD with $params")
    val sc = new SparkContext(conf)
    
    val dataPath = params.dataPath
    val numIterations = params.numIterations
    val stepSize = params.stepSize

    // Load training data in LabeledPoint format.
    val data: RDD[LabeledPoint] = sc.objectFile(dataPath)
    
    // Building the model
    val model = LinearRegressionWithSGD.train(data, numIterations, stepSize)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = data.map { point =>
      val prediction = model.predict(point.features)
        (point.label, prediction)
    }
    val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
    println("Training Mean Squared Error = " + MSE)

    sc.stop()
  }
}
