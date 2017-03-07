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
package com.intel.hibench.sparkbench.ml

import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
// $example off$

object LogisticRegressionWithLBFGSiExamples {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogisticRegressionWithLBFGS")
    val sc = new SparkContext(conf)

    var inputPath = ""

    if (args.length == 2) {
      inputPath = args(0)
    }

    // $example on$
    // Load training data in LIBSVM format.
    // val data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
    val data = sc.textFile(inputPath).map { pairStr =>
      val pair = pairStr.substring(1, pairStr.length()-1).split(",",2)
      val labeledData = LabeledPoint(pair(0).toDouble, Vectors.dense(pair(1).substring(1, pair(1).length()-1).split(",").map(_.toDouble)))
      labeledData
    }

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    // Save and load model
    //model.save(sc, "target/tmp/scalaLogisticRegressionWithLBFGSModel"
    //val sameModel = LogisticRegressionModel.load(sc,
    //  "target/tmp/scalaLogisticRegressionWithLBFGSModel")
    // $example off$

    sc.stop()
  }
}
// scalastyle:on println
