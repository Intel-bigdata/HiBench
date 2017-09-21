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

/*
 * Copied from org.apache.spark.examples.mllib.SparkNaiveBayes
 */
package org.apache.spark.examples.mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._

/**
 * An example naive Bayes app. Run with
 * {{{
 * ./bin/run-example org.apache.spark.examples.mllib.SparseNaiveBayes [options] <input>
 * }}}
 * If you use it as a template to create your own app, please use `spark-submit` to submit your app.
 */
object SparseNaiveBayes {

  case class Params(
      input: String = null,
      minPartitions: Int = 0,
      numFeatures: Int = -1,
      lambda: Double = 1.0)

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("SparseNaiveBayes") {
      head("SparseNaiveBayes: an example naive Bayes app for LIBSVM data.")
      opt[Int]("numPartitions")
        .text("min number of partitions")
        .action((x, c) => c.copy(minPartitions = x))
      opt[Int]("numFeatures")
        .text("number of features")
        .action((x, c) => c.copy(numFeatures = x))
      opt[Double]("lambda")
        .text(s"lambda (smoothing constant), default: ${defaultParams.lambda}")
        .action((x, c) => c.copy(lambda = x))
      arg[String]("<input>")
        .text("input paths to labeled examples in LIBSVM format")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val conf = new SparkConf().setAppName(s"SparseNaiveBayes with $params")
    val sc = new SparkContext(conf)

//    Logger.getRootLogger.setLevel(Level.WARN)

    val minPartitions =
      if (params.minPartitions > 0) params.minPartitions else sc.defaultMinPartitions

    // Generate vectors according to input documents
    val data = sc.sequenceFile[Text, Text](params.input).map{case (k, v) => (k.toString, v.toString)}
    val wordCount = data
      .flatMap{ case (key, doc) => doc.split(" ")}
      .map((_, 1L))
      .reduceByKey(_ + _)
    val wordSum = wordCount.map(_._2).reduce(_ + _)
    val wordDict = wordCount.zipWithIndex()
      .map{case ((key, count), index) => (key, (index.toInt, count.toDouble / wordSum)) }
      .collectAsMap()
    val sharedWordDict = sc.broadcast(wordDict)

    // for each document, generate vector based on word freq
    val vector = data.map { case (dockey, doc) =>
      val docVector = doc.split(" ").map(x => sharedWordDict.value(x)) //map to word index: freq
        .groupBy(_._1) // combine freq with same word
        .map { case (k, v) => (k, v.map(_._2).sum)}

      val (indices, values) = docVector.toList.sortBy(_._1).unzip
      val label = dockey.substring(6).head.toDouble
      (label, indices.toArray, values.toArray)
    }

    val d = if (params.numFeatures > 0){
      params.numFeatures
    } else {
      vector.persist(StorageLevel.MEMORY_ONLY)
      vector.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    val examples = vector.map{ case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))
    }

    // Cache examples because it will be used in both training and evaluation.
    examples.cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0)
    val test = splits(1)

    val numTraining = training.count()
    val numTest = test.count()

    println(s"numTraining = $numTraining, numTest = $numTest.")

    val model = new NaiveBayes().setLambda(params.lambda).run(training)

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))
    val accuracy = predictionAndLabel.filter(x => x._1 == x._2).count().toDouble / numTest

    println(s"Test accuracy = $accuracy.")

    sc.stop()
  }
}
