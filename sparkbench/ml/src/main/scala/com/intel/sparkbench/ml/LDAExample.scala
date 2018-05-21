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

import org.apache.spark.mllib.clustering.{LDA, DistributedLDAModel, LocalLDAModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import scopt.OptionParser
object LDAExample {
  case class Params(
      inputPath: String = null,
      outputPath: String = null,
      numTopics: Int = 10,
      maxIterations: Int = 10,
      optimizer: String = "online",
      maxResultSize: String = "1g")
	  
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
	
    val parser = new OptionParser[Params]("LDA") {
	  head("LDA: an example app for LDA.")
	  opt[String]("optimizer")
        .text(s"optimizer, default: ${defaultParams.optimizer}")
        .action((x, c) => c.copy(optimizer = x))
      opt[String]("maxResultSize")
        .text("max resultSize, default: ${defaultParams.maxResultSize}")
        .action((x, c) => c.copy(maxResultSize = x))
      opt[Int]("numTopics")
        .text(s"number of Topics, default: ${defaultParams.numTopics}")
        .action((x, c) => c.copy(numTopics = x))
      opt[Int]("maxIterations")
        .text(s"number of max iterations, default: ${defaultParams.maxIterations}")
        .action((x, c) => c.copy(maxIterations = x))
      arg[String]("<inputPath>")
        .required()
        .text("Input paths")
        .action((x, c) => c.copy(inputPath = x))
      arg[String]("<outputPath>")
        .required()
        .text("outputPath paths")
        .action((x, c) => c.copy(outputPath = x))		

    }
	parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }
  
  def run(params: Params): Unit = {
    val conf = new SparkConf()
        .setAppName(s"LDA Example with $params")
        .set("spark.driver.maxResultSize", params.maxResultSize)
    val sc = new SparkContext(conf)

    val corpus: RDD[(Long, Vector)] = sc.objectFile(params.inputPath)
    
    // Cluster the documents into numTopics topics using LDA
    val ldaModel = new LDA().setK(params.numTopics).setMaxIterations(params.maxIterations).setOptimizer(params.optimizer).run(corpus)

    // Save and load model.
    ldaModel.save(sc, params.outputPath)
    val savedModel = LocalLDAModel.load(sc, params.outputPath)

    sc.stop()
  }
}
