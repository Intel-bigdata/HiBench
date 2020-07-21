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

import org.apache.hadoop.io.LongWritable
import org.apache.mahout.math.VectorWritable
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scopt.OptionParser

case class Params(
  k: Int = -1,
  numIterations: Int = 10,
  storageLevel: String= "MEMORY_ONLY",
  input: String = null)

object GaussianMixtureModel {
  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("GaussianMixtureModel") {
      head(s"${this.getClass.getSimpleName}: An example demonstrating Gaussian Mixture Model (GMM)")
      opt[Int]('k', "k")
        .required()
        .text(s"number of clusters, required")
        .action((x, c) => c.copy(k = x))
      opt[Int]("numIterations")
        .text(s"number of iterations, default; ${defaultParams.numIterations}")
        .action((x, c) => c.copy(numIterations = x))
      opt[String]("storageLevel")
        .text(s"storage level, default: ${defaultParams.storageLevel}")
        .action((x, c) => c.copy(storageLevel = x))
      arg[String]("<input>")
        .text("input paths to examples")
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
    val spark = SparkSession
        .builder
        .appName(s"${this.getClass.getSimpleName}")
        .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val cacheStart = System.currentTimeMillis()

    // Loads data
    val data = sc.sequenceFile[LongWritable, VectorWritable](params.input)

    val storageLevel = StorageLevel.fromString(params.storageLevel)
    val examples = data.map { case (k, v) =>
      val vector: Array[Double] = new Array[Double](v.get().size)
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      // Should use Tuple1 to wrap around for calling toDF
      Tuple1(Vectors.dense(vector))
    }.toDF("features").persist(storageLevel)

    val numExamples = examples.count()

    println(s"Loading data time (ms) = ${System.currentTimeMillis() - cacheStart}")
    println(s"numExamples = $numExamples.")

    val trainingStart = System.currentTimeMillis()

    // Trains Gaussian Mixture Model
    val gmm = new GaussianMixture()
      .setK(params.k)
      .setMaxIter(params.numIterations)
    val model = gmm.fit(examples)

    println(s"Training time (ms) = ${System.currentTimeMillis() - trainingStart}")

    // output parameters of mixture model model
    for (i <- 0 until model.getK) {
      println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
          s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }

    spark.stop()
  }
}
