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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scopt.OptionParser

object SVDExample {

  case class Params(
    numFeatures: Int = 0,
    numSingularValues: Int = 0,
    computeU: Boolean = true,
    maxResultSize: String = "1g",
    dataPath: String = null
  )

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("SVD") {
      head("SVD: an example of SVD for matrix decomposition.")
      opt[Int]("numFeatures")
        .text(s"numFeatures, default: ${defaultParams.numFeatures}")
        .action((x,c) => c.copy(numFeatures = x))
      opt[Int]("numSingularValues")
        .text(s"numSingularValues, default: ${defaultParams.numSingularValues}")
        .action((x,c) => c.copy(numSingularValues = x))
      opt[Boolean]("computeU")
        .text(s"computeU, default: ${defaultParams.computeU}")
        .action((x,c) => c.copy(computeU = x))
      opt[String]("maxResultSize")
        .text(s"maxResultSize, default: ${defaultParams.maxResultSize}")
        .action((x,c) => c.copy(maxResultSize = x))
      arg[String]("<dataPath>")
        .required()
        .text("data path of SVD")
        .action((x,c) => c.copy(dataPath = x))
    }
     parser.parse(args, defaultParams) match {
       case Some(params) => run(params)
       case _ => sys.exit(1)
     }
  }

  def run(params: Params): Unit = {

    val conf = new SparkConf()
        .setAppName(s"SVD with $params")
        .set("spark.driver.maxResultSize", params.maxResultSize)
    val sc = new SparkContext(conf)

    val dataPath = params.dataPath
    val numFeatures = params.numFeatures
    val numSingularValues = params.numSingularValues
    val computeU = params.computeU

    val data: RDD[Vector] = sc.objectFile(dataPath) 
    val mat: RowMatrix = new RowMatrix(data)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(numSingularValues, computeU)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s  // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V  // The V factor is a local dense matrix.

    sc.stop()
  }
}
