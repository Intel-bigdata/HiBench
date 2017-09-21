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

package com.intel.hibench.sparkbench.mllib

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

object SVDTest {

  def main(args: Array[String]): Unit = {
    var inputPath = ""
    var numFeatures = 0
    var maxResultSize = "1g"

    if (args.length == 3) {
      inputPath = args(0)
      numFeatures = args(1).toInt
      maxResultSize = args(2)
    }

    val conf = new SparkConf()
        .setAppName("SVDTest")
        .set("spark.driver.maxResultSize",maxResultSize)
    val sc = new SparkContext(conf)

    val dataRDD: RDD[Vector] = sc.objectFile(inputPath) 
    val mat: RowMatrix = new RowMatrix(dataRDD)

    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(numFeatures-1, computeU = true)
    val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
    val s: Vector = svd.s  // The singular values are stored in a local dense vector.
    val V: Matrix = svd.V  // The V factor is a local dense matrix.

    sc.stop()
  }
}
