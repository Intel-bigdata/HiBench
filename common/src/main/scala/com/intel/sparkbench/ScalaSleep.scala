// Copyright (C) 2011-2012 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.intel.sparkbench.sleep

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaSleep{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaSleep <parallel> <seconds>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSleep")
    val sc = new SparkContext(sparkConf)

    val parallel = args(0).toInt
    val seconds  = args(1).toInt
    val workload = sc.parallelize(1 to parallel, parallel).map(x=> Thread.sleep(seconds * 1000L))
    workload.collect()
    sc.stop()
  }
}
