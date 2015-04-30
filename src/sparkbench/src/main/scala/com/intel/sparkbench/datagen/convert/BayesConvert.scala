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


package com.intel.sparkbench.datagen.convert

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object BayesConvert{
  val conf = new Configuration()
  def main(args: Array[String]){
    if (args.length!=1){
      System.err.println("Usage: Convert <input_path>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("HiBench Bayes Converter")
    val sc = new SparkContext(sparkConf)

    val input_path =   args(0)  //"hdfs://localhost:54310/HiBench/Bayes/Input"
    val output_vector_name = input_path + "/vectors.txt"
    val parallel = sc.getConf.getInt("spark.default.parallelism", sc.defaultParallelism)

    val data = sc.sequenceFile[Text, Text](input_path).map{case(k, v) => (k.toString, v.toString)}

    data.repartition(parallel)
    val wordcount = data.flatMap{case(key, doc) => doc.split(" ")}
                        .map(word => (word, 1))
                        .reduceByKey(_ + _)
    val wordsum = wordcount.map(_._2).reduce(_ + _)

    val word_dict = wordcount.zipWithIndex()
                             .map{case ((key, count), index) =>
                                  (key, (index, count.toDouble / wordsum))}
                             .collectAsMap()
    val shared_word_dict = sc.broadcast(word_dict)

    // for each document, generate vector based on word freq
    val vector = data.map { case (key, doc) =>
      val doc_vector = doc.split(" ").map(x => shared_word_dict.value(x)) //map to word index: freq
        .groupBy(_._1) // combine freq with same word
        .map { case (k, v) => (k, v.map(_._2).sum)}

      val sorted_doc_vector = doc_vector.toList.sortBy(_._1)
        .map { case (k, v) => "%d:%f".format(k + 1,  // LIBSVM's index starts from 1 !!!
                                             v)} // convert to LIBSVM format

      // key := /classXXX
      // key.substring(6) := XXX
      // label index1:value1 index2:value2 ...
      key.substring(6) + " " + sorted_doc_vector.mkString(" ")
    }
    vector.saveAsTextFile(output_vector_name)
    sc.stop()
  }
}
