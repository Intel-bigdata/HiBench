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
 * Modified from org.apache.spark.examples.graphx.PageRankExample
 *   * Deduplicate edges
 *   * Remove join the ranks with the usernames
 *   * Use saveAsText instead of print to present the result.
 */

package org.apache.spark.examples.graphx

import com.intel.hibench.sparkbench.common.IOCommon
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

/**
  * Computes the PageRank of URLs from an input file. It's implemented with GraphX.
  * Input file should be in format of:
  * URL         neighbor URL
  * URL         neighbor URL
  * URL         neighbor URL
  * ...
  * where URL and their neighbors are separated by space(s).
  */

object GraphXPageRank {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage: GraphXPageRank <input_file> <output_filename> [<iter>]")
      System.exit(1)
    }

    val input_path = args(0)
    val output_path = args(1)
    val iters = if (args.length > 2) args(2).toInt else 10

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName("GraphXPageRank")
      .getOrCreate()
    val sc = spark.sparkContext

    // Load the edges as a graph
    val graph0 = GraphLoader.edgeListFile(sc, input_path)
    // Deduplicate edges
    val graph = graph0.groupEdges((_, _) => 1)

    // Run PageRank
    val ranks = graph.staticPageRank(iters).vertices

    val io = new IOCommon(sc)
    io.save(output_path, ranks)

    spark.stop()
  }
}

