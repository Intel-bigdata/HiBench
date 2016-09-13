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

package com.intel.sparkbench.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/*
 * ported from HiBench's hive bench
 */
object ScalaSparkSQLBench{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaSparkSQLBench <workload name> <SQL sciprt file>"
      )
      System.exit(1)
    }
    val workload_name = args(0)
    val sql_file = args(1)
    val sparkConf = new SparkConf().setAppName(workload_name)
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    val _sql = scala.io.Source.fromFile(sql_file).mkString
    _sql.split(';').foreach { x =>
      if (x.trim.nonEmpty)
        hc.sql(x)
    }

    sc.stop()
  }
}
