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

package com.intel.sparkbench.scan

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/*
 * ported from HiBench's hive bench
 */
object ScalaScan{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaScan <INPUT_DATA_URL> <OUTPUT_DATA_URL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaScan")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    hc.sql("DROP TABLE IF EXISTS rankings")
    hc.sql("""CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT)
           ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
           STORED AS SEQUENCEFILE LOCATION '%s/rankings'""".format(args(0)))
    hc.sql("FROM rankings SELECT *").rdd.saveAsTextFile("%s/rankings".format(args(1)))
    sc.stop()
  }
}
