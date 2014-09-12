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

package com.intel.sparkbench.aggregation

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object ScalaAggregation{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaAggregation <INPUT_DATA_URL> <OUTPUT_DATA_URL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaAggregation")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    hc.hql("DROP TABLE IF EXISTS uservisits")
    hc.hql("DROP TABLE IF EXISTS uservisits_aggre")
    hc.hql(
      """CREATE EXTERNAL TABLE uservisits
          (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,
          countryCode STRING,languageCode STRING,searchWord STRING,duration INT )
         ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
         STORED AS SEQUENCEFILE LOCATION '%s/uservisits'""".stripMargin.format(args(0)))
    hc.hql(
      """CREATE TABLE uservisits_aggre
          ( sourceIP STRING, sumAdRevenue DOUBLE)
         STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'""".stripMargin.format(args(1)))
    hc.hql("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP")
    sc.stop()
  }
}
