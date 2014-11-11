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

package com.intel.sparkbench.join

import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

/*
 * ported from HiBench's hive bench
 */
object ScalaJoin{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaJoin <INPUT_DATA_URL> <OUTPUT_DATA_URL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaJoin")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    hc.hql("DROP TABLE IF EXISTS rankings")
    hc.hql("DROP TABLE IF EXISTS uservisits")
    hc.hql("DROP TABLE IF EXISTS rankings_uservisits_join")
    hc.hql(
      """CREATE EXTERNAL TABLE rankings
        (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT
        DELIMITED FIELDS TERMINATED BY ','
        STORED AS SEQUENCEFILE LOCATION '%s/rankings'""".stripMargin.format(args(0)))
    hc.hql(
      """CREATE EXTERNAL TABLE uservisits
         (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,
          userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT)
          ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
          STORED AS SEQUENCEFILE LOCATION '%s/uservisits'""".stripMargin.format(args(0)))
    hc.hql(
      """CREATE TABLE rankings_uservisits_join
         ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS SEQUENCEFILE
          LOCATION '%s/rankings_uservisits_join'""".stripMargin.format(args(1)))
    hc.hql(
      """INSERT OVERWRITE TABLE rankings_uservisits_join
          SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank)
          FROM rankings R JOIN
            (SELECT sourceIP, destURL, adRevenue
             FROM uservisits UV
             WHERE (datediff(UV.visitDate, '1999-01-01')>=0
                AND datediff(UV.visitDate, '2000-01-01')<=0))
          NUV ON (R.pageURL = NUV.destURL)
          group by sourceIP order by totalRevenue DESC limit 1""".stripMargin)

    sc.stop()
  }
}
