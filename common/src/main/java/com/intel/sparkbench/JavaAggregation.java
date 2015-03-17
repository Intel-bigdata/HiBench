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
package com.intel.sparkbench.aggregation;

import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/*
 * ported from HiBench's hive bench
 */
public final class JavaAggregation {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaAggregation <hdfs_url>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaAggregation");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    HiveContext hc = new HiveContext(ctx.sc());

    hc.sql("DROP TABLE IF EXISTS uservisits");
    hc.sql("DROP TABLE IF EXISTS uservisits_aggre");
    hc.sql(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits'", args[0]));
    hc.sql(String.format("CREATE TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", args[1]));
    hc.sql("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
    
    ctx.stop();
  }
}

