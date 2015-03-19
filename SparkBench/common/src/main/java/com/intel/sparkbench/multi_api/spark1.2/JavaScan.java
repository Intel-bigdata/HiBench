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

package com.intel.sparkbench.scan;

import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.regex.Pattern;

/*
 * ported from HiBench's hive bench
 */
public final class JavaScan {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaScan <hdfs_url>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaScan");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaHiveContext hc = new JavaHiveContext(ctx);


    hc.sql("DROP TABLE if exists rankings");
    hc.sql(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/rankings'", args[0]));
    hc.sql(String.format("CREATE EXTERNAL TABLE rankings_copy (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/rankings'", args[1]));
    hc.sql("INSERT OVERWRITE TABLE rankings_copy SELECT * FROM rankings");


    ctx.stop();
  }
}

