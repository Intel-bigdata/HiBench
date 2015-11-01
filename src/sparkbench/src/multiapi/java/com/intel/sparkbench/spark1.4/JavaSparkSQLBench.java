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
package com.intel.sparkbench.sql;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.hive.HiveContext;

import java.io.FileReader;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/*
 * ported from HiBench's hive bench
 */
public final class JavaSparkSQLBench {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: JavaSparkSqlBench <workload_name> <hdfs_url>");
            System.exit(1);
        }
        String workload_name = args[0];
        String sql_script = args[1];
        SparkConf sparkConf = new SparkConf().setAppName(workload_name);
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        HiveContext hc = new HiveContext(ctx.sc());
        FileReader in = new FileReader(sql_script);
        StringBuilder contents = new StringBuilder();
        char[] buffer = new char[40960];
        int read = 0;
        do {
            contents.append(buffer, 0, read);
            read = in.read(buffer);
        } while (read >= 0);

        for (String s : contents.toString().split(";")) {
            if (!s.trim().isEmpty()) {
                hc.sql(s);
            }
        }

        ctx.stop();
    }
}

