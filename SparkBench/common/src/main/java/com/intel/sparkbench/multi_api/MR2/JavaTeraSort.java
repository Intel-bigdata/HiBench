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

package com.intel.sparkbench.terasort;

import com.intel.sparkbench.IOCommon;
import org.apache.hadoop.examples.terasort.TeraInputFormat;
import org.apache.hadoop.examples.terasort.TeraOutputFormat;
import org.apache.hadoop.io.Text;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import java.util.regex.Pattern;

public final class JavaTeraSort {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaTeraSort <HDFS_INPUT> <HDFS_OUTPUT>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaTeraSort");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaPairRDD<Text, Text> lines = ctx.newAPIHadoopFile(args[0], TeraInputFormat.class, Text.class, Text.class, ctx.hadoopConfiguration());
    Integer parallel = sparkConf.getInt("spark.default.parallelism", ctx.defaultParallelism());
    Integer reducer  = Integer.parseInt(IOCommon.getProperty("sparkbench.reducer").get());
    JavaPairRDD<String, String> words = lines.mapToPair(new PairFunction<Tuple2<Text, Text>, String, String>() {
        @Override
        public Tuple2<String, String> call(Tuple2<Text, Text> e) throws Exception {
            return new Tuple2<String, String>(e._1().toString(), e._2().toString());
        }
    });


    JavaPairRDD<String, String> sorted = words.sortByKey(true, reducer);
    JavaPairRDD<Text, Text> result = sorted.mapToPair(new PairFunction<Tuple2<String, String>, Text, Text>() {
        @Override
        public Tuple2<Text, Text> call(Tuple2<String, String> e) throws Exception {
            return new Tuple2<Text, Text>(new Text(e._1()), new Text(e._2()));
        }
    });
    result.saveAsNewAPIHadoopFile(args[1], Text.class, Text.class, TeraOutputFormat.class);

    ctx.stop();
  }
}

