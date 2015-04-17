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

package com.intel.sparkbench.sleep;

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaSleep{
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      System.err.println("Usage: JavaSleep <seconds>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaSleep");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    Integer parallel = sparkConf.getInt("spark.default.parallelism", ctx.defaultParallelism());
    Integer seconds = Integer.parseInt(args[0]);

    Integer[] init_val = new Integer[parallel];
    Arrays.fill(init_val, seconds);

    JavaRDD<Integer> workload = ctx.parallelize(Arrays.asList(init_val), parallel).map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer s) throws InterruptedException {
	    Thread.sleep(s * 1000);
        return 0;
      }
    });

    List<Integer> output = workload.collect();
    ctx.stop();
  }
}

