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

package com.intel.sparkbench.sort;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.Tuple2;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

import com.intel.sparkbench.sort.PatchedJavaPairRDD;

public final class JavaSort {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length != 3) {
      System.err.println("Usage: JavaSort <HDFS_INPUT> <HDFS_OUTPUT> <PARALLELISM>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaSort");
    Integer parallel = Integer.parseInt(args[2]);
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(SPACE.split(s));
      }
    });

    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairRDD<String, Integer> counts = new PatchedJavaPairRDD(ones.rdd(),
              ClassTag$.MODULE$.apply(String.class),
              ClassTag$.MODULE$.apply(Integer.class)
      ).sortByKeyWithHashedPartitioner( true, parallel / 2);

    JavaRDD<String> result = counts.map(new Function<Tuple2<String, Integer>, String>() {
        @Override
        public String call(Tuple2<String, Integer> e) throws Exception {
            return e._1();
        }
    });

    result.saveAsTextFile(args[1]);

    ctx.stop();
  }
}

