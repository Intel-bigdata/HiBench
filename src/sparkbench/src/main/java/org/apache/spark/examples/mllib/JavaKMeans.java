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

/*
 * Copied from org.apache.spark.examples.mllib.JavaKMeans
 */
package org.apache.spark.examples.mllib;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.math.VectorWritable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

/**
 * Example using MLLib KMeans from Java.
 */
public final class JavaKMeans {

  public static void main(String[] args) {
    if (args.length < 4) {
      System.err.println(
        "Usage: JavaKMeans <input_file> <input_cluster> <k> <max_iterations> [<runs>]");
      System.exit(1);
    }
    String inputFile = args[0];
    String inputCluster = args[1];
    int k = Integer.parseInt(args[2]);
    int iterations = Integer.parseInt(args[3]);
    int runs = 1;

    if (args.length >= 5) {
      runs = Integer.parseInt(args[4]);
    }
    SparkConf sparkConf = new SparkConf().setAppName("JavaKMeans");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    // Load input points
    JavaPairRDD<LongWritable, VectorWritable> data = sc.sequenceFile(inputFile,
                LongWritable.class, VectorWritable.class);

    JavaRDD<Vector> points = data.map(new Function<Tuple2<LongWritable, VectorWritable>, Vector>() {
        @Override
        public Vector call(Tuple2<LongWritable, VectorWritable> e) {
            VectorWritable val = e._2();
            double[] v = new double[val.get().size()];
            for (int i = 0; i < val.get().size(); ++i) {
                v[i] = val.get().get(i);
            }
            return Vectors.dense(v);
        }
    });

    // Load initial centroids
    JavaPairRDD<Text, Kluster> clusters = sc.sequenceFile(inputCluster, Text.class, Kluster.class);
    JavaRDD<Vector> centroids = clusters.map(new Function<Tuple2<Text, Kluster>, Vector>() {
      @Override
      public Vector call(Tuple2<Text, Kluster> e) {
        org.apache.mahout.math.Vector centroid = e._2().getCenter();
        double[] v = new double[centroid.size()];
        for (int i = 0; i < centroid.size(); ++i) {
          v[i] = centroid.get(i);
        }
        return Vectors.dense(v);
      }
    });

    // Train model
    KMeansModel initModel = new KMeansModel(centroids.collect());
    KMeansModel model = new KMeans()
        .setK(k)
        .setMaxIterations(iterations)
        .setRuns(runs)
        .setInitialModel(initModel)
        .run(points.rdd());

    System.out.println("Cluster centers:");
    for (Vector center : model.clusterCenters()) {
      System.out.println(" " + center);
    }
    double cost = model.computeCost(points.rdd());
    System.out.println("Cost: " + cost);

    sc.stop();
  }
}
