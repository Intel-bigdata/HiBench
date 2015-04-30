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

package com.intel.sparkbench.bayes;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import scala.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.hadoop.io.Text;

import java.lang.Boolean;
import java.lang.Double;
import java.lang.Long;
import java.util.*;
import java.util.regex.Pattern;


/*
 * Adopted from spark's doc: http://spark.apache.org/docs/latest/mllib-naive-bayes.html
 */
public final class JavaBayes {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaBayes <file>");
      System.exit(1);
    }

    Random rand = new Random();

    SparkConf sparkConf = new SparkConf().setAppName("JavaBayes");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
//    int numFeatures = Integer.parseInt(args[1]);

    // Generate vectors according to input documents
    JavaPairRDD<String, String> data = ctx.sequenceFile(args[0], Text.class, Text.class)
            .mapToPair(new PairFunction<Tuple2<Text, Text>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<Text, Text> e) {
                    return new Tuple2<String, String>(e._1().toString(), e._2().toString());
                }
            });

    JavaPairRDD<String, Integer> wordCount = data
            .flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
                @Override
                public Iterable<String> call(Tuple2<String, String> e) {
                    return Arrays.asList(SPACE.split(e._2()));
                }
            })
            .mapToPair(new PairFunction<String, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(String e) {
                    return new Tuple2<String, Integer>(e, 1);
                }
            })
            .reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                    return i1 + i2;
                }
            });

      final Integer wordSum = wordCount.map(new Function<Tuple2<String, Integer>, Integer>(){
          @Override
          public Integer call(Tuple2<String, Integer> e) {
              return e._2();
          }
      })
      .reduce(new Function2<Integer, Integer, Integer>() {
          @Override
          public Integer call(Integer v1, Integer v2) throws Exception {
              return v1 + v2;
          }
      });

    List<Tuple2<String, Tuple2<Integer, Double>>> wordDictList = wordCount.zipWithIndex()
            .map(new Function<Tuple2<Tuple2<String, Integer>, Long>, Tuple2<String, Tuple2<Integer, Double>>>() {
                @Override
                public Tuple2<String, Tuple2<Integer, Double>> call(Tuple2<Tuple2<String, Integer>, Long> e) throws Exception {
                    String key = e._1()._1();
                    Integer count = e._1()._2();
                    Long index = e._2();
                    return new Tuple2<String, Tuple2<Integer, Double>>(key, new Tuple2<Integer, Double>(index.intValue(),
                            count.doubleValue() / wordSum));
                }
            }).collect();

    Map<String, Tuple2<Integer, Double>> wordDict = new HashMap();
    for (Tuple2<String, Tuple2<Integer, Double>> item : wordDictList) {
        wordDict.put(item._1(), item._2());
    }

    final Broadcast<Map<String, Tuple2<Integer, Double>>> sharedWordDict = ctx.broadcast(wordDict);

    // for each document, generate vector based on word freq
      JavaRDD<Tuple3<Double, Integer[], Double[]>> vector = data.map(new Function<Tuple2<String, String>, Tuple3<Double, Integer[], Double[]>>() {
          @Override
          public Tuple3<Double, Integer[], Double[]> call(Tuple2<String, String> v1) throws Exception {
              String dockey = v1._1();
              String doc = v1._2();
              String[] keys = SPACE.split(doc);
              Tuple2<Integer, Double>[] datas = new Tuple2[keys.length];
              for (int i = 0; i < keys.length; i++) {
                  datas[i] = sharedWordDict.getValue().get(keys[i]);
              }
              Map<Integer, Double> vector = new HashMap<Integer, Double>();
              for (int i = 0; i < datas.length; i++) {
                  Integer indic = datas[i]._1();
                  Double value = datas[i]._2();
                  if (vector.containsKey(indic)) {
                      vector.put(indic, value + vector.get(indic));
                  } else {
                      vector.put(indic, value);
                  }
              }

              Integer[] indices = new Integer[vector.size()];
              Double[] values = new Double[vector.size()];

              SortedSet<Integer> sortedKeys = new TreeSet<Integer>(vector.keySet());
              int c = 0;
              for (Integer key : sortedKeys) {
                  indices[c] = key;
                  values[c] = vector.get(key);
                  c+=1;
              }

              Double label = Double.parseDouble(dockey.substring(6));
              return new Tuple3<Double, Integer[], Double[]>(label, indices, values);
          }
      });

      vector.persist(StorageLevel.MEMORY_ONLY());
       final Integer d = vector
               .map(new Function<Tuple3<Double,Integer[],Double[]>, Integer>() {
                   @Override
                   public Integer call(Tuple3<Double, Integer[], Double[]> v1) throws Exception {
                       Integer[] indices = v1._2();
                       if (indices.length > 0) {
                           return indices[indices.length - 1];
                       } else return Integer.valueOf(0);
                   }
               })
              .reduce(new Function2<Integer, Integer, Integer>() {
                  @Override
                  public Integer call(Integer v1, Integer v2) throws Exception {
                      //System.out.println("v1:"+v1+"  v2:"+v2);
                      return v1 > v2 ? v1 : v2;
                  }
              }) + 1;

    RDD<LabeledPoint> examples = vector.map(new Function<Tuple3<Double,Integer[],Double[]>, LabeledPoint>() {
        @Override
        public LabeledPoint call(Tuple3<Double, Integer[], Double[]> v1) throws Exception {
            int intIndices [] = new int[v1._2().length];
            double intValues [] = new double[v1._3().length];
            for (int i=0; i< v1._2().length; i++){
                intIndices[i] = v1._2()[i];
                intValues[i] = v1._3()[i];
            }
            return new LabeledPoint(v1._1(), Vectors.sparse(d.intValue(),
                    intIndices, intValues));
        }
    }).rdd();

    //RDD<LabeledPoint> examples = MLUtils.loadLibSVMFile(ctx.sc(), args[0], false, numFeatures);
    RDD<LabeledPoint>[] split = examples.randomSplit(new double[]{0.8, 0.2}, rand.nextLong());

    JavaRDD<LabeledPoint> training = split[0].toJavaRDD();
    JavaRDD<LabeledPoint> test = split[1].toJavaRDD();

    final NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);
    JavaRDD<Double> prediction =
        test.map(new Function<LabeledPoint, Double>() {
            @Override
            public Double call(LabeledPoint p) {
                return model.predict(p.features());
            }
        });

    JavaPairRDD < Double, Double > predictionAndLabel =
        prediction.zip(test.map(new Function<LabeledPoint, Double>() {
            @Override
            public Double call(LabeledPoint p) {
                return p.label();
            }
        }));

    double accuracy = 1.0 * predictionAndLabel.filter(
            new Function<Tuple2<Double, Double>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Double, Double> pl) {
                    return pl._1() == pl._2();
                }
            }). count() / test.count();

    System.out.println(String.format("Test accuracy = %f", accuracy));
    ctx.stop();
  }
}

