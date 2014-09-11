package com.intel.sparkbench.bayes;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.mllib.classification.NaiveBayes;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;



public final class JavaBayes {
  private static final Pattern SPACE = Pattern.compile(" ");


  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaBayes <file> <numFeatures>");
      System.exit(1);
    }

    Random rand = new Random();

    SparkConf sparkConf = new SparkConf().setAppName("JavaBayes");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    int numFeatures = Integer.parseInt(args[1]);

    RDD<LabeledPoint> examples = MLUtils.loadLibSVMFile(ctx.sc(), args[0], false, numFeatures);
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

