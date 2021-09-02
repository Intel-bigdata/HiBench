package com.intel.hibench.sparkbench.ml

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import scopt.OptionParser
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix
import org.apache.spark.sql.Row


object CorrelationExample {

  case class Params(
       input: String = null,
       corrType: String = "pearson",
       fracTest: Double = 0.25
       )

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearRegressionWithElasticNet") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[String]("corrType")
        .text(s"String specifying the method to use for computing correlation. " +
          s"Supported: `pearson` (default), `spearman`, default: ${defaultParams.corrType}")
        .action((x, c) => c.copy(corrType = x))
      opt[Double]("fracTest")
        .text(s"fraction of data to hold out for testing. If given option testInput, " +
          s"this option is ignored. default: ${defaultParams.fracTest}")
        .action((x, c) => c.copy(fracTest = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
      checkConfig { params =>
        if (params.fracTest < 0 || params.fracTest >= 1) {
          failure(s"fracTest ${params.fracTest} value incorrect; should be in [0,1).")
        } else {
          success
        }
      }
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val conf = new SparkConf().setAppName(s"Correlations with $params")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder
      .getOrCreate()

    val training: DataFrame = loadDatasets(params.input,"regression",params.fracTest)


    println(s"Summary of data file: ${params.input}")
    println(s"${training.count()} data points")


    val numTraining = training.count()

    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println(s"  numTraining = $numTraining")
    println(s"  numFeatures = $numFeatures")

    val corrType = "pearson"

    println(s"Correlation ($corrType) between label and each feature")
    println(s"Feature\tCorrelation")
    val df = training.toDF("label", "features")
    val Row(coeff: Matrix) = Correlation.corr(df, "features", params.corrType).head
    println(s"Correlation matrix:\n $coeff")

    sc.stop()
  }

  private[ml] def loadDatasets(input: String,
                               algo: String,
                               fracTest: Double): DataFrame ={
    println("Loaded data:")
    val spark = SparkSession
      .builder
      .getOrCreate()

    // Load training data
    val data: RDD[LabeledPoint] = spark.sparkContext.objectFile(input)
    import spark.implicits._
    val origExamples = data.toDF()

    // Load or create test set
    val dataframes: Array[DataFrame] = origExamples.randomSplit(Array(1.0 - fracTest, fracTest), seed = 12345)

    val training = dataframes(0).cache()

    training
  }
}
