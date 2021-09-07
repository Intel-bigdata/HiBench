package com.intel.hibench.sparkbench.ml

import org.apache.spark.ml.feature.LabeledPoint
import scopt.OptionParser
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.ml.stat.Correlation

object CorrelationExample {

  case class Params(
       input: String = null,
       corrType: String = "pearson",
       )

  def main(args: Array[String]): Unit = {

    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearRegressionWithElasticNet") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[String]("corrType")
        .text(s"String specifying the method to use for computing correlation. " +
          s"Supported: `pearson` (default), `spearman`, default: ${defaultParams.corrType}")
        .action((x, c) => c.copy(corrType = x))
      arg[String]("<input>")
        .text("input path to labeled examples")
        .required()
        .action((x, c) => c.copy(input = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) => run(params)
      case _ => sys.exit(1)
    }
  }

  def run(params: Params): Unit = {
    val spark = SparkSession
      .builder
      .appName(s"Correlations with $params")
      .getOrCreate()

    val data: RDD[LabeledPoint] = spark.sparkContext.objectFile(params.input)
    import spark.implicits._
    val training = data.toDF().cache()


    val numTraining = training.count()

    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println(s"  numTraining = $numTraining")
    println(s"  numFeatures = $numFeatures")


    println(s"Correlation ${params.corrType} between label and each feature")
    println(s"Feature\tCorrelation")

    val df = training.toDF("label", "features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features", params.corrType)
    println(s"Pearson correlation matrix:\n $coeff1.")

    spark.stop()
  }

}
