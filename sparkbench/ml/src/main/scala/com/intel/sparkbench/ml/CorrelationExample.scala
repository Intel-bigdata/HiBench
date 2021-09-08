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

    val parser = new OptionParser[Params]("Correlation") {
      head("Correlation: an example app for computing correlations.")
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
    val training = data.toDF()

    val df = training.toDF("label", "features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features", params.corrType).head()
    println(s"Correlation matrix:\n $coeff1.")

    spark.stop()
  }

}
