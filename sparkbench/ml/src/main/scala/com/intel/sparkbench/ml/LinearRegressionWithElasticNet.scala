package com.intel.hibench.sparkbench.ml

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser

object LinearRegressionWithElasticNet {
  case class Params(
      input: String = null,
      regParam: Double = 0.3,
      elasticNetParam: Double = 0.8,
      maxIter: Int = 10,
      tol: Double = 1E-6,
      fracTest: Double = 0.25)

  def main(args: Array[String]): Unit = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("LinearRegressionWithElasticNet") {
      head("LinearRegressionExample: an example Linear Regression with Elastic-Net app.")
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Double]("elasticNetParam")
        .text(s"ElasticNet mixing parameter. For alpha = 0, the penalty is an L2 penalty. " +
          s"For alpha = 1, it is an L1 penalty. For 0 < alpha < 1, the penalty is a combination of " +
          s"L1 and L2, default: ${defaultParams.elasticNetParam}")
        .action((x, c) => c.copy(elasticNetParam = x))
      opt[Int]("maxIter")
        .text(s"maximum number of iterations, default: ${defaultParams.maxIter}")
        .action((x, c) => c.copy(maxIter = x))
      opt[Double]("tol")
        .text(s"the convergence tolerance of iterations, Smaller value will lead " +
          s"to higher accuracy with the cost of more iterations, default: ${defaultParams.tol}")
        .action((x, c) => c.copy(tol = x))
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
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNet")
      .getOrCreate()

    val training: DataFrame = loadDatasets(params.input,"regression",params.fracTest)

    val lr = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(params.maxIter)
      .setRegParam(params.regParam)
      .setElasticNetParam(params.elasticNetParam)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")

    spark.stop()
  }

  private[ml] def loadDatasets(input: String,
      algo: String,
      fracTest: Double): DataFrame ={
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

    val numTraining = training.count()

    val numFeatures = training.select("features").first().getAs[Vector](0).size
    println("Loaded data:")
    println(s"  numTraining = $numTraining")
    println(s"  numFeatures = $numFeatures")
    training
  }
}
