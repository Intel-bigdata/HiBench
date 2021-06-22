package HiBench

import scopt.OptionParser
import org.apache.hadoop.io.Text
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.Random

//
// If use_dense is true: generate dense dataset in parquet format
// Otherwise, convert old HiBench bayes input dataset file to parquet dataset
//
object BayesDataGen {

  case class Params(input: String = null,
                    output: String = null,
                    numFeatures: Int = -1,
                    useDense: Boolean = false,
                    examples: Int = -1,
                    features: Int = -1,
                    classes: Int = -1
                   )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BayesDataGen") {
      head("BayesDataGen: generate Bayes dense dataset in parquet or convert old input dataset to parquet.")
      opt[Int]("numFeatures")
        .text("number of features")
        .action((x, c) => c.copy(numFeatures = x))
      opt[String]("input")
        .text("input path to old hibench bayes input dataset")
        .required()
        .action((x, c) => c.copy(input = x))
      opt[String]("output")
        .text("output path to converted parquet dataset")
        .required()
        .action((x, c) => c.copy(output = x))
      opt[Boolean]("useDense")
        .text("if generate dense dataset or convert old dataset")
        .required()
        .action((x, c) => c.copy(useDense = x))
      opt[Int]("examples")
        .text("example number of dense dataset")
        .required()
        .action((x, c) => c.copy(examples = x))
      opt[Int]("features")
        .text("features number of dense dataset")
        .required()
        .action((x, c) => c.copy(features = x))
      opt[Int]("classes")
        .text("classes number of dense dataset")
        .required()
        .action((x, c) => c.copy(classes = x))
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def convertOldDataset(spark: SparkSession, params: Params): DataFrame = {
    val sc = spark.sparkContext

    import spark.implicits._

    // Generate vectors according to input documents
    val data = sc.sequenceFile[Text, Text](params.input).map { case (k, v) => (k.toString, v.toString) }
    val wordCount = data
      .flatMap { case (key, doc) => doc.split(" ") }
      .map((_, 1L))
      .reduceByKey(_ + _)
    val wordSum = wordCount.map(_._2).reduce(_ + _)
    val wordDict = wordCount.zipWithIndex()
      .map { case ((key, count), index) => (key, (index.toInt, count.toDouble / wordSum)) }
      .collectAsMap()
    val sharedWordDict = sc.broadcast(wordDict)

    // for each document, generate vector based on word freq
    val vector = data.map { case (dockey, doc) =>
      val docVector = doc.split(" ").map(x => sharedWordDict.value(x)) //map to word index: freq
        .groupBy(_._1) // combine freq with same word
        .map { case (k, v) => (k, v.map(_._2).sum) }

      val (indices, values) = docVector.toList.sortBy(_._1).unzip
      // dockey: /class123 => label: 123.0
      val label = dockey.substring(6).toDouble
      (label, indices.toArray, values.toArray)
    }

    val d = if (params.numFeatures > 0) {
      params.numFeatures
    } else {
      vector.persist(StorageLevel.MEMORY_ONLY)
      vector.map { case (label, indices, values) =>
        indices.lastOption.getOrElse(0)
      }.reduce(math.max) + 1
    }

    val examples = vector.map { case (label, indices, values) =>
      LabeledPoint(label, Vectors.sparse(d, indices, values))
    }

    val df = examples.map { case LabeledPoint(label, features) => (label, features.asML) }
      .toDF("origin_label", "features")

    df
  }

  def genDenseDataset(spark: SparkSession, params: Params, seed: Int = 1234): DataFrame = {
    val sc = spark.sparkContext
    import spark.implicits._

    val nparts = sc.defaultParallelism
    val eps = 3

    val df = sc.parallelize(0 until params.examples, nparts).map { idx =>
      val rnd = new Random(seed + idx)

      val label: Double = rnd.nextInt(params.classes).toDouble
      val features = Array.fill[Double](params.features) {
        (rnd.nextGaussian() + (label * eps)).abs
      }

      Tuple2(label, Vectors.dense(features))
    }.toDF("origin_label", "features")

    df
  }

  def run(params: Params) {
    val spark = SparkSession
      .builder
      .appName(s"BayesDataGen with $params")
      .getOrCreate()

    val df = if (!params.useDense)
      convertOldDataset(spark, params)
    else
      genDenseDataset(spark, params)

    val stringIndexer = new StringIndexer()
      .setInputCol("origin_label")
      .setOutputCol("label")

    val processedDF = stringIndexer.fit(df).transform(df).select("label", "features")

    processedDF.write.mode("overwrite").parquet(params.output)

    spark.stop()
  }
}

