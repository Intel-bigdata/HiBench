package HiBench

import scopt.OptionParser
import org.apache.hadoop.io.Text
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object BayesDataGen {

  case class Params(input: String = null,
                    output: String = null,
                    numFeatures: Int = -1
                   )

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("BayesDataGen") {
      head("BayesDataGen: convert old HiBench bayes input dataset file to parquet dataset.")
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
    }

    parser.parse(args, defaultParams).map { params =>
      run(params)
    }.getOrElse {
      sys.exit(1)
    }
  }

  def run(params: Params) {
    val spark = SparkSession
      .builder
      .appName(s"BayesDataGen with $params")
      .getOrCreate()

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
      val label = dockey.substring(6).head.toDouble
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

    val stringIndexer = new StringIndexer()
      .setInputCol("origin_label")
      .setOutputCol("label")

    val processedDF = stringIndexer.fit(df).transform(df).select("label", "features")

    processedDF.write.mode("overwrite").parquet(params.output)

    spark.stop()
  }
}

