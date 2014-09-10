import java.io._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/*
 * Convert generated document by mahout into vector based on frequency of words
 * Spark needed!
 */

object Convert{
  val conf = new Configuration()
  def main(args: Array[String]){
    if (args.length!=2){
      System.err.println("Usage: Convert <input_path> <PARALLEL>")
      System.exit(1)
    }

    val input_path =   args(0)  //"hdfs://localhost:54310/HiBench/Bayes/Input"
    val parallel   = args(1).toInt
    val output_vector_name = input_path + "/vectors.txt"

    val sparkConf = new SparkConf().setAppName("HiBench Bayes Converter")
    val sc = new SparkContext(sparkConf)

    val data = sc.sequenceFile[Text, Text](input_path).map{case(k, v) => (k.toString, v.toString)}

    data.repartition(parallel)
    val wordcount = data.flatMap{case(key, doc) => doc.split(" ")}
                        .map(word => (word, 1))
                        .reduceByKey(_ + _)
    val wordsum = wordcount.map(_._2).reduce(_ +_)

    val word_dict = wordcount.zipWithIndex()
                             .map{case ((key, count), index)=> (key, (index, count.toDouble / wordsum))}
                             .collectAsMap()
    val shared_word_dict = sc.broadcast(word_dict)

    // for each document, generate vector based on word freq
    val vector = data.map { case (key, doc) =>
      val doc_vector = doc.split(" ").map(x => shared_word_dict.value(x)) //map to word index: freq
        .groupBy(_._1) // combine freq with same word
        .map { case (k, v) => (k, v.map(_._2).sum)}

      val sorted_doc_vector = doc_vector.toList.sortBy(_._1)
        .map { case (k, v) => "%d:%f".format(k + 1,  // LIBSVM's index starts from 1 !!!
                                             v)} // convert to LIBSVM format

      // key := /classXXX
      // key.substring(6) := XXX
      key.substring(6) + " " + sorted_doc_vector.mkString(" ") // label index1:value1 index2:value2 ...
    }
    vector.saveAsTextFile(output_vector_name)
    sc.stop()
  }
}

