import java.io._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.util.ReflectionUtils
import org.apache.mahout.math.VectorWritable
import org.apache.mahout.math.Vector
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object Convert{
  val conf = new Configuration()
  def main(args: Array[String]) {
    if ( args.length != 3 ) {
      System.err.println("Usage: Convert <input_directory> <output_file_path> <PARALLEL>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Doc2Vector")
    val sc = new SparkContext(sparkConf)

    val input_path = args(0) //"hdfs://localhost:54310/SparkBench/KMeans/Input/samples/"
    val output_name = args(1) //"/HiBench/KMeans/Input/samples.txt"
    val parallel = args(2).toInt  //256

    BytesWritable
    val data = sc.sequenceFile[LongWritable, VectorWritable](input_path)
    data.repartition(parallel)
    data.map { case (k, v) =>
      var vector: Array[Double] = null
      if (vector == null) {
        vector = new Array[Double](v.get().size)
      }
      for (i <- 0 until v.get().size) vector(i) = v.get().get(i)
      vector.mkString(" ")
    }.saveAsTextFile(output_name)
    sc.stop()
  }
}

