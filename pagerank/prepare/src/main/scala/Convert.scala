import java.io.{BufferedWriter, OutputStreamWriter, InputStreamReader, BufferedReader}

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

object Convert{
    val conf = new Configuration()
    def main(args: Array[String]){
      if (args.length != 3){
        System.err.println("Usage: Convert <input_directory> <output_file_path> <PARALLEL>")
        System.exit(1)
      }

      val input_path = args(0)  //"/HiBench/Pagerank/Input/edges"
      val output_name = args(1) // "/HiBench/Pagerank/Input/edges.txt"
      val parallel = args(2).toInt  //256

      val sparkConf = new SparkConf().setAppName("HiBench PageRank Converter")
      val sc = new SparkContext(sparkConf)

      val data = sc.textFile(input_path).map{case(line)=>
          val elements = line.split('\t')
          "%s  %s".format(elements(1), elements(2))
      }

      data.repartition(parallel)
      data.saveAsTextFile(output_name)
      sc.stop()
    }
  }

