package org.apache.spark.SparkBench

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext

import scala.reflect.runtime.{universe => ru}

/**
 * Created by lv on 14-10-21.
 */
class IOCommon(val sc:SparkContext) {
  def load(filename:String) = {
    val input_format = sc.getConf.get("sparkbench.inputformat", "TextInputFormat")
    input_format match {
      case "TextInputFormat" => sc.textFile(filename)
      case "ObjectInputFormat" => sc.objectFile(filename)
      case _ => throw new UnsupportedOperationException(s"Unknown inpout format: $input_format")
    }
  }

  def save[T](filename:String, data:RDD[T]) = {
    val output_format = sc.getConf.get("sparkbench.outputformat", "TextOutputFormat")
    val output_format_codec = sc.getConf.get("sparkbench.outputformat.codec",
      "org.apache.hadoop.io.compress.SnappyCodec")
    output_format match {
      case "TextOutputFormat" => data.saveAsTextFile(filename)
      case "ObjectOutputFormat" => data.saveAsObjectFile(filename)
      case "CompressedTextOutputFormat" => {
        val cls = Class.forName(output_format_codec).newInstance().asInstanceOf[CompressionCodec]
        data.saveAsTextFile(filename, cls.getClass)
      }
      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
    }
  }
}
