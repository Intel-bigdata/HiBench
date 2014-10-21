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
    val input_format = System.getProperty("sparkbench.inputformat", "Text")
    input_format match {
      case "Text" => sc.textFile(filename)
      case "Object" => sc.objectFile(filename)
      case _ => throw new UnsupportedOperationException(s"Unknown inpout format: $input_format")
    }
  }

  def save[T](filename:String, data:RDD[T], prefix:String = "sparkbench.outputformat") = {
    val output_format = System.getProperty(prefix, "Text")
    val output_format_codec = System.getProperty(prefix + ".codec",
      "org.apache.hadoop.io.compress.SnappyCodec")
    output_format match {
      case "Text" => {
        if (output_format_codec == "None") data.saveAsTextFile(filename)
        else {
          val cls = Class.forName(output_format_codec).newInstance().asInstanceOf[CompressionCodec]
          data.saveAsTextFile(filename, cls.getClass)
        }
      }
      case "Object" => data.saveAsObjectFile(filename)
      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
    }
  }
}
