package org.apache.spark.SparkBench

import org.apache.hadoop.io.{LongWritable, Writable, BytesWritable, NullWritable}
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext

import scala.reflect.runtime.{universe => ru}

import org.apache.spark.SparkContext._

/**
 * Created by lv on 14-10-21.
 */
class IOCommon(val sc:SparkContext) {
  def load[T](filename:String) = {
    val input_format = System.getProperty("sparkbench.inputformat", "Text")
    input_format match {
      case "Text" => sc.textFile(filename)
      case "Object" => sc.objectFile[T](filename)
      case "Sequence" => {
        val input_key_cls = loadClassByName[Writable](System.getProperty("sparkbench.inputformat.key_class",
              "org.apache.hadoop.io.LongWritable"))
        val input_value_cls = loadClassByName[T](System.getProperty("sparkbench.inputformat.value_class",
              "org.apache.hadoop.io.LongWritable"))
        val input_value_cls_method = System.getProperty("sparkbench.inputformat.value_class_method", "get")
        val data = sc.sequenceFile(filename, input_key_cls.getClass, input_value_cls.getClass).map(_._2)
        data.map(x=>callMethod[Writable, T](x, input_value_cls_method))
      }
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
          data.saveAsTextFile(filename, loadClassByName[CompressionCodec](output_format_codec).getClass)
        }
      }
      case "Sequence" => data.map(x=> (NullWritable.get(), new BytesWritable(Utils.serialize(x))))
          .saveAsSequenceFile(filename)
      case "Object" => data.saveAsObjectFile(filename)
      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
    }
  }

  private def loadClassByName[T](name:String) = Class.forName(name).newInstance().asInstanceOf[T]
  private def callMethod[T, R](o:T, method_name:String) = {
    val m = ru.runtimeMirror(o.getClass.getClassLoader)
    val im = m.reflect(new T)
    val methodX = ru.typeOf[T].declaration(ru.newTermName(method_name)).asMethod
    val mm = im.reflectMethod(methodX)
    mm().asInstanceOf[R]
  }
}
