package com.intel.sparkbench.datagen.pythonconverter

import scala.collection.JavaConversions._

import org.apache.spark.api.python.Converter
import org.apache.mahout.math.VectorWritable

/**
 * Created by lv on 15-2-27.
 */
class MahoutVectorToStringConverter extends Converter[Any, String]{
  override def convert(obj: Any): String = {
    val result = obj.asInstanceOf[VectorWritable]
    result.toString
  }
}
