/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.sparkbench

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.util.Properties

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkException}

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

class IOCommon(val sc:SparkContext) {
  def load[T:ClassTag:TypeTag](filename:String, force_format:Option[String]=None) = {
    val input_format = force_format.getOrElse(
      IOCommon.getProperty("sparkbench.inputformat").getOrElse("Text"))

    input_format match {
      case "Text" =>
        sc.textFile(filename)

      case "Sequence" =>
        sc.sequenceFile[NullWritable, Text](filename).map(_._2.toString)

      case _ => throw new UnsupportedOperationException(s"Unknown inpout format: $input_format")
    }
  }

  def save(filename:String, data:RDD[_], prefix:String) = {
    val output_format = IOCommon.getProperty(prefix).getOrElse("Text")
    val output_format_codec =
      loadClassByName[CompressionCodec](IOCommon.getProperty(prefix + ".codec"))

    output_format match {
      case "Text" =>
        if (output_format_codec.isEmpty)  data.saveAsTextFile(filename)
        else data.saveAsTextFile(filename, output_format_codec.get)

      case "Sequence" =>
        val sequence_data = data.map(x => (NullWritable.get(), new Text(x.toString)))
        if (output_format_codec.isEmpty) {
          sequence_data.saveAsHadoopFile[SequenceFileOutputFormat[NullWritable, Text]](filename)
        } else {
          sequence_data.saveAsHadoopFile[SequenceFileOutputFormat[NullWritable, Text]](filename,
            output_format_codec.get)
        }

      case _ => throw new UnsupportedOperationException(s"Unknown output format: $output_format")
    }
  }

  def save(filename:String, data:RDD[_]):Unit = save(filename, data, "sparkbench.outputformat")

  private def loadClassByName[T](name:Option[String]) = {
    if (!name.isEmpty) Some(Class.forName(name.get)
      .newInstance.asInstanceOf[T].getClass) else None
  }

  private def callMethod[T, R](obj:T, method_name:String) =
    obj.getClass.getMethod(method_name).invoke(obj).asInstanceOf[R]
}

object IOCommon{
  private val sparkbench_conf: HashMap[String, String] =
    getPropertiesFromFile(System.getenv("SPARKBENCH_PROPERTIES_FILES"))

  def getPropertiesFromFile(filenames: String): HashMap[String, String] = {
    val result = new HashMap[String, String]
    filenames.split(',').filter(_.stripMargin.length > 0).foreach { filename =>
      val file = new File(filename)
      require(file.exists, s"Properties file $file does not exist")
      require(file.isFile, s"Properties file $file is not a normal file")

      val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
      try {
        val properties = new Properties()
        properties.load(inReader)
        result ++= properties.stringPropertyNames()
          .map(k => (k, properties(k).trim)).toMap
      } catch {
        case e: IOException =>
          val message = s"Failed when loading Sparkbench properties file $file"
          throw new SparkException(message, e)
      } finally {
        inReader.close()
      }
    }
    result.filter{case (key, value) => value.toLowerCase != "none"}
  }

  def getProperty(key:String):Option[String] = sparkbench_conf.get(key)

  def dumpProperties(): Unit = sparkbench_conf
      .foreach{case (key, value)=> println(s"$key\t\t$value")}
}
