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

package com.intel.hibench.streambench.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import StreamingContext._
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import org.apache.spark.storage._
//import scala.collection.mutable.Map

object TestKafkaJob2{
 def main(args: Array[String]){
  val conf=new SparkConf().setMaster("spark://sr119:7077").setAppName("kafka test").setSparkHome("/home/ruirui/spark-1.0.0-bin-hadoop1").setJars(Seq("/home/ruirui/spark-1.0.0-bin-hadoop1/myStreamBench/target/scala-2.10/streaming-bench-spark_0.1-assembly-1.0.0.jar")).set("spark.cleaner.ttl","7200")

  val ssc=new StreamingContext(conf,Seconds(1))
  System.out.println("Current home:spark 1,topic:"+args(0))

  val kafkaParams=Map[String,String]("zookeeper.connect"->"sr464:2181","group.id"->"sparkScala")
//  kafkaParams+=("zookeeper.connect","sr464:2181")
//  kafkaParams+=("group.id","sparkScala")
  val lines=KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,Map(args(0)->1),StorageLevel.MEMORY_ONLY_SER_2)

  val fields=lines.map(line=>line)

  //lines.print()
  //fields.print()
  var totalCount=0L
  fields.foreachRDD(rdd=>{
    val count=rdd.count;
    System.out.println("Current count:"+count);
    totalCount+=count;
    System.out.println("Current total count:"+totalCount);
  })

  ssc.start()
  ssc.awaitTermination()
}
}
