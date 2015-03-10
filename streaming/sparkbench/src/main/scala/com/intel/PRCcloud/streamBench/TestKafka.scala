package com.intel.PRCcloud.streamBench

import com.intel.PRCcloud.streamBench.metrics.LatencyListener
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds,StreamingContext}
import StreamingContext._
import org.apache.spark.streaming.kafka._

object TestKafkaJob{
 def main(args: Array[String]){
//  val conf=new SparkConf().setMaster("spark://luruiruideMacBook-Pro.local:7077").setAppName("kafka test").setSparkHome("/Users/luruirui/IdeaProjects/StreamingBench/sparkbench/target/scala-2.10/streaming-bench-spark_0.1-assembly-1.2.1-SNAPSHOT.jar").set("spark.cleaner.ttl","7200")
  val conf=new SparkConf().setMaster("spark://luruiruideMacBook-Pro.local:7077").setAppName("kafka test").setSparkHome("/Users/luruirui/MySoftwares/program/spark/spark").setJars(Seq("/Users/luruirui/IdeaProjects/StreamingBench/sparkbench/target/scala-2.10/streaming-bench-spark_0.1-assembly-1.2.1-SNAPSHOT.jar")).set("spark.cleaner.ttl","7200")

  val ssc=new StreamingContext(conf,Seconds(1))
  System.out.println("Current home:spark 1,topic:"+args(0))
//   val listener=new LatencyListener()
//   ssc.addStreamingListener(listener)
  val lines=KafkaUtils.createStream(ssc,"localhost:2181","spark_consumer",Map(args(0)->1))

  val fields=lines.map(_._2)

  //lines.print()
  //fields.print()
  var totalCount=0L
  fields.foreachRDD(rdd=>{
//    val count=rdd.count;
//    System.out.println("Current count:"+count);
//    totalCount+=count;
//    System.out.println("Current total count:"+totalCount);
  })

  ssc.start()
  ssc.awaitTermination()
}
}
