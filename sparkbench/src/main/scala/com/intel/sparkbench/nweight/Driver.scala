package com.intel.sparkbench.nweight 

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.scheduler.{JobLogger, StatsReportListener}

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}

/** 
 * Compute NWeight for Graph G(V, E) as defined below
 *     Weight(1)(u, v) = edge(u, v)
 *     Weight(n)(u, v) = Sum (over {x|there are edges (u, x) and (x, v)}) Weight(n-1)(u, x)*Weight(1)(x, v)
 * 
 * Input is given in Text file format. Each line represents a Node and all out edges of that node (edge weight specified) 
 *  <vertex> <vertex1>:<weight1>, <vertex2>:<weight2> ...) 
 */
object NWeight extends Serializable{
 
  def parseArgs(args: Array[String]) = {
    if (args.length < 7) {
      System.err.println("Usage: <input> <output> <step> <max Out edges> " +
          "<no. of result partitions> <storageLevel> <model>")
      System.exit(1)
    }
    val input = args(0)
    val output =  args(1)
    val step = args(2).toInt
    val maxDegree = args(3).toInt
    val numPartitions = args(4).toInt
    val storageLevel = args(5).toInt match {
        case 0 => StorageLevel.OFF_HEAP
        case 1 => StorageLevel.DISK_ONLY
        case 2 => StorageLevel.DISK_ONLY_2
        case 3 => StorageLevel.MEMORY_ONLY
        case 4 => StorageLevel.MEMORY_ONLY_2
        case 5 => StorageLevel.MEMORY_ONLY_SER 
        case 6 => StorageLevel.MEMORY_ONLY_SER_2
        case 7 => StorageLevel.MEMORY_AND_DISK
        case 8 => StorageLevel.MEMORY_AND_DISK_2
        case 9 => StorageLevel.MEMORY_AND_DISK_SER
        case 10 => StorageLevel.MEMORY_AND_DISK_SER_2
        case _ => StorageLevel.MEMORY_AND_DISK
    }
    val disableKryo = args(6).toBoolean
    val model = args(7)

    (input, output, step, maxDegree, numPartitions, storageLevel, disableKryo, model)
  }
  
  def main(args: Array[String]) {
    val (input, output, step, maxDegree, numPartitions, storageLevel, disableKryo, model) = parseArgs(args)

    if(!disableKryo) {
      System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    }
    val sparkConf = new SparkConf()
    if (model.toLowerCase == "graphx") 
      sparkConf.setAppName("NWeightGraphX")
    else
      sparkConf.setAppName("NWeightPregel")
    val sc = new SparkContext(sparkConf)
 
    sc.addSparkListener(new JobLogger)
    sc.addSparkListener(new StatsReportListener)

    if (model.toLowerCase == "graphx") {
      GraphxNWeight.nweight(sc, input, output, step, maxDegree, numPartitions, storageLevel)
    } else {
      PregelNWeight.nweight(sc, input, output, step, maxDegree, numPartitions, storageLevel)
    }
  }
}
