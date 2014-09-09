import java.io._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.mahout.math.VectorWritable
import org.apache.mahout.math.Vector
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._



object Convert{
  val conf = new Configuration()
  def main(args: Array[String]){
//    if (args.length!=4){
//      System.err.println("Usage: Convert <hdfs_master> <input_directory> <output_file_path> <PARALLEL>")
//      System.exit(1)
//    }

    val sparkConf = new SparkConf().setAppName("Doc2Vector")
    val sc = new SparkContext(sparkConf)

//    val hdfs_master =  args(0) //"hdfs://localhost:54310/"
    val input_path =   "hdfs://localhost:54310/SparkBench/KMeans/Input/samples/"
//    val output_name =  args(2) //"/HiBench/KMeans/Input/samples.txt"
//    val parallel = args(3).toInt

    val data = sc.textFile(input_path)
    data.foreach{line=>
      println(line)
    }
    sc.stop()
//
//    conf.setStrings("fs.default.name", hdfs_master)
//    conf.setStrings("dfs.replication", "1")
//
//    val fileSystem = FileSystem.get(conf)
//    val out = fileSystem.create(new Path(output_name)) //new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(output_name))))
//
//    val dirs = fileSystem.listStatus(new Path(input_path))
//    dirs.foreach { it =>
//      if (it.getPath.getName.startsWith("part-")) {
//        println("Processing file %s".format(it.getPath))
//        IterTextRecordInputStream(fileSystem, it.getPath, conf, out)
//      }
//    }
//    out.close()
  }

  def IterTextRecordInputStream(fs: FileSystem, path:Path, conf:Configuration, out:FSDataOutputStream):Int = {
    var r: SequenceFile.Reader = new SequenceFile.Reader(fs, path, conf)
    var key:Writable = ReflectionUtils.newInstance(r.getKeyClass.asSubclass(classOf[Writable]), conf)
    var `val`:VectorWritable = ReflectionUtils.newInstance(r.getValueClass.asSubclass(classOf[VectorWritable]), conf)

    var vector:Array[Double] = null
    var size = -1
    while (true){
      if (!r.next(key, `val`)) return -1
      val v:Vector = `val`.get()
      if (vector == null) {
        vector = new Array[Double](v.size)
        size = v.size
      }
      for (i <- 0 until size) vector(i) = v.get(i)
      out.write((vector.mkString(" ")+"\n").getBytes)
    }
    0
  }
}

