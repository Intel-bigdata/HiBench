import java.io._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/*
 * Convert generated document by mahout into vector based on frequency of words
 * Spark needed!
 */

object Convert{
  val conf = new Configuration()
  def main(args: Array[String]){
    if (args.length!=2){
      System.err.println("Usage: Convert <hdfs_master> <input_path>")
      System.exit(1)
    }

    val hdfs_master =  "hdfs://localhost:54310/"
    val input_path =   "hdfs://localhost:54310/HiBench/Bayes/Input"
    val output_name =  input_path +"/samples.txt"
    val output_vector_name = input_path + "/vectors.txt"
    val hdfs_head = hdfs_master.length - 1

    conf.setStrings("fs.default.name", hdfs_master)
    conf.setStrings("dfs.replication", "1")

    val fileSystem = FileSystem.get(conf)
    val out = fileSystem.create(new Path(output_name.substring(hdfs_head))) //new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(output_name))))

    val dirs = fileSystem.listStatus(new Path(input_path.substring(hdfs_head)))
    dirs.foreach { it =>
      if (it.getPath.getName.startsWith("part-")) {
        println("Processing file %s".format(it.getPath))
        IterTextRecordInputStream(fileSystem, it.getPath, conf, out)
      }
    }
    out.close()

    // calc word frequency
    val sparkConf = new SparkConf().setAppName("Doc2Vector")
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(output_name).map({ line =>
      val x = line.split(":")
      (x(0), x(1))
    }) // map line to key:data
    val wordcount = data.flatMap{case(key, doc) => doc.split(" ")}
                        .map(word => (word, 1))
                        .reduceByKey(_ + _)
    val wordsum = wordcount.map(_._2).reduce(_ +_)

    val word_dict = wordcount.zipWithIndex()
                             .map{case ((key, count), index)=> (key, (index, count.toDouble / wordsum))}
                             .collectAsMap()
    val shared_word_dict = sc.broadcast(word_dict)

    // for each document, generate vector based on word freq
    val vector = data.map { case (key, doc) =>
      val doc_vector = doc.split(" ").map(x => shared_word_dict.value(x)) //map to word index: freq
        .groupBy(_._1) // combine freq with same word
        .map { case (k, v) => (k, v.map(_._2).sum)}

      val sorted_doc_vector = doc_vector.toList.sortBy(_._1)
        .map { case (k, v) => "%d:%f".format(k + 1,  // LIBSVM's index starts from 1 !!!
                                             v)} // convert to LIBSVM format

      // key := /classXXX
      // key.substring(6) := XXX
      key.substring(6) + " " + sorted_doc_vector.mkString(" ") // label index1:value1 index2:value2 ...
    }
//    println(vector.count())
//    println("delete:" + output_vector_name.substring(hdfs_head))
    try { fileSystem.delete(new Path(output_vector_name.substring(hdfs_head)), true) } catch { case _ : Throwable => { } }
    vector.saveAsTextFile(output_vector_name)
    fileSystem.close()
    sc.stop()
  }

  def IterTextRecordInputStream(fs: FileSystem, path:Path, conf:Configuration, out:FSDataOutputStream):Int = {
    var r: SequenceFile.Reader = new SequenceFile.Reader(fs, path, conf)
    var key:Writable = ReflectionUtils.newInstance(r.getKeyClass.asSubclass(classOf[Writable]), conf)
    var `val`:Writable = ReflectionUtils.newInstance(r.getValueClass.asSubclass(classOf[Writable]), conf)

    var vector:Array[Double] = null
    var size = -1
    while (true){
      if (!r.next(key, `val`)) return -1
      out.write("%s:%s\n".format(key, `val`).getBytes)
    }
    0
  }
}

