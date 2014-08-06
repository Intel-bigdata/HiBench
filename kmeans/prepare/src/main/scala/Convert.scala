import java.io._

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.hadoop.util.ReflectionUtils

private class TextRecordInputStream(fs: FileSystem, path:Path, conf:Configuration) extends InputStream{
  var r: SequenceFile.Reader = new SequenceFile.Reader(fs, path, conf)
  var key: WritableComparable[_] = ReflectionUtils.newInstance(r.getKeyClass.asSubclass(classOf[WritableComparable[_]]), conf)
  var `val`: Writable = ReflectionUtils.newInstance(r.getValueClass.asSubclass(classOf[Writable]), conf)
  
  val inbuf = new DataInputBuffer
  val outbuf = new DataOutputBuffer


  def read: Int = {
    var ret: Int = 0
    if (null == inbuf || -1 == {
      ret = inbuf.read
      ret
    }) {
      if (!r.next(key, `val`)) {
        return -1
      }
      var tmp: Array[Byte] = key.toString.getBytes
      outbuf.write(tmp, 0, tmp.length)
      outbuf.write('\t')
      tmp = `val`.toString.getBytes
      outbuf.write(tmp, 0, tmp.length)
      outbuf.write('\n')
      inbuf.reset(outbuf.getData, outbuf.getLength)
      outbuf.reset
      ret = inbuf.read
    }
    ret
  }
}

object Convert{
    val conf = new Configuration()
    def main(args: Array[String]){
      if (args.length>3){
        System.err.println("Usage: Convert <hdfs_master> <input_directory> <output_file_path>")
        System.exit(1)
      }

      val hdfs_master =  "hdfs://localhost:54310/"
      val input_path =   "/HiBench/KMeans/Input/samples/"
      val output_name =  "/HiBench/Pagerank/Input/samples.txt"

      conf.setStrings("fs.default.name", hdfs_master)
      conf.setStrings("dfs.replication", "1")

      val fileSystem = FileSystem.get(conf)
      val out = fileSystem.create(new Path(output_name)) //new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(output_name))))

      val dirs = fileSystem.listStatus(new Path(input_path))
      dirs.foreach { it =>
        if (it.getPath.getName.startsWith("part-")) {
          println("open file", it.getPath)
          val in = new BufferedReader(new InputStreamReader(new TextRecordInputStream(fileSystem, it.getPath, conf), "utf-8"))
          Iterator.continually(in.readLine()).takeWhile(_ != null).foreach{line =>
            println(line)
            //val elements = line.split('\t')
            //out.write("%s  %s\n".format(elements(1), elements(2)).getBytes)
          }
          in.close()
        }
      }
      out.close()
    }
  }

