import java.io.{BufferedWriter, OutputStreamWriter, InputStreamReader, BufferedReader}

import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

object Convert{
    val conf = new Configuration()
    def main(args: Array[String]){
      if (args.length<3){
        System.err.println("Usage: Convert <hdfs_master> <input_directory> <output_file_path>")
        System.exit(1)
      }

      val hdfs_master = args(0) //"hdfs://localhost:54310/"
      val input_path = args(1)  //"/HiBench/Pagerank/Input/edges/"
      val output_name = args(2) // "/HiBench/Pagerank/Input/edges.txt"

      conf.setStrings("fs.default.name", hdfs_master)
      conf.setStrings("dfs.replication", "1")

      val fileSystem = FileSystem.get(conf)
      val out = fileSystem.create(new Path(output_name)) //new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path(output_name))))

      val dirs = fileSystem.listStatus(new Path(input_path))
      dirs.foreach { it =>
        if (it.getPath.getName.startsWith("part-")) {
          println("Processing file %s".format(it.getPath))
          val in = new BufferedReader(new InputStreamReader(fileSystem.open(it.getPath), "utf-8"))
          Iterator.continually(in.readLine()).takeWhile(_ != null).foreach{line =>
            val elements = line.split('\t')
            out.write("%s  %s\n".format(elements(1), elements(2)).getBytes)
          }
          in.close()
        }
      }
      out.close()
    }
  }

