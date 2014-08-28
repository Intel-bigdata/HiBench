
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaWordCount{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaWordCount <INPUT_HDFS> <OUTPUT_HDFS>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
    sc.stop()
  }
}
