
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaWordCount{
  def main(args: Array[String]){
    if (args.length < 1){
      System.err.println(
        s"Usage: $ScalaWordCount <INPUT_DATA_URL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaWordCount")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.collect().foreach(it =>
      println(it)
    )
    sc.stop()
  }
}
