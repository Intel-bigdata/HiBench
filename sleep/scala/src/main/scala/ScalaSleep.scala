
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ScalaSleep{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaSleep <parallel> <seconds>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaSleep")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val parallel = args(0).toInt
    val seconds  = args(1).toInt
    val workload = sc.parallelize(1 to parallel, parallel).map(x=> Thread.sleep(seconds * 1000L))
    workload.collect();
    sc.stop()
  }
}
