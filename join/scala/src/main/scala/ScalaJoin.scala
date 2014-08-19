
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext

object ScalaJoin{
  def main(args: Array[String]){
    if (args.length < 2){
      System.err.println(
        s"Usage: $ScalaJoin <INPUT_DATA_URL> <OUTPUT_DATA_URL>"
      )
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("ScalaJoin")//.setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)

    hc.hql("DROP TABLE IF EXISTS rankings")
    hc.hql("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/rankings'".format(args(0)))
    hc.hql("FROM rankings SELECT *").saveAsTextFile("%s/rankings".format(args(1)))
    sc.stop()
  }
}
