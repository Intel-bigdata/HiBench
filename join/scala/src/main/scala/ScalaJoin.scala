
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
    hc.hql("DROP TABLE IF EXISTS uservisits")
    hc.hql("DROP TABLE IF EXISTS rankings_uservisits_join")
    hc.hql("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/rankings'".format(args(0)))
    hc.hql("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits'".format(args(0)))
    hc.hql("CREATE TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'".format(args(1)))
    hc.hql("INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC limit 1")

    sc.stop()
  }
}
