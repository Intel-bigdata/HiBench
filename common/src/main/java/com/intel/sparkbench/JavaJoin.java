package com.intel.sparkbench.join;

import org.apache.spark.sql.hive.api.java.JavaHiveContext;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaJoin {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaJoin <hdfs_url>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaJoin");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaHiveContext hc = new JavaHiveContext(ctx);

    hc.hql("DROP TABLE IF EXISTS rankings");
    hc.hql("DROP TABLE IF EXISTS uservisits");
    hc.hql("DROP TABLE IF EXISTS rankings_uservisits_join");
    hc.hql(String.format("CREATE EXTERNAL TABLE rankings (pageURL STRING, pageRank INT, avgDuration INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/rankings'", args[0]));
    hc.hql(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits'", args[0]));
    hc.hql(String.format("CREATE TABLE rankings_uservisits_join ( sourceIP STRING, avgPageRank DOUBLE, totalRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '%s/rankings_uservisits_join'", args[1]));
    hc.hql("INSERT OVERWRITE TABLE rankings_uservisits_join SELECT sourceIP, sum(adRevenue) as totalRevenue, avg(pageRank) FROM rankings R JOIN (SELECT sourceIP, destURL, adRevenue FROM uservisits UV WHERE (datediff(UV.visitDate, '1999-01-01')>=0 AND datediff(UV.visitDate, '2000-01-01')<=0)) NUV ON (R.pageURL = NUV.destURL) group by sourceIP order by totalRevenue DESC limit 1");

    ctx.stop();
  }
}

