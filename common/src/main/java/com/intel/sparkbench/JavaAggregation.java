package com.intel.sparkbench.aggregation;

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

public final class JavaAggregation {
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaAggregation <hdfs_url>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaAggregation");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaHiveContext hc = new JavaHiveContext(ctx);

    hc.hql("DROP TABLE IF EXISTS uservisits");
    hc.hql("DROP TABLE IF EXISTS uservisits_aggre");
    hc.hql(String.format("CREATE EXTERNAL TABLE uservisits (sourceIP STRING,destURL STRING,visitDate STRING,adRevenue DOUBLE,userAgent STRING,countryCode STRING,languageCode STRING,searchWord STRING,duration INT ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS SEQUENCEFILE LOCATION '%s/uservisits'", args[0]));
    hc.hql(String.format("CREATE TABLE uservisits_aggre ( sourceIP STRING, sumAdRevenue DOUBLE) STORED AS SEQUENCEFILE LOCATION '%s/uservisits_aggre'", args[1]));
    hc.hql("INSERT OVERWRITE TABLE uservisits_aggre SELECT sourceIP, SUM(adRevenue) FROM uservisits GROUP BY sourceIP");
    
    ctx.stop();
  }
}

