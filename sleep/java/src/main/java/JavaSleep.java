import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaSleep{
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaSleep <Parallel> <seconds>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("JavaSleep");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    Integer parallel = Integer.parseInt(args[0]);
    Integer seconds = Integer.parseInt(args[1]);

    Integer[] init_val = new Integer[parallel];
    Arrays.fill(init_val, seconds);

    JavaRDD<Integer> workload = ctx.parallelize(Arrays.asList(init_val)).map(new Function<Integer, Integer>() {
      @Override
      public Integer call(Integer s) throws InterruptedException {
	    Thread.sleep(s * 1000);
        return 0;
      }
    });

    List<Integer> output = workload.collect();
    ctx.stop();
  }
}

