package com.intel.hammer.log.generator.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.intel.hammer.log.util.Common;


public class LogGenJob {

	public static void main(String args[]) {
		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs();

			if ((5 != otherArgs.length)&&(3 != otherArgs.length)) {
				System.err.println("Usage: exe <in> <out> <partNum> <maxUser> <maxItem>");
				System.exit(2);
			}

			if (5 == otherArgs.length) {
				Common.SPLIT_NUM = Integer.valueOf(otherArgs[2]);
				Common.CUSTOMER_MAX = Integer.valueOf(otherArgs[3]);
				Common.ITEM_MAX = Integer.valueOf(otherArgs[4]);
			}

//			conf.set("stream.reduce.output.field.separator", "");
			conf.setInt("reduce.output.split.num", Common.SPLIT_NUM);
//			conf.setBoolean("mapred.compress.map.output", false); 
			
			Job job = new Job(conf, "Hammer Job - Generate web logs");
			job.setJarByClass(LogGenJob.class);
			job.setMapperClass(LogGenMapper.class);
			job.setReducerClass(LogGenReducer.class);
			job.setPartitionerClass(LogGenPartitioner.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			job.setNumReduceTasks(Common.REDUCE_NUM);

			for (int i = 1; i <= Common.SPLIT_NUM; i++) {
				MultipleOutputs.addNamedOutput(job, Integer.toString(i),
						TextOutputFormat.class, LongWritable.class, Text.class);
			}
			MultipleOutputs.addNamedOutput(job, "cookies",
					TextOutputFormat.class, LongWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "useragents",
					TextOutputFormat.class, LongWritable.class, Text.class);
			MultipleOutputs.addNamedOutput(job, "ip",
					TextOutputFormat.class, LongWritable.class, Text.class);
			
			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
