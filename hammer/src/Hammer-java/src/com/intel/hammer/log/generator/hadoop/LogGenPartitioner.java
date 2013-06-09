package com.intel.hammer.log.generator.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LogGenPartitioner extends Partitioner<LongWritable, Text> {
	@Override
	public int getPartition(LongWritable key, Text value, int numPartitions) {
		// partition by month
		// java.util.Date date = new java.util.Date(key.get() * 1000);
		// int year = date.getYear();
		// int month = year * 12 + date.getMonth();
		// return month % numPartitions;

		// partition by date
		java.util.Calendar Cal = java.util.Calendar.getInstance();
		Cal.setTime(java.sql.Date.valueOf(new java.text.SimpleDateFormat(
				"yyyy-MM-dd").format(new java.util.Date(key.get() * 1000))));
		
		return (int) (Cal.getTimeInMillis() / 86400000 % numPartitions);
	}
}