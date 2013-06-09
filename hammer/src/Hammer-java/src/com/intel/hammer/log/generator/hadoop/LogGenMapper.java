package com.intel.hammer.log.generator.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import ua.main.java.eu.bitwalker.useragentutils.UserAgent;

import com.intel.hammer.log.generator.entity.SalesRow;
import com.intel.hammer.log.generator.entity.WebLog;
import com.intel.hammer.log.util.Common;
import com.intel.hammer.log.util.EnhancedRandom;
import com.intel.hammer.log.util.IPUtil;

public class LogGenMapper extends Mapper<Object, Text, LongWritable, Text> {
	public EnhancedRandom random;
	public MultipleOutputs<LongWritable, Text> mos;
	
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		random = new EnhancedRandom();
		mos = new MultipleOutputs<LongWritable, Text>(context);
		super.setup(context);
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		if (value.toString().contains("\\N")) {
			return;
		}

		String[] cells = value.toString().split("\\|");

		java.sql.Date thisdate = java.sql.Date.valueOf(cells[3]);
		java.util.Calendar Cal = java.util.Calendar.getInstance();
		Cal.setTime(thisdate);
		
		long timeInSeconds = Cal.getTimeInMillis() / 1000;
		
		SalesRow row = new SalesRow();
		row.time = cells[0];
		row.item = cells[1];
		row.customer = cells[2];
		row.date = cells[3];
		row.url = cells[4];
		
		WebLog log = row.createLog();
		context.write(
				new LongWritable(timeInSeconds
						+ Long.parseLong(row.time)), new Text(log.toString()));

		// generate logs for this customer, gaussian random with mean=3
		int number = (int) random.nextGaussian(3);
		for (int i = 0; i < number; i++) {
			row.item = Integer.toString(random.nextInt(Common.ITEM_MAX) + 1);
			row.time = Long.toString(Long.parseLong(row.time)
					+ (int) random.nextGaussian(5));
			log = row.createLog();
			context.write(
					new LongWritable(timeInSeconds
							+ Long.parseLong(row.time)),
					new Text(log.toString()));
		}
		
		// write to dim tables
		mos.write("cookies", row.customer, new Text(log.cookieId));
		mos.write("ip", IPUtil.ipToInt(log.ip), new Text(log.ip));
		
		UserAgent ua = UserAgent.parseUserAgentString(log.agent);
		mos.write("useragents", ua.getId(), new Text(ua.getOperatingSystem() + "\t" + ua.getBrowser() + ua.getBrowserVersion()));
		
		// generate logs for other 2 customers randomly
		number = (int) random.nextGaussian(4);
		int split = random.nextInt(Math.abs(number) + 1) + 1;
		int j = 0;

		// customer 1
		row.customer = Integer.toString(random.nextInt(Common.CUSTOMER_MAX)+1);
		int time_mean = random.nextInt(86400);
		for (; j < split; j++) {
			row.item = Integer.toString(Math.abs(random.nextInt(Common.ITEM_MAX))+1);
			long time = Math.abs(time_mean + (int) random.nextGaussian(5));
			row.time = Long.toString(time);
			log = row.createLog();
			context.write(
					new LongWritable(timeInSeconds + time),
					new Text(log.toString()));
		}

		// customer 2
		row.customer = Integer.toString(random.nextInt(Common.CUSTOMER_MAX)+1);
		time_mean = random.nextInt(86400);
		for (; j < number; j++) {
			row.item = Integer.toString(random.nextInt(Common.ITEM_MAX)+1);
			long time = Math.abs(time_mean + (int) random.nextGaussian(5));
			row.time = Long.toString(time);
			log = row.createLog();
			context.write(
					new LongWritable(timeInSeconds + time),
					new Text(log.toString()));
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
		super.cleanup(context);
	}

}
