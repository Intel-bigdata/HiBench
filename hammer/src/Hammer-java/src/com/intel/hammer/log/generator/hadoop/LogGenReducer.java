package com.intel.hammer.log.generator.hadoop;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class LogGenReducer extends
		Reducer<LongWritable, Text, LongWritable, Text> {

	public MultipleOutputs<LongWritable, Text> mos;
	public Random random;
	public int outputSplitNum = 1;

	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text val : values) {
			// random write to file
			int rand = random.nextInt(outputSplitNum);
			if ((rand < outputSplitNum) && (rand > 0)) {
				mos.write(Integer.toString(rand), null, val);
			} else {
				context.write(null, val);
			}

			// // write to dateOfmonth
			// java.util.Date date = new java.util.Date(key.get() * 1000);
			// int day = date.getDate();
			// mos.write(Integer.toString(day), null, val);

		}
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		mos = new MultipleOutputs<LongWritable, Text>(context);
		random = new Random();
		outputSplitNum = context.getConfiguration().getInt(
				"reduce.output.split.num", 1);
		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
		super.cleanup(context);
	}

}