/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.dfsioe;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.dfsioe.TestDFSIOEnh.LinearInterpolator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Analyzer {
	
	public static class _Mapper extends Mapper<Object, Text, Text, Text> {
		private Text t = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tokens = new StringTokenizer(value.toString(), " \t\n\r\f%");
			String attr = tokens.nextToken(); 
			if (attr.endsWith(":tput_samples")){
	            String[] tags=attr.split(":");
	            String[] samples = tokens.nextToken().split(";");
				for(int j=0; !samples[j].startsWith("EoR"); j++){
					t.set(samples[j]);
	                context.write(new Text(tags[1]), t);
	            }
	        }
		}
	}
	
	public static class _Reducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		private int plotInterval;
		private long sampleUnit;
		private long execTime;
		private long fileSize;
		private long tStart;
		private int maxslot;
				
		public void setup(Context context) throws IOException, InterruptedException {
			this.tStart = context.getConfiguration().getLong("ana_tStart", 1347347421736L);
			this.plotInterval = context.getConfiguration().getInt("ana_plotInterval", 1000);
			this.sampleUnit = context.getConfiguration().getLong("ana_sampleUnit", 1024*1024);
			this.execTime = context.getConfiguration().getLong("ana_execTime", 767829);
			this.fileSize = context.getConfiguration().getLong("ana_fileSize", 500*1024*1024);
			this.maxslot = (int)(execTime/plotInterval)+1;
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	        LinearInterpolator processor = new LinearInterpolator();
			for (Text val : values) {
				String[] ss = val.toString().split(":");
				long timeStamp = Long.parseLong(ss[0]);
                long bytes = Long.parseLong(ss[1]);    
                double timePassed = (timeStamp-tStart)/(double)plotInterval;
                processor.add((double)timePassed,(double)bytes);
			}
			processor.add(0, 0);
			processor.add(maxslot+0.1, fileSize);
			double[] resultValue = new double [maxslot+1];
			double[]  bytesChanged = new double[maxslot+1];
			for(int i = 0; i<=maxslot; i++){
	            resultValue[i]=processor.get(i);
		    }
			for (int i = 0; i<=maxslot-1; i++) {
                bytesChanged[i] = resultValue[i+1]-resultValue[i];
			}
			bytesChanged[maxslot] = 0;
			for (int ri = 0; ri<=maxslot; ri++) {
				result.set(ri+","+resultValue[ri]/(double)sampleUnit+","+bytesChanged[ri]/(double)sampleUnit);
                context.write(key, result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(conf);
	    Path outdir = new Path("/result.txt");
	    fs.delete(outdir, true);
	    Job job = new Job(conf, "Result Analyzer");
	    job.setJarByClass(Analyzer.class);
	    job.setMapperClass(_Mapper.class);
	    job.setReducerClass(_Reducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(TestDFSIOEnh.READ_DIR, "part-00000"));
	    FileOutputFormat.setOutputPath(job, outdir);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}