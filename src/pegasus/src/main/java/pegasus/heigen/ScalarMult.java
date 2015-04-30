/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Authors: U Kang, Duen Horng Chau, and Christos Faloutsos

This software is licensed under Apache License, Version 2.0 (the  "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-------------------------------------------------------------------------
File: ScalarMult.java
 - Multiply a vector with a scalar.
Version: 2.0
***********************************************************************/

package pegasus;

import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

// y = sy
public class ScalarMult extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: make initial pagerank vector
    //////////////////////////////////////////////////////////////////////

	// MapStage1: 
	public static class MapStage1Double extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable>
    {
		private final IntWritable from_node_int = new IntWritable();
		private boolean isYpath = false;
		private boolean isXpath = false;
		private double s;

		public void configure(JobConf job) {
			s = Double.parseDouble(job.get("s"));

			System.out.println("ScalarMult.MapStage1: s = " + s);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, DoubleWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			int tabpos = line_text.indexOf("\t");

			int out_key = Integer.parseInt(line_text.substring(0, tabpos));
			double out_val = 0;

			if( line_text.charAt(tabpos+1) == 'v') {
				out_val = Double.parseDouble(line_text.substring(tabpos+2));
			} else {
				out_val = Double.parseDouble(line_text.substring(tabpos+1));
			}

			output.collect(new IntWritable(out_key), new DoubleWritable(s*out_val));
		}
	}

	public static class MapStage1Text extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		private final IntWritable from_node_int = new IntWritable();
		private boolean isYpath = false;
		private boolean isXpath = false;
		private double s;

		public void configure(JobConf job) {
			s = Double.parseDouble(job.get("s"));

			System.out.println("ScalarMult.MapStage1: s = " + s);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			int tabpos = line_text.indexOf("\t");

			int out_key = Integer.parseInt(line_text.substring(0, tabpos));
			double out_val = 0;

			if( line_text.charAt(tabpos+1) == 'v') {
				out_val = Double.parseDouble(line_text.substring(tabpos+2));
			} else {
				out_val = Double.parseDouble(line_text.substring(tabpos+1));
			}

			output.collect(new IntWritable(out_key), new Text("v" + (s*out_val)) );
		}
	}


    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected int nreducers = 1;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new ScalarMult(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("ScalarMult <in_path> <s>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 2 ) {
			return printUsage();
		}

		Path in_path = new Path(args[0]);
		double s = Double.parseDouble(args[1]);

		final FileSystem fs = FileSystem.get(getConf());

		Path smult_output = new Path("smult_output");
		fs.delete(smult_output);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing ScalarMult. In_path=" + in_path.getName() + ", s=" + s + "\n");

		JobClient.runJob( configScalarMult(in_path, smult_output, s) );

		System.out.println("\n[PEGASUS] ScalarMult computed. Output is saved in HDFS " + smult_output.getName() + "\n");

		return 0;
    }

	// Configure ScalarMult
    protected JobConf configScalarMult (Path in_path, Path smult_output, double s) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ScalarMult.class);
		conf.set("s", "" + s);
		conf.setJobName("ScalarMult");

		conf.setMapperClass(ScalarMult.MapStage1Text.class);        

		FileInputFormat.setInputPaths(conf, in_path);  
		FileOutputFormat.setOutputPath(conf, smult_output);  

		conf.setNumReduceTasks( 0 );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);//conf.setOutputValueClass(DoubleWritable.class);

		return conf;
    }

}

