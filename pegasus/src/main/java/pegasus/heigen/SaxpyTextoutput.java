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
File: SaxpyTextoutput.java
 - Compute Saxpy operation which is to compute ax + b
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

// y = y + ax
public class SaxpyTextoutput extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: make initial pagerank vector
    //////////////////////////////////////////////////////////////////////

	// MapStage1: 
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable>
    {
		private final IntWritable from_node_int = new IntWritable();
		private boolean isYpath = false;
		private boolean isXpath = false;
		private double a;

		public void configure(JobConf job) {
			String y_path = job.get("y_path");
			String x_path = job.get("x_path");
			a = Double.parseDouble(job.get("a"));

			String input_file = job.get("map.input.file");
			if(input_file.contains(y_path))
				isYpath = true;
			else if(input_file.contains(x_path))
				isXpath = true;

			System.out.println("SaxpyTextoutput.MapStage1: map.input.file = " + input_file + ", isYpath=" + isYpath + ", isXpath=" + isXpath + ", a=" + a);
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

			if( isYpath ) {
				output.collect( new IntWritable(out_key) , new DoubleWritable(out_val) );
			} else if( isXpath ) {
				output.collect( new IntWritable(out_key) , new DoubleWritable( a * out_val ) );
			}
		}
	}

		

	// RedStage1
    public static class RedStage1 extends MapReduceBase	implements Reducer<IntWritable, DoubleWritable, IntWritable, Text>
    {
		public void reduce (final IntWritable key, final Iterator<DoubleWritable> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i = 0;
			double val_double[] = new double[2];
			val_double[0] = 0;
			val_double[1] = 0;

			while (values.hasNext()) {
				val_double[i] = values.next().get();
				i++;
			}

			double result = val_double[0] + val_double[1];
			if( result != 0 )
				output.collect(key, new Text("v" + result));
		}
    }



    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected int nreducers = 1;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new SaxpyTextoutput(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("SaxpyTextoutput <# of reducers> <y_path> <x_path> <a>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 4 ) {
			return printUsage();
		}

		int ret_val = 0;

		nreducers = Integer.parseInt(args[0]);
		Path y_path = new Path(args[1]);
		Path  x_path = new Path(args[2]);
		double param_a = Double.parseDouble(args[3]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing SaxpyTextoutput. y_path=" + y_path.getName() + ", x_path=" + x_path.getName() + ", a=" + param_a + "\n");

		final FileSystem fs = FileSystem.get(getConf());

		Path saxpy_output = new Path("saxpy_output");
		if( y_path.getName().equals("saxpy_output") ) {
			System.out.println("saxpy(): output path name is same as the input path name: changing the output path name to saxpy_output1");
			saxpy_output = new Path("saxpy_output1");
			ret_val = 1;
		}
		fs.delete(saxpy_output);

		JobClient.runJob( configSaxpyTextoutput(y_path, x_path, saxpy_output, param_a) );

		System.out.println("\n[PEGASUS] SaxpyTextoutput computed. Output is saved in HDFS " + saxpy_output.getName() + "\n");

		return ret_val;
		// return value : 1 (output path is saxpy_output1)
		//                0 (output path is saxpy_output)
    }

	// Configure SaxpyTextoutput
    protected JobConf configSaxpyTextoutput (Path py, Path px, Path saxpy_output, double a) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), SaxpyTextoutput.class);
		conf.set("y_path", py.getName());
		conf.set("x_path", px.getName());
		conf.set("a", "" + a);
		conf.setJobName("SaxpyTextoutput");
		
		conf.setMapperClass(SaxpyTextoutput.MapStage1.class);        
		conf.setReducerClass(SaxpyTextoutput.RedStage1.class);

		FileInputFormat.setInputPaths(conf, py, px);  
		FileOutputFormat.setOutputPath(conf, saxpy_output);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

