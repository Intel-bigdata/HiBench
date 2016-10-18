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
File: SaxpyBlock.java
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
public class SaxpyBlock extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: make initial pagerank vector
    //////////////////////////////////////////////////////////////////////

	// MapStage1: 
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		private final IntWritable from_node_int = new IntWritable();
		private boolean isYpath = false;
		private boolean isXpath = false;
		private double a;
		protected int block_width = 16;

		public void configure(JobConf job) {
			String y_path = job.get("y_path");
			String x_path = job.get("x_path");
			a = Double.parseDouble(job.get("a"));
			block_width = Integer.parseInt(job.get("block_width"));

			String input_file = job.get("map.input.file");
			if(input_file.contains(y_path))
				isYpath = true;
			else if(input_file.contains(x_path))
				isXpath = true;

			System.out.println("SaxpyBlock.MapStage1: map.input.file = " + input_file + ", isYpath=" + isYpath + ", isXpath=" + isXpath + ", a=" + a + ", block_width=" + block_width);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			int tabpos = line_text.indexOf("\t");

			int out_key = Integer.parseInt(line_text.substring(0, tabpos));
			String val_str = line_text.substring(tabpos+1);

			char fc = val_str.charAt(0);
			if( fc == 's' || fc == 'v')
				val_str = val_str.substring(1);

			if( isYpath ) {
				output.collect( new IntWritable(out_key) , new Text(val_str) );

			} else if( isXpath ) {
				double []xvec = MatvecUtils.decodeBlockVector(val_str, block_width) ;
				for(int i = 0; i < block_width; i++) {
					if( xvec[i] != 0 )
						xvec[i] = xvec[i] * a;
				}

				String new_val_str = MatvecUtils.encodeBlockVector(xvec, block_width);

				output.collect( new IntWritable(out_key) , new Text( new_val_str ) );
			}
		}
	}

	// RedStage1
    public static class RedStage1 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		protected int block_width = 16;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));
			System.out.println("DotProductBlock:RedStage1 : configure is called. block_width=" + block_width);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			double[] v1 = null, v2 = null;
			double[] result = new double[block_width];
			int input_index= 0;

			while (values.hasNext()) {
				String cur_value_str = values.next().toString();

				if(input_index == 0 ) {
					v1 = MatvecUtils.decodeBlockVector( cur_value_str, block_width );
					input_index++;
				} else
					v2 = MatvecUtils.decodeBlockVector( cur_value_str, block_width );
			}

			int i ;
			if( v1 != null && v2 != null ) {
				for(i = 0; i < block_width ; i++)
					result[i] = v1[i] + v2[i];
			} else if( v1 != null && v2 == null ) {
				for(i = 0; i < block_width ; i++)
					result[i] = v1[i];
			} else if( v1 == null && v2 != null ) {
				for(i = 0; i < block_width ; i++)
					result[i] = v2[i];
			} else {
				for(i = 0; i < block_width ; i++)
					result[i] = 0;
			}

			String new_val_str = MatvecUtils.encodeBlockVector(result, block_width);

			if( new_val_str.length() > 0 )
				output.collect( key , new Text( new_val_str ) );
		}
    }



    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected int nreducers = 1;
	int block_width = 16;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new SaxpyBlock(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("SaxpyBlock <# of reducers> <y_path> <x_path> <a>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 5 ) {
			return printUsage();
		}

		int ret_val = 0;

		nreducers = Integer.parseInt(args[0]);
		Path y_path = new Path(args[1]);
		Path  x_path = new Path(args[2]);
		double param_a = Double.parseDouble(args[3]);
		block_width = Integer.parseInt(args[4]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing SaxpyBlock. y_path=" + y_path.getName() + ", x_path=" + x_path.getName() + ", a=" + param_a + "\n");

		final FileSystem fs = FileSystem.get(getConf());

		Path saxpy_output = new Path("saxpy_output");
		if( y_path.getName().equals("saxpy_output") ) {
			System.out.println("saxpy(): output path name is same as the input path name: changing the output path name to saxpy_output1");
			saxpy_output = new Path("saxpy_output1");
			ret_val = 1;
		}
		fs.delete(saxpy_output);

		JobClient.runJob( configSaxpy(y_path, x_path, saxpy_output, param_a) );

		System.out.println("\n[PEGASUS] SaxpyBlock computed. Output is saved in HDFS " + saxpy_output.getName() + "\n");

		return ret_val;
		// return value : 1 (output path is saxpy_output1)
		//                0 (output path is saxpy_output)
    }

	// Configure SaxpyBlock
    protected JobConf configSaxpy (Path py, Path px, Path saxpy_output, double a) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), SaxpyBlock.class);
		conf.set("y_path", py.getName());
		conf.set("x_path", px.getName());
		conf.set("a", "" + a);
		conf.set("block_width", "" + block_width);
		conf.setJobName("Lanczos.SaxpyBlock");
		
		conf.setMapperClass(MapStage1.class);        
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, py, px);  
		FileOutputFormat.setOutputPath(conf, saxpy_output);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

