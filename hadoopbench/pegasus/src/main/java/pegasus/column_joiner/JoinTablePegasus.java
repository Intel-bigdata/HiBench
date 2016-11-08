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
File: JoinTablePegasus.java
 - Join Columns
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

public class JoinTablePegasus extends Configured implements Tool 
{
	static int OuterJoin = 1, SemiJoin = 2;

	public static class MapPass1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		int number_tables = 0;
		int column_index = 0;

		public void configure(JobConf job) {
			number_tables = Integer.parseInt(job.get("number_tables"));

			System.out.println("MapPass1 : configure is called. number_tables=" + number_tables );

			String input_file = job.get("map.input.file");
			System.out.println("input_file=" + input_file);
			String path_name ="";
			for(int i = 1; i <= number_tables; i++) {
				path_name = job.get("path" + i);
				if( input_file.indexOf( path_name ) >= 0 ) {
					column_index = i;
					break;
				}
			}
			System.out.println("Column index = " + column_index +  ", path_name=" + path_name);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in input file
				return;

			String[] line = line_text.split("[\t]");

			int tab_pos = line_text.indexOf("\t");
			String second_str = line_text.substring(tab_pos+1);
			if( second_str.charAt(0) == 'v' )
				second_str = second_str.substring(1);
			else if( second_str.startsWith("bsf")) {
				int colon_pos = second_str.indexOf(":");
				second_str = second_str.substring(colon_pos+1);
			}

			String value_str = "" + column_index + " " + second_str;


			output.collect( new IntWritable(Integer.parseInt(line[0])), new Text(value_str) );
		}
	}

    public static class	RedPass1 extends MapReduceBase implements Reducer<IntWritable, Text, Text, Text>
    {
		int number_tables = 0;
		int join_type = OuterJoin;
		String sep = "\t";

		public void configure(JobConf job) {
			number_tables = Integer.parseInt(job.get("number_tables"));
			join_type = Integer.parseInt(job.get("join_type"));

			System.out.println("RedPass1 : configure is called. number_tables=" + number_tables );
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<Text, Text> output, final Reporter reporter) throws IOException
        {
			Map<Integer, String> val_map = new HashMap<Integer, String>();

			while (values.hasNext()) {
				String cur_string = values.next().toString();
				final String[] tokens = cur_string.split(" ");
				int column_index = Integer.parseInt(tokens[0]);

				val_map.put(column_index, tokens[1]);
			}

			String output_values = "" + key.get() ;//+ " ";
			int received_count = 0;
			for(int i = 1; i <= number_tables; i++) {
				String saved_val = val_map.get(i);
				if( i <= number_tables )
					output_values += sep;

				if( saved_val != null ) {
					output_values += saved_val;
					received_count++;
				}
			}

			if( join_type == OuterJoin )
				output.collect(new Text(output_values), new Text("") );
			else if( received_count == number_tables )	// SemiJoin
				output.collect(new Text(output_values), new Text("") );
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected Path output_path = null;
	ArrayList<Path> input_paths = new ArrayList<Path>();
	protected int nreducer = 10;
	protected int number_tables = 1;	// number of values to join
	int join_type = OuterJoin;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new JoinTablePegasus(), args);

		System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("JoinTablePegasus <# of reducers> <OuterJoin or SemiJoin> <output_path> <input_path 1> <input_path 2> ...");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length < 5 ) {
			return printUsage();
		}

		nreducer = Integer.parseInt(args[0]);
		if( args[1].startsWith("Semi") )
			join_type = SemiJoin;
		output_path = new Path(args[2]);

		number_tables = args.length - 3;

		System.out.println("Output path = " + args[2] + ", Nreducer =" + nreducer  + ", number_tables=" + number_tables );
		if( join_type == OuterJoin )
			System.out.println("Join type = OuterJoin");
		else
			System.out.println("Join type = SemiJoin");

		for(int i = 3; i < args.length; i++) {
			System.out.println("  input path : " + args[i] );
			input_paths.add( new Path(args[i]) );
		}

		// run job
		JobClient.runJob(configPass1());

		System.out.println("Joined table is in HDFS " + args[2]);

		return 0;
    }

    // Configure pass1
    protected JobConf configPass1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), JoinTablePegasus.class);
		conf.set("number_tables", "" + number_tables);
		conf.set("join_type", "" + join_type);

		conf.setJobName("JoinTablePegasus");

		conf.setMapperClass(MapPass1.class);
		conf.setReducerClass(RedPass1.class);

		int i = 1;
		Iterator<Path> iter = input_paths.iterator();
		while( iter.hasNext() ){
			Path cur_path = iter.next();
			FileInputFormat.addInputPath(conf, cur_path);  
			conf.set("path" + i, cur_path.toString() );
			i++;
		}
		FileOutputFormat.setOutputPath(conf, output_path);  

		final FileSystem fs = FileSystem.get(conf);
		fs.delete(output_path);

		conf.setNumReduceTasks( nreducer );

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}
