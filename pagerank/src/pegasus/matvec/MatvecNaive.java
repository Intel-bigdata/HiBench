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
File: MatvecNaive.java
 - Plain matrix vector multiplication.
Version: 2.0
***********************************************************************/

package pegasus.matvec;

import java.io.*;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MatvecNaive extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // PASS 1: Hash join using Vector.rowid == Matrix.colid
    //////////////////////////////////////////////////////////////////////
	public static class MapPass1 extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
    {
		private final LongWritable from_node_int = new LongWritable();
		int makesym = 0;
		int transpose = 0;
		int ignore_weights = 0;

		public void configure(JobConf job) {
			makesym = Integer.parseInt(job.get("makesym"));
			transpose = Integer.parseInt(job.get("transpose"));
			ignore_weights = Integer.parseInt(job.get("ignore_weights"));

			String input_file = job.get("map.input.file");

			System.out.println("MatvecNaive.MapPass1: makesym = " + makesym);
			System.out.println("input_file = " + input_file);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");

			if( line.length == 2 || ignore_weights == 1) {	// vector : ROWID	VALUE('vNNNN')
				if( line[1].charAt(0) == 'v' ) {	// vector : ROWID	VALUE('vNNNN')
					from_node_int.set( Long.parseLong(line[0]) );
					output.collect( from_node_int, new Text(line[1]) );
				} else {					// edge : ROWID		COLID
					if(transpose == 0) {
						output.collect( new LongWritable(Long.parseLong(line[1])), new Text(line[0]) );
						if(makesym == 1)
							output.collect( new LongWritable(Long.parseLong(line[0])), new Text(line[1]) );
					} else {
						output.collect( new LongWritable(Long.parseLong(line[0])), new Text(line[1]) );
						if(makesym == 1)
							output.collect( new LongWritable(Long.parseLong(line[1])), new Text(line[0]) );
					}
				}
			} else if(line.length == 3) {					// edge: ROWID    COLID    VALUE
				if(transpose == 0) {
					output.collect( new LongWritable(Long.parseLong(line[1])), new Text(line[0] + "\t" + line[2]) );
					if(makesym == 1)
						output.collect( new LongWritable(Long.parseLong(line[0])), new Text(line[1] + "\t" + line[2]) );
				} else {
					output.collect( new LongWritable(Long.parseLong(line[0])), new Text(line[1] + "\t" + line[2]) );
					if(makesym == 1)
						output.collect( new LongWritable(Long.parseLong(line[1])), new Text(line[0] + "\t" + line[2]) );
				}
			}
		}
	}

    public static class RedPass1 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, DoubleWritable>
    {
	    ArrayList<Long> to_nodes_list = new ArrayList<Long>();
	    ArrayList<Double> to_val_list = new ArrayList<Double>();

		public void reduce (final LongWritable key, final Iterator<Text> values, final OutputCollector<LongWritable, DoubleWritable> output, final Reporter reporter) throws IOException
        {
			int i;
			double vector_val = 0;

			boolean isValReceived = false;

	        Map<Long, Double> to_map = new HashMap<Long, Double>();

			while (values.hasNext()) {
				String line_text = values.next().toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	
					if(line_text.charAt(0) == 'v') {	// vector : VALUE
						vector_val = Double.parseDouble(line_text.substring(1));
						if( isValReceived == false ) {
							isValReceived = true;

							// empty queue
							Iterator<Map.Entry<Long, Double>> iter = to_map.entrySet().iterator();
							while(iter.hasNext()){
								Map.Entry<Long, Double> entry = iter.next();
								output.collect( new LongWritable( entry.getKey() ), new DoubleWritable( vector_val * entry.getValue() ) );
							}
							to_map.clear();
						}
					} else {		// edge : ROWID
						if( isValReceived == false )
							to_map.put(Long.parseLong(line[0]), new Double(1.0) );
						else {
							output.collect(new LongWritable(Long.parseLong(line[0])), new DoubleWritable(vector_val) );
						}
					}
				} else {					// edge : ROWID		VALUE
					if( isValReceived == false )
						to_map.put(Long.parseLong(line[0]), Double.parseDouble(line[1]) );
					else {
						output.collect(new LongWritable(Long.parseLong(line[0])), new DoubleWritable(vector_val * Double.parseDouble(line[1])) );
					}
				}
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // PASS 2: merge partial multiplication results
    //////////////////////////////////////////////////////////////////////
	public static class MapPass2 extends MapReduceBase
	implements Mapper<LongWritable, Text, LongWritable, DoubleWritable>
    {
		private final LongWritable from_node_int = new LongWritable();

		public void map (final LongWritable key, final Text value, final OutputCollector<LongWritable, DoubleWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			from_node_int.set( Long.parseLong(line[0]) );
			output.collect( from_node_int, new DoubleWritable( Double.parseDouble(line[1]) ) );
		}
	}

    public static class RedPass2 extends MapReduceBase
	implements Reducer<LongWritable, DoubleWritable, LongWritable, Text>
    {
		public void reduce (final LongWritable key, final Iterator<DoubleWritable> values, final OutputCollector<LongWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
			double next_rank = 0;

			while (values.hasNext()) {
				String cur_value_str = values.next().toString();
				next_rank += Double.parseDouble( cur_value_str ) ;
			}

			output.collect( key, new Text( "v" + next_rank ) );
		}
    }


    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path tempmv_path = null;
	protected Path output_path = null;
	protected Path vector_path = null;
	protected int number_nodes = 0;
	protected int nreducer = 1;
	int makesym = 0;
	int transpose = 0;
	int ignore_weights = 0;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new MatvecNaive(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("MatvecNaive <edge_path> <tempmv_path> <output_path> <# of nodes> <# of reducers> <makesym or nosym>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length < 5 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		tempmv_path = new Path(args[1]);
		output_path = new Path(args[2]);				
		nreducer = Integer.parseInt(args[3]);
		if(args[4].equals("makesym"))
			makesym = 1;
		if( args.length > 5 )
			vector_path = new Path(args[5]);
		if( args.length > 6 )
			transpose = Integer.parseInt(args[6]);
		if( args.length > 7 )
			ignore_weights = Integer.parseInt(args[7]);

		final FileSystem fs = FileSystem.get(getConf());
		fs.delete(tempmv_path);
		fs.delete(output_path);

		JobClient.runJob(configPass1());
		JobClient.runJob(configPass2());

		fs.delete(tempmv_path);

		return 0;
    }

	// Configure pass1
    protected JobConf configPass1 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), MatvecNaive.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("makesym", "" + makesym);
		conf.set("transpose", "" + transpose);
		conf.set("ignore_weights", "" + ignore_weights);

		conf.setJobName("MatvecNaive_pass1");
		
		conf.setMapperClass(MapPass1.class);        
		conf.setReducerClass(RedPass1.class);

		if( vector_path == null )
			FileInputFormat.setInputPaths(conf, edge_path);  
		else
			FileInputFormat.setInputPaths(conf, edge_path, vector_path);  
		FileOutputFormat.setOutputPath(conf, tempmv_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapOutputValueClass(Text.class);

		return conf;
    }

	// Configure pass2
    protected JobConf configPass2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), MatvecNaive.class);
		conf.set("number_nodes", "" + number_nodes);

		conf.setJobName("MatvecNaive_pass2");
		
		conf.setMapperClass(MapPass2.class);        
		conf.setReducerClass(RedPass2.class);

		FileInputFormat.setInputPaths(conf, tempmv_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(LongWritable.class);
		conf.setMapOutputValueClass(DoubleWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

}

