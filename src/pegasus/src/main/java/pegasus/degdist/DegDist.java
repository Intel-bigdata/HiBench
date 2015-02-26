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
File: DegDist.java
 - Degree Distribution
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

public class DegDist extends Configured implements Tool
{
	static int InDeg = 1, OutDeg = 2, InOutDeg = 3;

    //////////////////////////////////////////////////////////////////////
    // PASS 1: group by node id.
	//  Input : edge list
	//  Output : key(node_id), value(degree)
    //////////////////////////////////////////////////////////////////////
	public static class	MapPass1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
		int deg_type = 0;

		public void configure(JobConf job) {
			deg_type = Integer.parseInt(job.get("deg_type"));

			System.out.println("MapPass1 : configure is called. degtype = " + deg_type );
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			String[] line = line_text.split("\t");
			IntWritable one_int = new IntWritable(1);

			if( deg_type == OutDeg ) {
				IntWritable key_node_int = new IntWritable();
				key_node_int.set(Integer.parseInt(line[0]));

				output.collect(key_node_int, one_int);
			} else if( deg_type == InDeg) {
				output.collect( new IntWritable(Integer.parseInt(line[1])), one_int );
			} else if( deg_type == InOutDeg) {		// emit both
				IntWritable from_node_int = new IntWritable();
				IntWritable to_node_int = new IntWritable();
				from_node_int.set(Integer.parseInt(line[0]));
				to_node_int.set(Integer.parseInt(line[1]));

				output.collect(from_node_int, to_node_int);
				output.collect(to_node_int, from_node_int);
			}
		}
	}

    public static class	RedPass1 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
		private final IntWritable one_int = new IntWritable(1);

		int deg_type = 0;

		public void configure(JobConf job) {
			deg_type = Integer.parseInt(job.get("deg_type"));

			System.out.println("RedPass1 : configure is called. degtype = " + deg_type );
		}

		public void reduce (final IntWritable key, final Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
        {
			int degree = 0;

			if( deg_type != InOutDeg) {
				while (values.hasNext()) {
					int cur_degree = values.next().get();
					degree += cur_degree;
				}

				output.collect(key, new IntWritable(degree) );
			} else { // deg_type == InOutDeg
				Set<Integer> outEdgeSet = new TreeSet<Integer>();
				while (values.hasNext()) {
					int cur_outedge = values.next().get();
					outEdgeSet.add( cur_outedge );
				}

				output.collect(key, new IntWritable(outEdgeSet.size()) );
			}
		}
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // PASS 2: group by degree
	//  Input : key(node id), value(degree)
	//  Output : key(degree), value(count)
    ////////////////////////////////////////////////////////////////////////////////////////////////
	public static class MapPass2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			String[] line = value.toString().split("\t");

			output.collect(new IntWritable(Integer.parseInt(line[1])), new IntWritable(1) );
		}
    }
  

    public static class RedPass2 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
		public void reduce (final IntWritable key, final Iterator<IntWritable> values, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
        {
			int count = 0;

			while (values.hasNext()) {
				int cur_count = values.next().get();
				count += cur_count;
			}

			output.collect(key, new IntWritable(count) );
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path node_deg_path = null;
	protected Path deg_count_path = null;
	protected int nreducer = 1;
	protected int deg_type;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new DegDist(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("DegDist <edge_path> <node_deg_path> <deg_count_path> <in or out or inout> <# of reducer> <edge_file>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 5 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		node_deg_path = new Path(args[1]);
		deg_count_path = new Path(args[2]);

		String deg_type_str = "In";
		deg_type = InDeg;
		if( args[3].compareTo("out") == 0 ) {
			deg_type = OutDeg;
			deg_type_str = "Out";
		} else if( args[3].compareTo("inout") == 0 ) {
			deg_type = InOutDeg;
			deg_type_str = "InOut";
		}

		nreducer = Integer.parseInt(args[4]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing degree distribution. Degree type = " + deg_type_str + "\n");

		// run job
		JobClient.runJob(configPass1());
		JobClient.runJob(configPass2());

		System.out.println("\n[PEGASUS] Degree distribution computed.");
		System.out.println("[PEGASUS] (NodeId, Degree) is saved in HDFS " + args[1] + ", (Degree, Count) is saved in HDFS " + args[2] + "\n" );

		return 0;
    }

    // Configure pass1
    protected JobConf configPass1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), DegDist.class);
		conf.set("deg_type", "" + deg_type);

		conf.setJobName("DegDist_pass1");

		conf.setMapperClass(MapPass1.class);        
		conf.setReducerClass(RedPass1.class);
		if( deg_type != InOutDeg) {
			conf.setCombinerClass(RedPass1.class);
		}

		FileInputFormat.setInputPaths(conf, edge_path);  
		FileOutputFormat.setOutputPath(conf, node_deg_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }

    // Configure pass2
    protected JobConf configPass2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), DegDist.class);

		conf.setJobName("DegDist_pass2");
		
		conf.setMapperClass(MapPass2.class);        
		conf.setReducerClass(RedPass2.class);
		conf.setCombinerClass(RedPass2.class);

		FileInputFormat.setInputPaths(conf, node_deg_path);  
		FileOutputFormat.setOutputPath(conf, deg_count_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }

}

