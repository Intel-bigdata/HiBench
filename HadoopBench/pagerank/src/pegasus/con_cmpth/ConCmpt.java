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
File: ConCmpt.java
 - HCC: Find Connected Components of graph
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


class ResultInfo
{
	public int changed;
	public int unchanged;
};

public class ConCmpt extends Configured implements Tool 
{
    public static int MAX_ITERATIONS = 2048;
	public static int changed_nodes[] = new int[MAX_ITERATIONS];
	public static int unchanged_nodes[] = new int[MAX_ITERATIONS];
	static int iter_counter = 0;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: join matrix elements and vector elements using matrix.dst_id and vector.row_id
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase	implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		private final IntWritable from_node_int = new IntWritable();
		private final IntWritable to_node_int = new IntWritable();
		int make_symmetric = 0;

		public void configure(JobConf job) {
			make_symmetric = Integer.parseInt(job.get("make_symmetric"));

			System.out.println("MapStage1 : make_symmetric = " + make_symmetric);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in the edge file
				return;

			final String[] line = line_text.split("\t");
			if( line.length < 2 )
				return;

			if( line[1].startsWith("m") ) {	// input sample: 11	msu5
				from_node_int.set(Integer.parseInt(line[0]));
				output.collect(from_node_int, new Text(line[1]));
			} else {												// (src, dst) edge
				to_node_int.set(Integer.parseInt(line[1]));

				output.collect(to_node_int, new Text(line[0]));		// invert dst and src

				if( make_symmetric == 1 ) {							// make inverse egges
					from_node_int.set(Integer.parseInt(line[0]));

					if( to_node_int.get() != from_node_int.get() )
						output.collect(from_node_int, new Text(line[1]));
				}
			}
		}
	}

    public static class	RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int number_nodes = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));

			System.out.println("RedStage1 : configure is called. number_nodes = " + number_nodes );
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			String component_id_str = "";
			Set<Integer> from_nodes_set = new HashSet<Integer>();
			boolean self_contained = false;
			String line="";

			while (values.hasNext()) {
				Text from_cur_node = values.next();
				line = from_cur_node.toString();

				if (line.startsWith("m")) {			// component info
					if( component_id_str.length() == 0 )
						component_id_str = line.substring(3);
				} else {						// edge line
					int from_node_int = Integer.parseInt(line);
					from_nodes_set.add( from_node_int );
					if( key.get() == from_node_int)
						self_contained = true;
				}
			}

			if( self_contained == false )		// add self loop, if not exists.
				from_nodes_set.add(key.get());

			Iterator from_nodes_it = from_nodes_set.iterator();
			while (from_nodes_it.hasNext()) {
				String component_info;
				int cur_key_int = ((Integer)from_nodes_it.next()).intValue();

				if( cur_key_int == key.get() ) {
					component_info = "msi" + component_id_str;
					output.collect(new IntWritable(cur_key_int), new Text(component_info));
				} else {
					component_info = "moi" + component_id_str;
					output.collect(new IntWritable(cur_key_int), new Text(component_info));
				}
			}
		}
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2: merge partial component ids.
	//  - Input: partial component ids
	//  - Output: combined component ids
    ////////////////////////////////////////////////////////////////////////////////////////////////
	public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		// Identity mapper
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");

			output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]) );
		}
    }

    public static class RedStage2 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
			String out_val ="ms";
			boolean bSelfChanged = false;
			char changed_prefix = 'x';
			String complete_cistring = "";
			int cur_min_nodeid = -1;
			int self_min_nodeid = -1;

			while (values.hasNext()) {
				String cur_ci_string = values.next().toString();
				int cur_nodeid = -1;
				try
				{
					cur_nodeid = Integer.parseInt( cur_ci_string.substring(3) );
				}
				catch (Exception ex)
				{
					System.out.println("Exception! cur_ci_string=[" + cur_ci_string + "]");
				}

				if( cur_ci_string.charAt(1) == 's' ) {	// for calculating individual diameter
					self_min_nodeid = cur_nodeid;
				}

				if( cur_min_nodeid == -1 ) {
					cur_min_nodeid = cur_nodeid;
				} else {
					if( cur_nodeid < cur_min_nodeid )
						cur_min_nodeid = cur_nodeid;
				}
			}

			if( self_min_nodeid == cur_min_nodeid ) {
				changed_prefix = 'f';	// unchanged
			} else
				changed_prefix = 'i';	// changed

			out_val = out_val + changed_prefix + Integer.toString(cur_min_nodeid);

			output.collect(key, new Text( out_val ) );
		}
    }


    public static class CombinerStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			String out_val ="moi";
			int cur_min_nodeid = -1;

			while (values.hasNext()) {
				Text cur_value_text = values.next();
				String cur_ci_string = cur_value_text.toString();
				int cur_nodeid = -1;
				try
				{
					cur_nodeid = Integer.parseInt( cur_ci_string.substring(3) );
				}
				catch (Exception ex)
				{
					System.out.println("Exception! cur_ci_string=[" + cur_ci_string + "]");
				}

				if( cur_ci_string.charAt(1) == 's' ) {			// for calculating individual diameter
					output.collect(key, new Text(cur_value_text) );
					continue;
				}

				if( cur_min_nodeid == -1 ) {
					cur_min_nodeid = cur_nodeid;
				} else {
					if( cur_nodeid < cur_min_nodeid )
						cur_min_nodeid = cur_nodeid;
				}
			}

			if( cur_min_nodeid != -1 ) {
				out_val += Integer.toString(cur_min_nodeid);
	
				output.collect(key, new Text( out_val ) );
			}
		}
    }


    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate number of nodes whose component id changed/unchanged.
	//  - Input: current component ids
	//  - Output: number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
		// output : f n		( n : # of node whose component didn't change)
		//          i m		( m : # of node whose component changed)
		public void map (final LongWritable key, final Text value, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException
		{
			if (value.toString().startsWith("#"))
				return;

			final String[] line = value.toString().split("\t");
			char change_prefix = line[1].charAt(2);

			output.collect(new Text(Character.toString(change_prefix)), new Text(Integer.toString(1)) );
		}
    }

    public static class	RedStage3 extends MapReduceBase	implements Reducer<Text, Text, Text, Text>
    {
		public void reduce (final Text key, final Iterator<Text> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException
		{
			int sum = 0;

			while (values.hasNext()) {
				final String line = values.next().toString();
				int cur_value = Integer.parseInt(line);

				sum += cur_value;
			}

			output.collect(key, new Text(Integer.toString(sum)) );
		}
    }


    //////////////////////////////////////////////////////////////////////
    // STAGE 4 : Summarize connected component information
	//    input : comcmpt_curbm
	//    output : comcmpt_summaryout
	//             min_node_id, number_of_nodes_in_the_component
    //////////////////////////////////////////////////////////////////////
	public static class MapStage4 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			final String[] line = line_text.split("\t");

			output.collect( new IntWritable(Integer.parseInt(line[1].substring(3))), new IntWritable(1) );
		}
	}

    public static class	RedStage4 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
		public void reduce (final IntWritable key, final Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
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
    protected Path curbm_path = null;
    protected Path tempbm_path = null;
    protected Path nextbm_path = null;
	protected Path output_path = null;
	protected Path summaryout_path = null;
	protected String local_output_path;
	protected int number_nodes = 0;
	protected int nreducers = 1;
	protected int cur_iter = 1;
	protected int start_from_newbm = 0;
	protected int make_symmetric = 0;		// convert directed graph to undirected graph

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new ConCmpt(), args);

		System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("concmpt <edge_path> <curbm_path> <tempbm_path> <nextbm_path> <output_path> <# of nodes> <# of tasks> <new or contNN> <makesym or nosym>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 9 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		curbm_path = new Path(args[1]);
		tempbm_path = new Path(args[2]);
		nextbm_path = new Path(args[3]);
		output_path = new Path(args[4]);
		summaryout_path = new Path("concmpt_summaryout");
		number_nodes = Integer.parseInt(args[5]);
		nreducers = Integer.parseInt(args[6]);

		if( args[7].compareTo("new") == 0 )
			start_from_newbm = 1;
		else {	// args[7] == contNN        e.g.) cont10
			start_from_newbm = 0;
			cur_iter = Integer.parseInt(args[7].substring(4));
			System.out.println("Starting from cur_iter = " + cur_iter);
		}

		if( args[8].compareTo("makesym") == 0 )
			make_symmetric = 1;
		else
			make_symmetric = 0;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing connected component. Edge path = " + args[0] + ", Newbm = " + args[7] + ", Reducers = " + nreducers );

		local_output_path = args[4] + "_temp";

		if( start_from_newbm == 1 ) {
			System.out.print("Generating initial component vector for " + number_nodes + " nodes ");
			// create bitmask generate command file, and copy to curbm_path
			gen_component_vector_file(number_nodes, curbm_path);
			System.out.println(" done");
		} else {
			System.out.println("Resuming from current component vector at radius(" + cur_iter + ")");
		}

		// Iteratively calculate neighborhood function. 
		for (int i = cur_iter; i < MAX_ITERATIONS; i++) {
			cur_iter++;

			JobClient.runJob(configStage1());
			JobClient.runJob(configStage2());
			JobClient.runJob(configStage3());

			FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));

			final FileSystem fs = FileSystem.get(getConf());

			// copy neighborhood information from HDFS to local disk, and read it!
			String new_path = local_output_path + "/" + i;
			fs.copyToLocalFile(output_path, new Path(new_path) ) ;
			ResultInfo ri = readIterationOutput(new_path);

			changed_nodes[iter_counter] = ri.changed;
			changed_nodes[iter_counter] = ri.unchanged;

			iter_counter++;

			System.out.println("Hop " + i + " : changed = " + ri.changed + ", unchanged = " + ri.unchanged);

			// Stop when the minimum neighborhood doesn't change
			if( ri.changed == 0 ) {
				System.out.println("All the component ids converged. Finishing...");
				fs.delete(curbm_path);
				fs.delete(tempbm_path);
				fs.delete(output_path);
				fs.rename(nextbm_path, curbm_path);

				break;
			}

			// rotate directory
			fs.delete(curbm_path);
			fs.delete(tempbm_path);
			fs.delete(output_path);
			fs.rename(nextbm_path, curbm_path);
		}

		FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));

		// calculate summary information using an additional stage
		System.out.println("Summarizing connected components information...");
		JobClient.runJob(configStage4());

		// finishing.
		System.out.println("\n[PEGASUS] Connected component computed.");
		System.out.println("[PEGASUS] Total Iteration = " + iter_counter);
		System.out.println("[PEGASUS] Connected component information is saved in the HDFS concmpt_curbm as\n\"node_id	'msf'component_id\" format");
		System.out.println("[PEGASUS] Connected component distribution is saved in the HDFS concmpt_summaryout as\n\"component_id	number_of_nodes\" format.\n");

		return 0;
    }

	// generate component vector creation command
	public void gen_component_vector_file(int number_nodes, Path curbm_path) throws IOException
	{
		int start_pos = 0;
		int i;
		int max_filesize = 10000000;

		for(i=0; i < number_nodes; i+=max_filesize)
		{
			int len=max_filesize;
			if(len > number_nodes-i)
				len = number_nodes - i;
			gen_one_file(number_nodes, i, len, curbm_path);
         }
	}

	// generate component vector creation command
	public void gen_one_file(int number_nodes, int start_pos,int len, Path curbm_path) throws IOException
	{
		// generate a temporary local bitmask command file
		int i, j = 0, threshold = 0, count=0;
		String file_name = "component_vector.temp."+start_pos;
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		out.write("# component vector file - hadoop\n");
		out.write("# number of nodes in graph = " + number_nodes+", start_pos="+start_pos+"\n");
		System.out.println("creating bitmask generation cmd for node " + start_pos + " ~ " + (start_pos+len));

		for(i=0; i < number_nodes; i++)
		{
			int cur_nodeid = start_pos + i;
			out.write(cur_nodeid + "\tmsi" + cur_nodeid + "\n");
			if(++j > len/10) {
					System.out.print(".");
					j = 0;
			}
			if(++count >= len)
					break;
		}
		out.close();
		System.out.println("");
		
		// copy it to curbm_path, and delete temporary local file.
		final FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile( true, new Path("./" + file_name), new Path (curbm_path.toString()+ "/" + file_name) );
	}

	// read neighborhood number after each iteration.
	public static ResultInfo readIterationOutput(String new_path) throws Exception
	{
		ResultInfo ri = new ResultInfo();
		ri.changed = ri.unchanged = 0;
		String output_path = new_path + "/part-00000";
		String file_line = "";

		try {
			BufferedReader in = new BufferedReader(	new InputStreamReader(new FileInputStream( output_path ), "UTF8"));

			// Read first line
			file_line = in.readLine();

			// Read through file one line at time. Print line # and line
			while (file_line != null){
			    final String[] line = file_line.split("\t");

				if(line[0].startsWith("i")) 
					ri.changed = Integer.parseInt( line[1] );
				else	// line[0].startsWith("u")
					ri.unchanged = Integer.parseInt( line[1] );

				file_line = in.readLine();
			}
			
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ri;//result;
	}

    // Configure stage1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("cur_iter", "" + cur_iter);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("ConCmpt_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, edge_path, curbm_path);  
		FileOutputFormat.setOutputPath(conf, tempbm_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

    // Configure stage2
    protected JobConf configStage2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("cur_iter", "" + cur_iter);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("ConCmpt_Stage2");
		
		conf.setMapperClass(MapStage2.class);        
		conf.setReducerClass(RedStage2.class);
		conf.setCombinerClass(CombinerStage2.class);

		FileInputFormat.setInputPaths(conf, tempbm_path);  
		FileOutputFormat.setOutputPath(conf, nextbm_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure stage3
    protected JobConf configStage3 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.setJobName("ConCmpt_Stage3");
		
		conf.setMapperClass(MapStage3.class);        
		conf.setReducerClass(RedStage3.class);
		conf.setCombinerClass(RedStage3.class);

		FileInputFormat.setInputPaths(conf, nextbm_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( 1 );	// This is necessary.

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

    // Configure stage4
    protected JobConf configStage4() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmpt.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("cur_iter", "" + cur_iter);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("ConCmpt_Stage4");

		conf.setMapperClass(MapStage4.class);
		conf.setReducerClass(RedStage4.class);
		conf.setCombinerClass(RedStage4.class);

		FileInputFormat.setInputPaths(conf, curbm_path);  
		FileOutputFormat.setOutputPath(conf, summaryout_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }
}

