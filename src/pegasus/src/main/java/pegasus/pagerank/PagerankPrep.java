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
File: PageRankPrep.java
 - Convert the original edge file into column-normalized adjacency matrix format.
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


public class PagerankPrep extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Convert the original edge file into column-normalized adjacency matrix format.
	//  - Input: edge file
	//  - Output: column-normalized adjacency matrix
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		int make_symmetric = 0;

		public void configure(JobConf job) {
			make_symmetric = Integer.parseInt(job.get("make_symmetric"));

			System.out.println("MapStage1 : make_symmetric = " + make_symmetric);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 2 )						// ignore ill-formated data.
				return;

			int src_id = Integer.parseInt(line[0]);
			int dst_id = Integer.parseInt(line[1]);
			output.collect( new IntWritable( src_id ), new Text(line[1]) );

			if( make_symmetric == 1 )
				output.collect( new IntWritable( dst_id ), new Text(line[0]) );
		}
	}

    public static class RedStage1 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
		    ArrayList<Integer> dst_nodes_list = new ArrayList<Integer>();

			while (values.hasNext()) {
				String cur_value_str = values.next().toString();
				dst_nodes_list.add( Integer.parseInt(cur_value_str) );
			}

			int deg = dst_nodes_list.size();
			double elem_value = 1.0 / (double)deg ;
			for( i = 0; i < deg; i++) {
				output.collect( new IntWritable( dst_nodes_list.get(i) ), new Text( Integer.toString(key.get()) + "\t"+ elem_value ));
			}

		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected Path output_path = null;
	protected Path edge_path = null;
	protected int nreducers = 1;
	protected int make_symmetric = 0;		// convert directed graph to undirected graph

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new PagerankPrep(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("PagerankPrep <edge_path> <output_path> <# of reducers> <makesym or nosym>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 4 ) {
			System.out.println("args.length = " + args.length);
			int i;
			for(i=0; i < args.length; i++) {
				System.out.println("args[" + i + "] = " + args[i] );
			}
			return printUsage();
		}

		edge_path = new Path(args[0]);
		output_path = new Path(args[1]);				
		nreducers = Integer.parseInt(args[2]);
		if( args[3].compareTo("makesym") == 0 )
			make_symmetric = 1;
		else
			make_symmetric = 0;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Converting the adjacency matrix to column-normalized format.\n");

		JobClient.runJob(configStage1());

		System.out.println("\n[PEGASUS] Conversion finished.");
		System.out.println("[PEGASUS] Column normalized adjacency matrix is saved in the HDFS " + args[1] + "\n");

		return 0;
    }

    // Configure pass1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), PagerankPrep.class);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("PagerankPrep_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, edge_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

