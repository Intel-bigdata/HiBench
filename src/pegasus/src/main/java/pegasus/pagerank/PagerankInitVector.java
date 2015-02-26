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
File: PageRankInitVector.java
 - Generate the initial vector for the PageRank.
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


public class PagerankInitVector extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: make initial pagerank vector
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");

			output.collect( new IntWritable(Integer.parseInt(line[0])), new Text(line[1] + "\t" + line[2]) );
		}
	}

    public static class RedStage1 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int number_nodes = 1;
		double initial_weight = 0.0f;
		String str_weight;
		private final IntWritable from_node_int = new IntWritable();

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			initial_weight = (double)1.0 / (double)number_nodes;
			str_weight = new String("" + initial_weight );
			System.out.println("MapStage1: number_nodes = " + number_nodes);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;

			while (values.hasNext()) {
				String cur_value_str = values.next().toString();
				final String[] line = cur_value_str.split("\t");

				int start_node = Integer.parseInt(line[0]);
				int end_node = Integer.parseInt(line[1]);

				for(i = start_node; i <= end_node; i++) {
					from_node_int.set( i );
					output.collect(from_node_int, new Text("v" + initial_weight));
				}
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
	protected Path output_path = null;
	protected Path initial_prinput_path = new Path("pr_input");
	protected int number_nodes = 0;
	protected int nreducers = 1;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new PagerankInitVector(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("PagerankInitVector <output_path> <# of nodes> <# of reducers>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 3 ) {
			System.out.println("args.length = " + args.length);
			int i;
			for(i=0; i < args.length; i++) {
				System.out.println("args[" + i + "] = " + args[i] );
			}
			return printUsage();
		}

		output_path = new Path(args[0]);				
		number_nodes = Integer.parseInt(args[1]);
		nreducers = Integer.parseInt(args[2]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Generating initial PageRank vector for " + number_nodes + " nodes.\n");

		// create PageRank generation command file, and copy to curpr_path
		gen_initial_pagerank_file(number_nodes, nreducers, initial_prinput_path);

		JobClient.runJob(configStage1());

		System.out.println("\n[PEGASUS] Initial vector for PageRank generated in HDFS " + args[0] + "\n");

		return 0;
    }

	// create PageRank init vector generation command
	public void gen_initial_pagerank_file(int number_nodes, int nmachines, Path initial_input_path) throws IOException
	{
		int gap = number_nodes / nmachines;
		int i;
		int start_node, end_node;
		String file_name = "pagerank.initial_rank.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		out.write("# number of nodes in graph = " + number_nodes+"\n");
		System.out.println("creating initial pagerank (total nodes = " + number_nodes + ")");

		for(i=0; i < nmachines; i++)
		{
			start_node = i * gap;
			if( i < nmachines - 1 )
				end_node = (i+1)*gap - 1;
			else
				end_node = number_nodes - 1;

			out.write("" + i + "\t" + start_node + "\t" + end_node + "\n" );
        }
		out.close();

		// copy it to initial_input_path, and delete the temporary local file.
		final FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile( true, new Path("./" + file_name), new Path (initial_input_path.toString()+ "/" + file_name) );
	}

    // Configure pass1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), PagerankInitVector.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.setJobName("PagerankInitVector_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, initial_prinput_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

