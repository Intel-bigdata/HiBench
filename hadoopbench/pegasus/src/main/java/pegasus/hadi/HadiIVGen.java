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
File: HadiIVGen.java
 - generate initial bitstrings for HADI-BLOCK
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

public class HadiIVGen extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Read bitstring generation command, and generate bitstrings.
	//  - Input: bitstring generation command
	//  - Output: nodeid   TAB   FM bitstring
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 3)
				return;

			output.collect( new IntWritable(Integer.parseInt(line[0])), new Text(line[1] + "\t" + line[2]) );
		}
	}

    public static class	RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int number_nodes = 0;
		int nreplication = 0;
		int encode_bitmask = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));

			System.out.println("RedStage1 : number_nodes = " + number_nodes + ", nreplication = " + nreplication + ", encode_bitmask = " + encode_bitmask);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int start_node, end_node;

			while (values.hasNext()) {
				Text cur_text = values.next();
				final String[] line = cur_text.toString().split("\t");

				start_node = Integer.parseInt(line[0]);
				end_node = Integer.parseInt(line[1]);

				for(int i = start_node; i <= end_node; i++) {
					String new_bitmask = generate_bitmask(number_nodes, nreplication);
					output.collect( new IntWritable(i), new Text(new_bitmask) );
				}
			}
		}

		// generate K replicated bitmasks for one node
		public String generate_bitmask(int number_node, int K) 
		{
			int i;
			int size_bitmask = (int) Math.ceil( Math.log(number_node) / Math.log(2) );
			String bitmask = "vi0:0:1";	// v is the initial prefix for every vector. i means 'incomplete'
			int bm_array[] = new int[K];

			for(i=0; i<K; i++) {
				if( encode_bitmask == 1 )
					bm_array[i]= FMBitmask.create_random_bm( number_node, size_bitmask );
				else
					bitmask = bitmask + "~" + Integer.toHexString(FMBitmask.create_random_bm(number_node, size_bitmask));
			}
	
			if( encode_bitmask == 1 ) {
				String encoded_bitmask = BitShuffleCoder.encode_bitmasks(bm_array, K);

				bitmask += ("~" + encoded_bitmask);
			}

			return bitmask;
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path input_path = null;
	protected Path output_path = null;
	protected int number_nodes = 0;
	protected int number_reducers = 1;
	protected int encode_bitmask = 0;
	protected int nreplication = 32;
	FileSystem fs ;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new HadiIVGen(), args);

		System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("HadiIVGen <output_path> <# of nodes> <# of reducers> <nreplication> <enc or noenc>");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 5 ) {
			return printUsage();
		}

		output_path = new Path(args[0]);
		String input_path_name = "hadi_ivcmd" + args[0].substring(args[0].length()-1);
		input_path = new Path(input_path_name);

		number_nodes = Integer.parseInt(args[1]);
		number_reducers = Integer.parseInt(args[2]);
		nreplication = Integer.parseInt(args[3]);
		if( args[4].compareTo("enc") == 0 )
			encode_bitmask = 1;
		else
			encode_bitmask = 0;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Generating initial bistring vector. Output path = " + args[0] + ", number of nodes = " + number_nodes + ", number of reducers =" + number_reducers + ", nreplication=" + nreplication + ", encode_bitmask = " + encode_bitmask + "\n");

		// Generate command file and copy to HDFS "input_ConCmptIVGen"
		gen_cmd_file(number_nodes, number_reducers, nreplication, input_path);

		// run job
		JobClient.runJob(configStage1());

		fs = FileSystem.get(getConf());
		fs.delete(input_path);

		System.out.println("\n[PEGASUS] Initial bistring vector for HADI generated in HDFS " + args[0] + "\n");

		return 0;
    }

	// generate bitmask command file which is used in the 1st iteration.
	public void gen_cmd_file(int num_nodes, int num_reducers, int nreplication, Path input_path) throws IOException
	{
		// generate a temporary local command file
		int i;
		String file_name = "hadi_iv.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		out.write("# component vector file from HadiIVGen\n");
		out.write("# number of nodes in graph = " + number_nodes + "\n");
		System.out.print("creating initial vector generation cmd...");

		int step = num_nodes/num_reducers;
		int start_node, end_node;

		for(i=0; i < num_reducers; i++)
		{
			start_node = i * step;
			if( i < num_reducers-1)
				end_node = step*(i+1) - 1;
			else
				end_node = num_nodes - 1;

			out.write(i + "\t" + start_node + "\t" + end_node + "\n");
		}
		out.close();
		System.out.println("done.");
		
		// copy it to curbm_path, and delete temporary local file.
		final FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile( true, new Path("./" + file_name), new Path (input_path.toString()+ "/" + file_name) );
	}

    // Configure pass1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiIVGen.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.setJobName("HadiIVGen_pass1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, input_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( number_reducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }
}

