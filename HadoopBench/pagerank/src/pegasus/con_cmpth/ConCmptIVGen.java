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
File: ConCmptIVGen.java
 - generate initial vectors for HCC
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

public class ConCmptIVGen extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Read init vector generation command, and generate the init vector.
	//  - Input: init vector generation command
	//  - Output: nodeid   TAB   initial_component_vector
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase	implements Mapper<LongWritable, Text, IntWritable, Text>
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

    public static class	RedStage1 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int number_nodes = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));

			System.out.println("RedStage1: number_nodes = " + number_nodes );
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
					output.collect( new IntWritable(i), new Text("v" + i) );
				}
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path input_path = null;
	protected Path output_path = null;
	protected int number_nodes = 0;
	protected int number_reducers = 1;
	FileSystem fs ;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new ConCmptIVGen(), args);

		System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("ConCmptIVGen <output_path> <# of nodes> <# of machines>");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 3 ) {
			return printUsage();
		}

		input_path = new Path("cc_ivcmd");
		output_path = new Path(args[0]);
		number_nodes = Integer.parseInt(args[1]);
		number_reducers = Integer.parseInt(args[2]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Generating initial vector. Output path = " + args[0] + ", Number of nodes = " + number_nodes + ", Number of machines =" + number_reducers + "\n");

		// Generate command file and copy to HDFS "input_ConCmptIVGen"
		gen_cmd_file(number_nodes, number_reducers, input_path);

		// run job
		JobClient.runJob(configStage1());

		fs = FileSystem.get(getConf());
		fs.delete(input_path);

		System.out.println("\n[PEGASUS] Initial connected component vector generated in HDFS " + args[0] + "\n");

		return 0;
    }

	// generate bitmask command file which is used in the 1st iteration.
	public void gen_cmd_file(int num_nodes, int num_reducers, Path input_path) throws IOException
	{
		// generate a temporary local command file
		int i;
		String file_name = "component_iv.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		out.write("# component vector file from ConCmptIVGen\n");
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
		
		// copy it to curbm_path, and delete the temporary local file.
		final FileSystem fs = FileSystem.get(getConf());
		fs.copyFromLocalFile( true, new Path("./" + file_name), new Path (input_path.toString()+ "/" + file_name) );
	}


    // Configure pass1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmptIVGen.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.setJobName("ConCmptIVGen_Stage1");

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

