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
File: RWRNaive.java
 - RWR using plain matrix-vector multiplication.
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

public class RWRNaive extends Configured implements Tool 
{
	protected static double converge_threshold = 0.05;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Generate partial matrix-vector multiplication results.
	//          Perform hash join using Vector.rowid == Matrix.colid.
	//  - Input: edge_file, rwr vector
	//  - Output: partial matrix-vector multiplication results.
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase	implements Mapper<LongWritable, Text, IntWritable, Text>
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
			if(line.length < 2 )
				return;

			if( line[1].charAt(0) == 'v' ) {	// vector : ROWID	VALUE('vNNNN')
				output.collect( new IntWritable(Integer.parseInt(line[0])), new Text(line[1]) );
			} else {							
				// In other matrix-vector multiplication, we output (dst, src) here
				// However, In RWR, the matrix-vector computation formula is M^T * v.
				// Therefore, we output (src,dst) here.
				int src_id = Integer.parseInt(line[0]);
				int dst_id = Integer.parseInt(line[1]);
				output.collect( new IntWritable( src_id ), new Text(line[1]) );

				if( make_symmetric == 1 )
					output.collect( new IntWritable( dst_id ), new Text(line[0]) );
			}
		}
	}

    public static class RedStage1 extends MapReduceBase	implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int number_nodes = 0;
		double mixing_c = 0;
		double random_coeff = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			mixing_c = Double.parseDouble(job.get("mixing_c"));
			random_coeff = (1-mixing_c) / (double)number_nodes;

			System.out.println("RedStage1: number_nodes = " + number_nodes + ", mixing_c = " + mixing_c + ", random_coeff = " + random_coeff);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
			double cur_rank = 0;

		    ArrayList<Integer> dst_nodes_list = new ArrayList<Integer>();

			while (values.hasNext()) {
				String line_text = values.next().toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	
					if(line_text.charAt(0) == 'v')	// vector : VALUE
						cur_rank = Double.parseDouble(line_text.substring(1));
					else {							// edge : ROWID
						dst_nodes_list.add( Integer.parseInt( line[0] ) );
					}
				} 
			}

			// add random coeff
			output.collect(key, new Text( "s" + cur_rank ));

			int outdeg = dst_nodes_list.size();
			if( outdeg > 0 )
				cur_rank = cur_rank / (double)outdeg;

			for( i = 0; i < outdeg; i++) {
				output.collect( new IntWritable( dst_nodes_list.get(i) ), new Text( "v" + cur_rank ) );
			}
		}
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2: merge multiplication results.
	//  - Input: partial multiplication results
	//  - Output: combined multiplication results
    ////////////////////////////////////////////////////////////////////////////////////////////////
	public static class MapStage2 extends MapReduceBase	implements Mapper<LongWritable, Text, IntWritable, Text>
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
		int number_nodes = 0;
		double mixing_c = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			mixing_c = Double.parseDouble(job.get("mixing_c"));

			System.out.println("RedStage2: number_nodes = " + number_nodes + ", mixing_c = " + mixing_c);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
			double next_rank = 0;
			double previous_rank = 0;

			while (values.hasNext()) {
				String cur_value_str = values.next().toString();
				if( cur_value_str.charAt(0) == 's' )
					previous_rank = Double.parseDouble( cur_value_str.substring(1) );
				else
					next_rank += Double.parseDouble( cur_value_str.substring(1) ) ;
			}

			next_rank = next_rank * mixing_c;

			output.collect( key, new Text("v" + next_rank ) );

		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: After finding rwr, calculate min/max rwr
	//  - Input: The converged RWR vector
	//  - Output: (key 0) minimum RWR, (key 1) maximum RWR
    //////////////////////////////////////////////////////////////////////
	public static class MapStage3 extends MapReduceBase	implements Mapper<LongWritable, Text, IntWritable, DoubleWritable>
    {
		private final IntWritable from_node_int = new IntWritable();

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, DoubleWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			double rwr = Double.parseDouble(line[1].substring(1));
			output.collect( new IntWritable(0) , new DoubleWritable( rwr ) );
			output.collect( new IntWritable(1) , new DoubleWritable( rwr ) );
		}
	}

    public static class RedStage3 extends MapReduceBase	implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>
    {
		int number_nodes = 0;
		double mixing_c = 0;
		double random_coeff = 0;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			mixing_c = Double.parseDouble(job.get("mixing_c"));
			random_coeff = (1-mixing_c) / (double)number_nodes;

			System.out.println("RedStage2: number_nodes = " + number_nodes + ", mixing_c = " + mixing_c + ", random_coeff = " + random_coeff );
		}

		public void reduce (final IntWritable key, final Iterator<DoubleWritable> values, final OutputCollector<IntWritable, DoubleWritable> output, final Reporter reporter) throws IOException
        {
			int i;
			double min_value = 1.0;
			double max_value = 0.0;

			int min_or_max = key.get();	// 0 : min, 1: max

			while (values.hasNext()) {
				double cur_value = values.next().get();

				if( min_or_max == 0 ) {	// find min
					if( cur_value < min_value )
						min_value = cur_value;
				} else {				// find max
					if( cur_value > max_value )
						max_value = cur_value;
				}
			}

			if( min_or_max == 0)
				output.collect( key, new DoubleWritable(min_value) );
			else
				output.collect( key, new DoubleWritable(max_value) );
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 4 : Find distribution of rwrs.
	//  - Input: The converged RWR vector
	//  - Output: The histogram of RWR vector in 1000 bins between min_RWR and max_RWR
    //////////////////////////////////////////////////////////////////////
	public static class MapStage4 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
		private final IntWritable from_node_int = new IntWritable();
		double min_rwr = 0;
		double max_rwr = 0;
		double gap_rwr = 0;
		int hist_width = 1000;

		public void configure(JobConf job) {
			min_rwr = Double.parseDouble(job.get("min_rwr"));
			max_rwr = Double.parseDouble(job.get("max_rwr"));
			gap_rwr = max_rwr - min_rwr;

			System.out.println("MapStage4: min_rwr = " + min_rwr + ", max_rwr = " + max_rwr);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			double rwr = Double.parseDouble(line[1].substring(1));
			int distr_index = (int)(hist_width * (rwr - min_rwr)/gap_rwr) + 1;
			if(distr_index == hist_width + 1)
				distr_index = hist_width;
			output.collect( new IntWritable(distr_index) , new IntWritable(1) );
		}
	}

    public static class RedStage4 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
		public void reduce (final IntWritable key, final Iterator<IntWritable> values, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
        {
			int sum = 0;

			while (values.hasNext()) {
				int cur_value = values.next().get();
				sum += cur_value;
			}

			output.collect( key, new IntWritable(sum) );
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
	protected Path vector_path = null;
	protected Path new_vector_path = null;
    protected Path tempmv_path = null;
	protected Path mv_output_path = null;
	protected Path query_raw_path = null;
	protected Path query_path = new Path("rwr_query_norm");
	protected Path diff_path = new Path("rwr_vector_difference");
	protected String local_output_path;
	protected Path minmax_path = new Path("rwr_minmax");
	protected Path distr_path = new Path("rwr_distr");
	protected long number_nodes = 0;
	protected int niteration = 32;
	protected double mixing_c = 0.85f;
	protected int nreducers = 1;
	protected int make_symmetric = 0;		// convert directed graph to undirected graph
	FileSystem fs;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new RWRNaive(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("RWRNaive <edge_path> <query_path> <# of nodes> <# of reducers> <max iteration> <makesym or nosym> <new or contNN> <c>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// y = y + ax
	public static Path Saxpy(Configuration conf, int nreducer, Path py, Path px, Path out_path, double a) throws Exception{
		//System.out.println("Running Saxpy: py=" + py.getName() + ", px=" + px.getName() + ", a=" +a);

		String [] args = new String[4];
		args[0] = new String("" + nreducer);
		args[1] = new String(py.getName());
		args[2] = new String(px.getName() );
		args[3] = new String("" + a);
		int saxpy_result = ToolRunner.run(conf, new Saxpy(), args);

		//Path ret_path = null;
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out_path, true);

		if( saxpy_result == 1 )
			fs.rename(new Path("saxpy_output1"), out_path );
		else
			fs.rename(new Path("saxpy_output"), out_path );
		
		return out_path;
	}

	// y = y + ax
	public static Path SaxpyTextoutput(Configuration conf, int nreducer, Path py, Path px, Path out_path, double a) throws Exception{
		//System.out.println("Running Saxpy: py=" + py.getName() + ", px=" + px.getName() + ", a=" +a);

		String [] args = new String[4];
		args[0] = new String("" + nreducer);
		args[1] = new String(py.getName());
		args[2] = new String(px.getName() );
		args[3] = new String("" + a);
		int saxpy_result = ToolRunner.run(conf, new SaxpyTextoutput(), args);

		//Path ret_path = null;
		FileSystem fs = FileSystem.get(conf);
		fs.delete(out_path, true);

		if( saxpy_result == 1 )
			fs.rename(new Path("saxpy_output1"), out_path );
		else
			fs.rename(new Path("saxpy_output"), out_path );
		
		return out_path;
	}

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 8 ) {
			return printUsage();
		}

		int i;
		edge_path = new Path(args[0]);
		vector_path = new Path("rwr_vector");
		tempmv_path = new Path("rwr_tempmv");
		mv_output_path = new Path("rwr_mv_output");
		new_vector_path = new Path("rwr_vector_new");
		query_raw_path = new Path(args[1]);		
		number_nodes = Long.parseLong(args[2]);
		nreducers = Integer.parseInt(args[3]);
		niteration = Integer.parseInt(args[4]);

		if( args[5].compareTo("makesym") == 0 )
			make_symmetric = 1;
		else
			make_symmetric = 0;

		int cur_iteration = 1; 
		if( args[6].startsWith("cont") )
			cur_iteration = Integer.parseInt(args[6].substring(4));

		mixing_c = Double.parseDouble(args[7]);

		local_output_path = "rwr_output_temp";

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing RWR. Max iteration = " +niteration + ", threshold = " + converge_threshold + ", cur_iteration=" + cur_iteration + ", |V|=" + number_nodes + "\n");

		fs = FileSystem.get(getConf());

		if( cur_iteration == 1 )
			gen_initial_vector(number_nodes, vector_path);

		// normalize query
		String []new_args = new String[4];
		new_args[0] = args[1];
		new_args[1] = "rwr_query_norm";
		new_args[2] = "" + nreducers;
		new_args[3] = "" + (1.0 - mixing_c);
		ToolRunner.run(getConf(), new NormalizeVector(), new_args);

		// Iterate until converges. 
		for (i = cur_iteration; i <= niteration; i++) {

			System.out.println("\n\nITERATION " + (i));

			// v1 <- c*W*v
			JobClient.runJob(configStage1());
			RunningJob job = JobClient.runJob(configStage2());

			// v2 <- v1 + q
			SaxpyTextoutput( getConf(), nreducers, mv_output_path, query_path, new_vector_path, 1.0);

			// diff = || v2 - vector ||
			Saxpy( getConf(), nreducers, new_vector_path, vector_path, diff_path, -1.0);

			// compute l1 norm
			new_args = new String[1];
			new_args[0] = diff_path.getName();

			ToolRunner.run(getConf(), new L1norm(), new_args);
			double difference = PegasusUtils.read_l1norm_result(getConf());
			FileSystem lfs = FileSystem.getLocal(getConf());
			lfs.delete(new Path("l1norm"), true);

			System.out.println("difference = " + difference );

			if( difference < converge_threshold ) {
				System.out.println("RWR vector converged. Now preparing to finish...");
				fs.delete(vector_path);
				fs.delete(tempmv_path);
				fs.rename(new_vector_path, vector_path);
				break;
			}

			// rotate directory
			fs.delete(vector_path);
			fs.delete(tempmv_path);
			fs.rename(new_vector_path, vector_path);
		}

		if( i == niteration ) {
			System.out.println("Reached the max iteration. Now preparing to finish...");
		}

		// find min/max of rwrs
		System.out.println("Finding minimum and maximum rwrs...");
		JobClient.runJob(configStage3());

		FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));
		String new_path = local_output_path + "/" ;
		fs.copyToLocalFile(minmax_path, new Path(new_path) ) ;

		MinMaxInfo mmi = readMinMax( new_path );
		System.out.println("min = " + mmi.min + ", max = " + mmi.max );

		// find distribution of rwr score
		JobClient.runJob(configStage4(mmi.min, mmi.max));

		System.out.println("\n[PEGASUS] RWR computed.");
		System.out.println("[PEGASUS] The final RWR scores are in the HDFS rwr_vector.");
		System.out.println("[PEGASUS] The minium and maximum scores are in the HDFS rwr_minmax.");
		System.out.println("[PEGASUS] The histogram of scores in 1000 bins are in the HDFS rwr_distr.\n");

		return 0;
    }

	// generate initial rwr vector
	public void gen_initial_vector(long number_nodes, Path vector_path) throws IOException
	{
		int i, j = 0;
		int milestone = (int)number_nodes/10;
		String file_name = "rwr_init_vector.temp";
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		System.out.print("Creating initial rwr vectors...");
		double initial_rank = 1.0 / (double)number_nodes;

		for(i=0; i < number_nodes; i++)
		{
			out.write(i + "\tv" + initial_rank +"\n");
			if(++j > milestone) {
					System.out.print(".");
					j = 0;
			}
		}
		out.close();
		System.out.println("");
		
		// copy it to curbm_path, and delete temporary local file.
		fs.delete(vector_path, true);
		fs.copyFromLocalFile( true, new Path("./" + file_name), new Path (vector_path.toString()+ "/" + file_name) );
	}

	// read neighborhood number after each iteration.
	public static MinMaxInfo readMinMax(String new_path) throws Exception
	{
		MinMaxInfo info = new MinMaxInfo();
		String mv_output_path = new_path + "/part-00000";
		String file_line = "";

		try {
			BufferedReader in = new BufferedReader(	new InputStreamReader(new FileInputStream( mv_output_path ), "UTF8"));

			// Read first line
			file_line = in.readLine();

			// Read through file one line at time. Print line # and line
			while (file_line != null){
			    final String[] line = file_line.split("\t");

				if(line[0].startsWith("0")) 
					info.min = Double.parseDouble( line[1] );
				else
					info.max = Double.parseDouble( line[1] );

				file_line = in.readLine();
			}
			
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return info;//result;
	}

	// Configure pass1
    protected JobConf configStage1 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), RWRNaive.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("RWR_Stage1");
		
		conf.setMapperClass(MapStage1.class);        
		conf.setReducerClass(RedStage1.class);

		fs.delete(tempmv_path, true);

		FileInputFormat.setInputPaths(conf, edge_path, vector_path);  
		FileOutputFormat.setOutputPath(conf, tempmv_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure pass2
    protected JobConf configStage2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), RWRNaive.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.setJobName("RWR_Stage2");
		
		conf.setMapperClass(MapStage2.class);        
		conf.setReducerClass(RedStage2.class);

		fs.delete(mv_output_path, true);

		FileInputFormat.setInputPaths(conf, tempmv_path);  
		FileOutputFormat.setOutputPath(conf, mv_output_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure pass3
    protected JobConf configStage3 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), RWRNaive.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.setJobName("RWR_Stage3" );
		
		conf.setMapperClass(MapStage3.class);        
		conf.setReducerClass(RedStage3.class);
		conf.setCombinerClass(RedStage3.class);

		fs.delete(minmax_path, true);

		FileInputFormat.setInputPaths(conf, vector_path);  
		FileOutputFormat.setOutputPath(conf, minmax_path);  

		conf.setNumReduceTasks( 1 );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		return conf;
    }

	// Configure pass4
    protected JobConf configStage4 (double min_rwr, double max_rwr) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), RWRNaive.class);
		conf.set("min_rwr", "" + min_rwr);
		conf.set("max_rwr", "" + max_rwr);

		conf.setJobName("RWR_Stage4");
		
		conf.setMapperClass(MapStage4.class);        
		conf.setReducerClass(RedStage4.class);
		conf.setCombinerClass(RedStage4.class);

		fs.delete(distr_path, true);

		FileInputFormat.setInputPaths(conf, vector_path);  
		FileOutputFormat.setOutputPath(conf, distr_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }

}

