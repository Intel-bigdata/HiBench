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
File: RWRBlock.java
 - RWR using block matrix-vector multiplication.
Version: 2.0
***********************************************************************/

package pegasus;

import pegasus.matvec.*;
import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class RWRBlock extends Configured implements Tool 
{
	protected static double converge_threshold = 0.05;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Generate partial matrix-vector multiplication results.
	//          Perform hash join using Vector.rowid == Matrix.colid.
	//  - Input: edge_file, RWR vector
	//  - Output: partial matrix-vector multiplication results.
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");

			if( line.length < 2 )
				return;

			if( line.length == 2 ) {	// vector. 
				output.collect( new IntWritable(Integer.parseInt(line[0])), new Text(line[1]) );
			} else {					// edge
				output.collect( new IntWritable(Integer.parseInt(line[1])), new Text(line[0] + "\t" + line[2]) );
			}
		}
	}


    public static class	RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		protected int block_width;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));

			System.out.println("RedStage1: block_width=" + block_width);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i;
			float vector_val = 0;

			ArrayList<Integer> to_nodes_list = new ArrayList<Integer>();
			ArrayList<Float> to_val_list = new ArrayList<Float>();

			ArrayList<VectorElem<Double>> vectorArr = null;		// save vector
			ArrayList<ArrayList<BlockElem<Double>>> blockArr = new ArrayList<ArrayList<BlockElem<Double>>>();	// save blocks
			ArrayList<Integer> blockRowArr = new ArrayList<Integer>();	// save block rows(integer)

			while (values.hasNext()) {
				// vector: key=BLOCKID, value= (IN-BLOCK-INDEX VALUE)s
				// matrix: key=BLOCK-COL	BLOCK-ROW, value=(IN-BLOCK-COL IN-BLOCK-ROW VALUE)s
				String line_text = values.next().toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	// vector : VALUE
					String vector_str = line_text;
					char fc = vector_str.charAt(0);
					if( fc == 's' || fc == 'v' )
						vector_str = vector_str.substring(1);
					vectorArr = GIMV.parseVectorVal(vector_str, Double.class);
				} else {					// edge : ROWID		VALUE
					blockArr.add( GIMV.parseBlockVal(line[1], Double.class) );	
					int block_row = Integer.parseInt(line[0]);
					blockRowArr.add( block_row );
				}
			}

			int blockCount = blockArr.size();
			if( vectorArr == null || blockCount == 0 ) // missing vector or block.
				return;

			// output 'self' block to check convergence
			Text self_output = GIMV.formatVectorElemOutput("s", vectorArr);
			output.collect(key, self_output );

			// For every matrix block, join it with vector and output partial results
			Iterator<ArrayList<BlockElem<Double>>> blockArrIter = blockArr.iterator();
			Iterator<Integer> blockRowIter = blockRowArr.iterator();
			while( blockArrIter.hasNext() ){
				ArrayList<BlockElem<Double>> cur_block = blockArrIter.next();
				int cur_block_row = blockRowIter.next();

				// multiply cur_block and vectorArr. 
				ArrayList<VectorElem<Double>> cur_mult_result = GIMV.multBlockVector( cur_block, vectorArr, block_width);
				String cur_block_output = "o";
				if( cur_mult_result != null && cur_mult_result.size() > 0 ) {
					Iterator<VectorElem<Double>> cur_mult_result_iter = cur_mult_result.iterator();

					while( cur_mult_result_iter.hasNext() ) {
						VectorElem<Double> elem = cur_mult_result_iter.next();
						if( elem.val != 0 ) {
							if( cur_block_output != "o" )
								cur_block_output += " ";
							cur_block_output += ("" + elem.row + " " + elem.val);
						}
					}
					
					// output the partial result of multiplication.
					if( cur_block_output.length() > 1 ) {
						output.collect(new IntWritable(cur_block_row), new Text(cur_block_output));
					}
				}
			}
		}

    }


    //////////////////////////////////////////////////////////////////////
    // PASS 2: merge partial multiplication results
    //////////////////////////////////////////////////////////////////////
	public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");

			IntWritable node_key = new IntWritable(Integer.parseInt(line[0]));
			output.collect(node_key, new Text(line[1]) );
		}
    }

    public static class RedStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		protected int block_width;
		double mixing_c = 0;
		int number_nodes = 1;
		int query_nodeid = -1;
		int query_blockrow = -1;
		int query_blockind = -1;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			block_width = Integer.parseInt(job.get("block_width"));
			mixing_c = Double.parseDouble(job.get("mixing_c"));
			query_nodeid = Integer.parseInt(job.get("query_nodeid"));

			query_blockrow = (int)(query_nodeid / block_width);
			query_blockind = query_nodeid % block_width;

			System.out.println("RedStage2 : block_width=" + block_width +  ", query_nodeid=" + query_nodeid);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			ArrayList<VectorElem<Double>> self_vector = null;
			int i;
			double [] out_vals = new double[block_width];

			for(i=0; i < block_width; i++)
				out_vals[i] = 0;

			while (values.hasNext()) {
				String cur_str = values.next().toString();

				if( cur_str.charAt(0) == 's' ) {
					self_vector = GIMV.parseVectorVal(cur_str.substring(1), Double.class);
					continue;
				}

				ArrayList<VectorElem<Double>> cur_vector = GIMV.parseVectorVal(cur_str.substring(1), Double.class);
				Iterator<VectorElem<Double>> vector_iter = cur_vector.iterator();

				while( vector_iter.hasNext() ) {
					VectorElem<Double> v_elem = vector_iter.next();
					out_vals[ v_elem.row ] += v_elem.val;
				}
			}

			// output updated RWR
			String out_str = "";//"v";
			for(i = 0; i < block_width; i++) {
				if( out_vals[i] > 0 ) {
					if( out_str.length() >1 )
						out_str += " ";
					out_vals[i] = out_vals[i] * mixing_c;
					
					out_str += ("" + i + " " + out_vals[i]) ;
				}
			}

			if( out_str.length() > 0 )
				output.collect( key, new Text(out_str) );

		}
    }

    //////////////////////////////////////////////////////////////////////
    // PASS 2.5: unfold the converged block RWR results to plain format.
	//         This is a map-only stage.
	//  - Input: the converged block RWR vector
	//  - Output: (node_id, "v"RWR_of_the_node)
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage25 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		int block_width;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));

			System.out.println("MapStage25: block_width = " + block_width);
		}

		// input sample :
		//0       v0 0.11537637712698735 1 0.11537637712698735
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");
			String[] tokens = null;
			
			if( line[1].charAt(0) == 's' )
				tokens = line[1].substring(1).split(" ");
			else
				tokens = line[1].split(" ");

			int i;
			int block_id = Integer.parseInt(line[0] );

			for(i = 0; i < tokens.length; i+=2) {
				int elem_row = Integer.parseInt(tokens[i]);
				double rwr_score = Double.parseDouble(tokens[i+1]);

				output.collect( new IntWritable(block_width * block_id + elem_row), new Text("v" + rwr_score) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: After finding RWR, calculate min/max RWR
	//  - Input: The converged RWR vector
	//  - Output: (key 0) minimum RWR, (key 1) maximum RWR
    //////////////////////////////////////////////////////////////////////

	// We reuse MapStage3 and RedStage3 of RWRNaive.java

    //////////////////////////////////////////////////////////////////////
    // STAGE 4 : Find distribution of RWR scores.
	//  - Input: The converged RWR vector
	//  - Output: The histogram of RWR vector in 1000 bins between min_RWR and max_RWR
    //////////////////////////////////////////////////////////////////////

	// We reuse MapStage4 and RedStage4 of RWRNaive.java

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
	protected Path query_block_path = new Path("rwr_query_norm_block");
	protected Path diff_path = new Path("rwr_vector_difference");
    protected Path vector_unfold_path = new Path("rwr_vector");
	protected Path minmax_path = new Path("rwr_minmax");
	protected Path distr_path = new Path("rwr_distr");
	protected String local_output_path;
	protected long number_nodes = 0;
	protected int niteration = 32;
	protected double mixing_c = 0.85f;
	protected long query_nodeid = -1;
	protected int nreducers = 1;
	protected int block_width = 64;
	FileSystem fs;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new RWRBlock(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("RWRBlock <edge_path> <vector_path> <query_path> <# of nodes> <# of reducers> <max iteration> <block_width> <c>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// y = y + ax
	public static Path SaxpyBlock(Configuration conf, int nreducer, Path py, Path px, Path out_path, double a, int block_width) throws Exception{
		//System.out.println("Running Saxpy: py=" + py.getName() + ", px=" + px.getName() + ", a=" +a);

		String [] args = new String[5];
		args[0] = new String("" + nreducer);
		args[1] = new String(py.getName());
		args[2] = new String(px.getName() );
		args[3] = new String("" + a);
		args[4] = new String("" + block_width);
		int saxpy_result = ToolRunner.run(conf, new SaxpyBlock(), args);

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
		vector_path = new Path(args[1]);
		tempmv_path = new Path("rwr_tempmv_block");
		mv_output_path = new Path("rwr_output_block");
		new_vector_path = new Path("rwr_vector_new");
		query_raw_path = new Path(args[2]);		
		number_nodes = Long.parseLong(args[3]);
		nreducers = Integer.parseInt(args[4]);
		niteration = Integer.parseInt(args[5]);
		block_width = Integer.parseInt(args[6]);
		mixing_c = Double.parseDouble(args[7]);

		local_output_path = "rwr_output_temp";

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing RWR using block method. Max iteration = " +niteration + ", threshold = " + converge_threshold + "\n");

		fs = FileSystem.get(getConf());

		// normalize query
		String []new_args = new String[4];
		new_args[0] = args[2];
		new_args[1] = "rwr_query_norm";
		new_args[2] = "" + nreducers;
		new_args[3] = "" + (1.0 - mixing_c);
		ToolRunner.run(getConf(), new NormalizeVector(), new_args);

		// block-encode the query
		new_args = new String[7];
		new_args[0] = "rwr_query_norm";
		new_args[1] = "rwr_query_norm_block";
		new_args[2] = "" + number_nodes;
		new_args[3] = "" + block_width;
		new_args[4] = "" + nreducers;
		new_args[5] = "null";
		new_args[6] = "nosym";
		ToolRunner.run(getConf(), new MatvecPrep(), new_args);

		// Iteratively calculate neighborhood function. 
		for (i = 0; i < niteration; i++) {
			System.out.println("\n\nITERATION " + (i+1));

			// v1 <- c*W*v
			JobClient.runJob(configStage1());
			RunningJob job = JobClient.runJob(configStage2());

			// v2 <- v1 + q
			SaxpyBlock( getConf(), nreducers, mv_output_path, query_block_path, new_vector_path, 1.0, block_width);

			// diff = || v2 - vector ||
			SaxpyBlock( getConf(), nreducers, new_vector_path, vector_path, diff_path, -1.0, block_width);

			// compute l1 norm
			new_args = new String[2];
			new_args[0] = diff_path.getName();
			new_args[1] = "" + block_width;

			ToolRunner.run(getConf(), new L1normBlock(), new_args);
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

		// unfold the block RWR to plain format
		System.out.println("Unfolding the block RWR to plain format...");
		JobClient.runJob(configStage25());


		// find min/max of RWR
		System.out.println("Finding minimum and maximum RWR scores...");
		JobClient.runJob(configStage3());

		FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));
		String new_path = local_output_path + "/" ;
		fs.copyToLocalFile(minmax_path, new Path(new_path) ) ;

		MinMaxInfo mmi = PagerankNaive.readMinMax( new_path );
		System.out.println("min = " + mmi.min + ", max = " + mmi.max );

		// find distribution of RWR scores
		JobClient.runJob(configStage4(mmi.min, mmi.max));

		System.out.println("\n[PEGASUS] RWR computed.");
		System.out.println("[PEGASUS] The final RWR scores are in the HDFS rwr_vector.");
		System.out.println("[PEGASUS] The minium and maximum RWRs are in the HDFS rwr_minmax.");
		System.out.println("[PEGASUS] The histogram of RWRs in 1000 bins between min_RWR and max_RWR are in the HDFS rwr_distr.\n");

		return 0;
    }

	// Configure pass1
    protected JobConf configStage1 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), RWRBlock.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.set("block_width", "" + block_width);
		conf.setJobName("RWRBlock_Stage1");
		
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
		final JobConf conf = new JobConf(getConf(), RWRBlock.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.set("query_nodeid", "" + query_nodeid);
		conf.set("block_width", "" + block_width);
		conf.setJobName("RWRBlock_Stage2");
		
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

    // Configure Stage25
    protected JobConf configStage25() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), ConCmptBlock.class);
		conf.set("block_width", "" + block_width);
		conf.setJobName("RWRBlock_Stage25");

		conf.setMapperClass(MapStage25.class);

		fs.delete(vector_unfold_path, true);

		FileInputFormat.setInputPaths(conf, vector_path);  
		FileOutputFormat.setOutputPath(conf, vector_unfold_path);  

		conf.setNumReduceTasks( 0 );		//This is essential for map-only tasks.

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure pass3
    protected JobConf configStage3 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), PagerankNaive.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.setJobName("RWRBlock_Stage3");
		
		conf.setMapperClass(PagerankNaive.MapStage3.class);        
		conf.setReducerClass(PagerankNaive.RedStage3.class);
		conf.setCombinerClass(PagerankNaive.RedStage3.class);

		fs.delete(minmax_path, true);

		FileInputFormat.setInputPaths(conf, vector_unfold_path);  
		FileOutputFormat.setOutputPath(conf, minmax_path);  

		conf.setNumReduceTasks( 1 );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);

		return conf;
    }

	// Configure pass4
    protected JobConf configStage4 (double min_pr, double max_pr) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), PagerankNaive.class);
		conf.set("min_pr", "" + min_pr);
		conf.set("max_pr", "" + max_pr);

		conf.setJobName("RWRBlock_Stage4" );
		
		conf.setMapperClass(PagerankNaive.MapStage4.class);        
		conf.setReducerClass(PagerankNaive.RedStage4.class);
		conf.setCombinerClass(PagerankNaive.RedStage4.class);

		fs.delete(distr_path, true);

		FileInputFormat.setInputPaths(conf, vector_unfold_path);  
		FileOutputFormat.setOutputPath(conf, distr_path);  

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }
}

