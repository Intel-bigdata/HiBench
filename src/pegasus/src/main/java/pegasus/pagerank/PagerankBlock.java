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
File: PageRankBlock.java
 - PageRank using block matrix-vector multiplication.
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

public class PagerankBlock extends Configured implements Tool 
{
    protected static enum PrCounters { CONVERGE_CHECK }
	protected static double converge_threshold = 0.000001;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: Generate partial matrix-vector multiplication results.
	//          Perform hash join using Vector.rowid == Matrix.colid.
	//  - Input: edge_file, pagerank vector
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
					vectorArr = GIMV.parseVectorVal(line_text.substring(1), Double.class);
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
						VectorElem elem = cur_mult_result_iter.next();
						if( cur_block_output != "o" )
							cur_block_output += " ";
						cur_block_output += ("" + elem.row + " " + elem.val);
					}
					
					// output the partial result of multiplication.
					output.collect(new IntWritable(cur_block_row), new Text(cur_block_output));
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
		double random_coeff = 0;
		double converge_threshold = 0;
		int number_nodes = 1;

		public void configure(JobConf job) {
			number_nodes = Integer.parseInt(job.get("number_nodes"));
			block_width = Integer.parseInt(job.get("block_width"));
			mixing_c = Double.parseDouble(job.get("mixing_c"));
			random_coeff = (1-mixing_c) / (double)number_nodes;
			converge_threshold = Double.parseDouble(job.get("converge_threshold"));
			System.out.println("RedStage2 : block_width=" + block_width + ", converge_threshold="+converge_threshold);
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

			// output updated PageRank
			String out_str = "v";
			for(i = 0; i < block_width; i++) {
				if( out_str.length() >1 )
					out_str += " ";
				out_vals[i] = out_vals[i] * mixing_c + random_coeff;
				out_str += ("" + i + " " + out_vals[i]) ;
			}

			output.collect( key, new Text(out_str) );

			// compare the previous and the current PageRank
			Iterator<VectorElem<Double>> sv_iter = self_vector.iterator();

			while( sv_iter.hasNext() ) {
				VectorElem<Double> cur_ve = sv_iter.next();
			
				double diff = Math.abs(cur_ve.val - out_vals[cur_ve.row]);

				if( diff > converge_threshold ) {
					reporter.incrCounter(PrCounters.CONVERGE_CHECK, 1);
					break;
				}
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // PASS 2.5: unfold the converged block PageRank results to plain format.
	//         This is a map-only stage.
	//  - Input: the converged block PageRank vector
	//  - Output: (node_id, "v"PageRank_of_the_node)
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
			final String[] tokens = line[1].substring(1).split(" ");
			int i;
			int block_id = Integer.parseInt(line[0] );

			for(i = 0; i < tokens.length; i+=2) {
				int elem_row = Integer.parseInt(tokens[i]);
				double pagerank = Double.parseDouble(tokens[i+1]);

				output.collect( new IntWritable(block_width * block_id + elem_row), new Text("v" + pagerank) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: After finding pagerank, calculate min/max pagerank
	//  - Input: The converged PageRank vector
	//  - Output: (key 0) minimum PageRank, (key 1) maximum PageRank
    //////////////////////////////////////////////////////////////////////

	// We reuse MapStage3 and RedStage3 of PageRankNaive.java

    //////////////////////////////////////////////////////////////////////
    // STAGE 4 : Find distribution of pageranks.
	//  - Input: The converged PageRank vector
	//  - Output: The histogram of PageRank vector in 1000 bins between min_PageRank and max_PageRank
    //////////////////////////////////////////////////////////////////////

	// We reuse MapStage4 and RedStage4 of PageRankNaive.java

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
    protected Path vector_path = null;
    protected Path tempmv_path = null;
	protected Path output_path = null;
    protected Path vector_unfold_path = null;
	protected Path minmax_path = null;
	protected Path distr_path = null;
	protected String local_output_path;
	protected int number_nodes = 0;
	protected int niteration = 32;
	protected double mixing_c = 0.85f;
	protected int nreducers = 1;
	protected int make_symmetric = 0;		// convert directed graph to undirected graph
	protected int block_width = 64;
	FileSystem fs ;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new PagerankBlock(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("PagerankBlock <edge_path> <vector_path> <tempmv_path> <output_path> <# of nodes> <# of reducers> <number of iteration> <block width>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 5 ) {
			return printUsage();
		}
		int i;

		edge_path = new Path(args[0]+"/pr_edge_block");
		vector_path = new Path(args[0]+"/pr_iv_block");
		tempmv_path = new Path(args[0]+"/pr_tempmv_block");
		output_path = new Path(args[0]+"/pr_output_block");
		vector_unfold_path = new Path(args[0]+"/pr_vector");
		minmax_path = new Path(args[0]+"/pr_minmax");
		distr_path = new Path(args[0]+"/pr_distr");
		number_nodes = Integer.parseInt(args[1]);
		nreducers = Integer.parseInt(args[2]);
		niteration = Integer.parseInt(args[3]);
		block_width = Integer.parseInt(args[4]);

		local_output_path = args[0]+"/pr_tempmv_block_temp";

		converge_threshold = ((double)1.0/(double) number_nodes)/50;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing PageRank using block method. Max iteration = " +niteration + ", threshold = " + converge_threshold + "\n");

		fs = FileSystem.get(getConf());

		// Iteratively calculate neighborhood function. 
		for (i = 0; i < niteration; i++) {
			JobClient.runJob(configStage1());
			RunningJob job = JobClient.runJob(configStage2());

			Counters c = job.getCounters();
			long changed = c.getCounter(PrCounters.CONVERGE_CHECK);
			System.out.println("Iteration = " + i + ", changed reducer = " + changed);

			if( changed == 0 ) {
				System.out.println("PageRank vector converged. Now preparing to finish...");
				fs.delete(vector_path);
				fs.delete(tempmv_path);
				fs.rename(output_path, vector_path);
				break;
			}

			// rotate directory
			fs.delete(vector_path);
			fs.delete(tempmv_path);
			fs.rename(output_path, vector_path);
		}

		if( i == niteration ) {
			System.out.println("Reached the max iteration. Now preparing to finish...");
		}

		// unfold the block PageRank to plain format
		System.out.println("Unfolding the block PageRank to plain format...");
		JobClient.runJob(configStage25());


		// find min/max of pageranks
		//System.out.println("Finding minimum and maximum pageranks...");
		//JobClient.runJob(configStage3());

		//FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));
		//String new_path = local_output_path + "/" ;
		//fs.copyToLocalFile(minmax_path, new Path(new_path) ) ;

		//MinMaxInfo mmi = PagerankNaive.readMinMax( new_path );
		//System.out.println("min = " + mmi.min + ", max = " + mmi.max );

		// find distribution of pageranks
		//JobClient.runJob(configStage4(mmi.min, mmi.max));


		System.out.println("\n[PEGASUS] PageRank computed.");
		System.out.println("[PEGASUS] The final PageRanks are in the HDFS pr_vector.");
		//System.out.println("[PEGASUS] The minium and maximum PageRanks are in the HDFS pr_minmax.");
		//System.out.println("[PEGASUS] The histogram of PageRanks in 1000 bins between min_PageRank and max_PageRank are in the HDFS pr_distr.\n");

		return 0;
    }

	// Configure pass1
    protected JobConf configStage1 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), PagerankBlock.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.set("block_width", "" + block_width);
		conf.setJobName("Pagerank_Stage1");
		
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
		final JobConf conf = new JobConf(getConf(), PagerankBlock.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("mixing_c", "" + mixing_c);
		conf.set("converge_threshold", "" + converge_threshold);
		conf.set("block_width", "" + block_width);
		conf.setJobName("Pagerank_Stage2");
		
		conf.setMapperClass(MapStage2.class);        
		conf.setReducerClass(RedStage2.class);

		fs.delete(output_path, true);

		FileInputFormat.setInputPaths(conf, tempmv_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

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
		conf.setJobName("Pagerank_Stage25");

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
		conf.set("converge_threshold", "" + converge_threshold);
		conf.setJobName("Pagerank_Stage3");
		
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

		conf.setJobName("Pagerank_Stage4");
		
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

