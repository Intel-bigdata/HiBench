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
File: HadiBlock.java
 - A main class for Hadi-block.
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

// HadiBlock Main Class
public class HadiBlock extends Configured implements Tool 
{
    public static int MAX_ITERATIONS = 2048;
	public static float N[] = new float[MAX_ITERATIONS];	// save N(h)
	static int iter_counter = 0;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: generate partial block-bitstring. 
	//          Hash-join edge and vector by Vector.BLOCKROWID == Edge.BLOCKCOLID where
	//          vector: key=BLOCKID, value= msu (IN-BLOCK-INDEX VALUE)s
	//                                      moc
	//          edge: key=BLOCK-ROW		BLOCK-COL, value=(IN-BLOCK-ROW IN-BLOCK-COL VALUE)s
	//  - Input: edge_file, bitstrings_from_the_last_iteration
	//  - Output: partial bitstrings
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in the edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 2 )
				return;

			if( line.length == 2 ) {	// vector
				IntWritable node_key = new IntWritable( Integer.parseInt(line[0]) );
				output.collect( node_key, new Text(line[1]) );
			} else {					// edge. line.length = 3.
				IntWritable node_key = new IntWritable( Integer.parseInt(line[1]) );
				output.collect( node_key, new Text(line[0] + "\t" + line[2]) );
			}
		}
	}

    public static class	RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int nreplication = 0;
		int encode_bitmask = 0;
		int block_width = 0;

		public void configure(JobConf job) {
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));
			block_width = Integer.parseInt(job.get("block_width"));

			System.out.println("RedStage1: nreplication = " + nreplication + ", encode_bitmask="+encode_bitmask+", block_width=" + block_width);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			ArrayList<VectorElem<String>> vectorArr = null;	// save vector
			ArrayList<ArrayList<BlockElem<Integer>>> blockArr = new ArrayList<ArrayList<BlockElem<Integer>>>();	// save blocks
			ArrayList<Integer> blockRowArr = new ArrayList<Integer>();	// save block rows(integer)

			while (values.hasNext()) {
				// vector: key=BLOCKID, value= (IN-BLOCK-INDEX VALUE)s
				// edge: key=BLOCK-COLID	BLOCK-ROWID, value=(IN-BLOCK-COL IN-BLOCK-ROW VALUE)s
				String line_text = values.next().toString();
				final String[] line = line_text.split("\t");

				if( line.length == 1 ) {	// vector : VALUE
					vectorArr = GIMV.parseHADIVector(line_text);
				} else {					// edge : BLOCK-ROWID		VALUE
					blockArr.add( GIMV.parseBlockVal(line[1], Integer.class) );
					int block_row = Integer.parseInt(line[0]);
					blockRowArr.add( block_row );
				}
			}

			if( vectorArr == null)
				return;

			// output 'self' block to check convergence
			Text self_output = GIMV.formatHADIVectorElemOutput("s", vectorArr);
			output.collect(key, self_output);

			// For every matrix block, join it with vector and output partial results
			Iterator<ArrayList<BlockElem<Integer>>> blockArrIter = blockArr.iterator();
			Iterator<Integer> blockRowIter = blockRowArr.iterator();
			int block_col_id = key.get();
			while( blockArrIter.hasNext() ){
				ArrayList<BlockElem<Integer>> cur_block = blockArrIter.next();
				int cur_block_row = blockRowIter.next();

				ArrayList<VectorElem<String>> cur_mult_result = GIMV.bworBlockVector( cur_block, vectorArr, block_width, nreplication, encode_bitmask);

				if( cur_mult_result.size() > 0 ) {
					Text partial_output = GIMV.formatVectorElemOutput("o", cur_mult_result);
					output.collect(new IntWritable(cur_block_row),  partial_output);
				}
			}
		}
    }

	////////////////////////////////////////////////////////////////////////////////////////////////
    // STAGE 2: merge partial bitstrings.
	//  - Input: partial bitstrings
	//  - Output: combined bitstrings
    ////////////////////////////////////////////////////////////////////////////////////////////////
	public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		// Identity mapper
		public void map (final LongWritable key, final Text value, final 	OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");

			output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]) );
		}
    }

    public static class RedStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int nreplication = 0;
		int encode_bitmask = 0;
		int cur_radius = 0;
		int block_width;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));
			cur_radius = Integer.parseInt(job.get("cur_radius"));

			System.out.println("RedStage2: block_width = " + block_width + ", nreplication = " + nreplication + ", encode_bitmask = "+encode_bitmask +", cur_radius = " + cur_radius);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			int i,j;
			long [][] self_bm = new long[block_width][nreplication];
			long [][] out_vals = new long[block_width][nreplication];
			char [] prefix = new char[block_width];
			String [] saved_rad_nh= new String[block_width];
			for(i=0; i < block_width; i++)
				for(j=0; j < nreplication; j++)
					out_vals[i][j] = 0;

			while (values.hasNext()) {
				String cur_str = values.next().toString();
				ArrayList<VectorElem<String>> cur_vector = GIMV.parseHADIVector(cur_str);
				Iterator<VectorElem<String>> vector_iter = cur_vector.iterator();

				j = 0;
				while( vector_iter.hasNext() ) {
					VectorElem<String> v_elem = vector_iter.next();
					out_vals[ v_elem.row ] = GIMV.updateHADIBitString( out_vals[v_elem.row], v_elem.val, nreplication, encode_bitmask );

					if( cur_str.charAt(0) == 's' ) { 
						self_bm[ v_elem.row ] = GIMV.parseHADIBitString( v_elem.val, nreplication, encode_bitmask );
						prefix[j] = v_elem.val.charAt(0);
						
						int tindex = v_elem.val.indexOf('~');
						if( tindex >= 2 )
							saved_rad_nh[j] = v_elem.val.substring(1, tindex);
						j++;
					}
				}
			}

			ArrayList<VectorElem<String>> new_vector = GIMV.makeHADIBitString( out_vals, block_width, self_bm, prefix, saved_rad_nh, nreplication, cur_radius, encode_bitmask );
			output.collect(key, GIMV.formatVectorElemOutput("s", new_vector) );
		}
    }

    public static class CombinerStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int nreplication = 0;
		int encode_bitmask = 0;
		int cur_radius = 0;
		int block_width;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));
			cur_radius = Integer.parseInt(job.get("cur_radius"));

			System.out.println("CombinerStage2: block_width = " + block_width + ", nreplication = " + nreplication + ", encode_bitmask = "+encode_bitmask +", cur_radius = " + cur_radius);

		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			int i,j;
			long [][] out_vals = new long[block_width][nreplication];
			int bs_count = 0;
			for(i=0; i < block_width; i++)
				for(j=0; j < nreplication; j++)
					out_vals[i][j] = 0;

			while (values.hasNext()) {
				String cur_str = values.next().toString();
				ArrayList<VectorElem<String>> cur_vector = GIMV.parseHADIVector(cur_str);
				if( cur_str.charAt(0) == 's' ) {
					output.collect(key, new Text(cur_str) );
					continue;
				}
				Iterator<VectorElem<String>> vector_iter = cur_vector.iterator();

				j = 0;
				while( vector_iter.hasNext() ) {
					VectorElem<String> v_elem = vector_iter.next();
					out_vals[ v_elem.row ] = GIMV.updateHADIBitString( out_vals[v_elem.row], v_elem.val, nreplication, encode_bitmask );
				}
				bs_count++;
			}

			if( bs_count > 0 ) {
				ArrayList<VectorElem<String>> new_vector = GIMV.makeHADICombinerBitString( out_vals, block_width, nreplication, cur_radius, encode_bitmask );
				output.collect(key, GIMV.formatVectorElemOutput("o", new_vector) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate N(h) and the number of changed nodes.
	//  - Input: the converged bitstrings
	//  - Output: Neighborhood(h)  TAB  number_of_converged_nodes   TAB  number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		private final IntWritable zero_id = new IntWritable(0);
		private Text output_val;

		int nreplication = 0;
		int encode_bitmask = 0;

		public void configure(JobConf job) {
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));

			System.out.println("MapStage3 : nreplication = " + nreplication + ", encode_bitmask="+encode_bitmask);
		}

		// input sample :
		// 1       s0 c4:4:7.5~e0000000~c0000000... 1 f2:2:3.7~e0000000~c0000000...
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			int i,j;
			String[] line = value.toString().split("\t");
			String[] tokens = line[1].substring(1).split(" ");
			double sum_nh = 0;
			int converged_count = 0;
			int changed_count = 0;

			for(i = 0; i < tokens.length; i += 2 ) {
				String cur_elem = tokens[i+1];
				if( cur_elem.charAt(0) == 'c' )
					converged_count++;
				if( cur_elem.charAt(0) == 'i' )
					changed_count++;

				double avg_bitpos = 0;
				if( encode_bitmask == 1 ) {
					int bitmask_start_index = cur_elem.indexOf('~');
					String bitmask_str = cur_elem.substring(bitmask_start_index+1);	
					int [] bitmask = BitShuffleCoder.decode_bitmasks( bitmask_str, nreplication );

					for(j = 0; j < nreplication; j++)
						avg_bitpos += (double) FMBitmask.find_least_zero_pos( bitmask[j] );
				} else {
					String[] bitmasks = cur_elem.split("~");
					for(j = 1; j < bitmasks.length; j++)
						avg_bitpos += (double) FMBitmask.find_least_zero_pos( Long.parseLong( bitmasks[j], 16 ) );
				}

				avg_bitpos = avg_bitpos / nreplication;
				sum_nh += Math.pow(2, avg_bitpos)/0.77351;
			}

			output_val = new Text( Double.toString(sum_nh ) + "\t" + converged_count + "\t" + changed_count);

			output.collect(zero_id, output_val);
		}
    }

    public static class	RedStage3 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		private Text output_val;

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			double nh_sum = 0.0f;				// N(h)
			int converged_sum = 0;				// number of converged nodes at this iteration
			int changed_sum = 0;				// number of changed nodes

			while (values.hasNext()) {
				final String[] line = values.next().toString().split("\t");

				nh_sum += Double.parseDouble(line[0]);
				converged_sum += Integer.parseInt(line[1]);
				changed_sum += Integer.parseInt(line[2]);
			}

			output_val = new Text( Double.toString(nh_sum) + "\t" + Integer.toString(converged_sum) + "\t" + Integer.toString(changed_sum) );
			output.collect(key, output_val);
		}
    }


    //////////////////////////////////////////////////////////////////////
    // STAGE 4: Calculate the effective radii of nodes, after the bitstrings converged.
	//         This is a map-only stage.
	//  - Input: the converged bitstrings
	//  - Output: (node_id, "bsf"max_radius:eff_radius)
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage4 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		int block_width;

		public void configure(JobConf job) {
			block_width = Integer.parseInt(job.get("block_width"));

			System.out.println("MapStage4: block_width = " + block_width);
		}

		// input sample :
		// 3       s0 i1:1:2.0:2:5.8~e0000000~e0000000... 1 i1:1:1.6:2:2.4~e0000000~c0000000...
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");
			final String[] tokens = line[1].substring(1).split(" ");
			int i, j;
			int block_id = Integer.parseInt(line[0] );

			for(i = 0; i < tokens.length; i+=2) {
				int max_radius = 0;
				double eff_radius=0;//int eff_radius = 0;
				double eff_nh = 0;

				String bit_str = tokens[i+1].substring(1) ;
				if( bit_str.length() > 0 ) {
					String[] radius_str = bit_str.split("~");
					String[] radius_info = radius_str[0].split(":");
					if( radius_info.length > 1 ) {
						max_radius = Integer.parseInt( radius_info[ radius_info.length -2] );
						eff_radius = max_radius;
						double max_nh = Double.parseDouble( radius_info[ radius_info.length -1] );
						eff_nh = max_nh;
						double ninety_th = max_nh * 0.9;
						for(j = radius_info.length -4; j >=1; j -= 2) {
							int cur_hop = Integer.parseInt( radius_info[j] );
							double cur_nh = Double.parseDouble( radius_info[j+1] );

							if( cur_nh >= ninety_th ) {
								eff_radius = cur_hop;
								eff_nh = cur_nh;
							} else {
								eff_radius = cur_hop + (double)(ninety_th - cur_nh)/(eff_nh - cur_nh);
								break;
							}
						}
					}

					int elem_row = Integer.parseInt(tokens[i]);
					DecimalFormat df = new DecimalFormat("#.##");

					output.collect( new IntWritable(block_width * block_id + elem_row), new Text("bsf" + max_radius + ":" + df.format(eff_radius)) );
				}
			}
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
	protected Path radius_path = null;
	protected Path radius_summary_path = null;
	protected String local_output_path;
	protected int number_nodes = 0;
	protected int nreplication = 0;
	protected int nreducer = 1;
	protected int encode_bitmask = 0;
	protected int cur_radius = 1;
	protected int start_from_newbm = 0;
	protected int resume_from_radius = 0;
	protected int block_width = 16;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new HadiBlock(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("hadiblock <edge_path> <curbm_path> <tempbm_path> <nextbm_path> <output_path> <# of nodes> <# of replication> <# of reducers> <enc or noenc> <newbm or contNN> <block_width>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		int i;
		int max_iteration = MAX_ITERATIONS;

		if( args.length != 12 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		curbm_path = new Path(args[1]);
		tempbm_path = new Path(args[2]);
		nextbm_path = new Path(args[3]);
		output_path = new Path(args[4]);
		number_nodes = Integer.parseInt(args[5]);
		radius_path = new Path("hadi_radius_block");
		radius_summary_path = new Path("hadi_radius_block_summary");
		nreplication = Integer.parseInt(args[6]);
		nreducer = Integer.parseInt(args[7]);

		if( args[8].compareTo("enc") == 0 )
			encode_bitmask = 1;

		if( args[9].compareTo("newbm") == 0 )
			start_from_newbm = 1;
		else {
			start_from_newbm = 0;
			cur_radius = Integer.parseInt(args[9].substring(4));
		}

		block_width = Integer.parseInt(args[10]);

		if( args[11].compareTo("max") != 0 ) 
			max_iteration = Integer.parseInt(args[11]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing Radii/Diameter using block method. Current hop: " + cur_radius + ", edge_path: " + args[0] + ", encode: " + encode_bitmask + ", # reducers: " + nreducer + ", block width: " + block_width + ", max_iteration: " + max_iteration + "\n");

		local_output_path = args[4] + number_nodes + "_tempblk";

		N[0] = number_nodes;

		// Iteratively run Stage1 to Stage3.
		for (i = cur_radius; i <= max_iteration; i++) {
			JobClient.runJob(configStage1());
			JobClient.runJob(configStage2());
			JobClient.runJob(configStage3());

			FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));

			final FileSystem fs = FileSystem.get(getConf());

			// copy neighborhood information from HDFS to local disk, and read it!
			String new_path = local_output_path + "/" + i;
			fs.copyToLocalFile(output_path, new Path(new_path) ) ;
			HadiResultInfo ri = HadiUtils.readNhoodOutput(new_path);
			N[i] = ri.nh;
			iter_counter++;

			System.out.println("Nh(" + i + "):\t" + N[i] + "\tGuessed Radius(" + (i-1) + "):\t" + ri.converged_nodes );

			// Stop when all radii converged.
			if( ri.changed_nodes == 0 ) {//if( i > 1 && N[i] == N[i-1] ) {
				System.out.println("All the bitstrings converged. Finishing...");
				fs.delete(curbm_path);
				fs.delete(tempbm_path);
				fs.rename(nextbm_path, curbm_path);
				break;
			}

			// rotate directory
			fs.delete(curbm_path);
			fs.delete(tempbm_path);
			if(i < MAX_ITERATIONS - 1 )
				fs.delete(output_path);
			fs.rename(nextbm_path, curbm_path);

			cur_radius++;
		}


		// Summarize Radius Information
		System.out.println("Calculating the effective diameter...");
		JobClient.runJob(configStage4());

		// Summarize Radius Information
		System.out.println("Summarizing radius information...");
		JobClient.runJob(configStage5());

		FileUtil.fullyDelete( FileSystem.getLocal(getConf()), new Path(local_output_path));

		// print summary information
		if( i > max_iteration ) 
			System.out.println("Reached Max Iteartion " + max_iteration);
		System.out.println("Total Iteration = " + iter_counter + ".");

		System.out.println("Neighborhood Summary:");
		for(int j = 0; j <= (i); j++)
			System.out.println("\tNh(" + (j) + "):\t" + N[j]);

		System.out.println("\n[PEGASUS] Radii and diameter computed.");
		System.out.println("[PEGASUS] Maximum diameter: " + (cur_radius - 1) );
		System.out.println("[PEGASUS] Average diameter: " + HadiUtils.average_diameter(N, cur_radius - 1) );
		System.out.println("[PEGASUS] 90% Effective diameter: " + HadiUtils.effective_diameter(N, cur_radius-1) );
		System.out.println("[PEGASUS] Radii are saved in the HDFS " + radius_path.getName() );
		System.out.println("[PEGASUS] Radii summary is saved in the HDFS " + radius_summary_path.getName()  + "\n");

		return 0;
    }

    // Configure pass1
    protected JobConf configStage1() throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiBlock.class);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.set("block_width", "" + block_width);
		conf.setJobName("HADIBlk_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, edge_path, curbm_path);  
		FileOutputFormat.setOutputPath(conf, tempbm_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

    // Configure pass2
    protected JobConf configStage2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiBlock.class);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.set("cur_radius", "" + cur_radius);
		conf.set("block_width", "" + block_width);
		conf.setJobName("HADIBlk_Stage2");

		conf.setMapperClass(MapStage2.class);        
		conf.setReducerClass(RedStage2.class);
		conf.setCombinerClass(CombinerStage2.class);

		FileInputFormat.setInputPaths(conf, tempbm_path);  
		FileOutputFormat.setOutputPath(conf, nextbm_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure Stage3
    protected JobConf configStage3 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiBlock.class);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.setJobName("HADIBlk_Stage3");
		
		conf.setMapperClass(MapStage3.class);        
		conf.setReducerClass(RedStage3.class);
		conf.setCombinerClass(RedStage3.class);

		FileInputFormat.setInputPaths(conf, nextbm_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure Stage4
    protected JobConf configStage4 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiBlock.class);
		conf.set("block_width", "" + block_width);
		conf.setJobName("HADIBlk_Stage4");
		
		conf.setMapperClass(MapStage4.class);        

		FileInputFormat.setInputPaths(conf, curbm_path);  
		FileOutputFormat.setOutputPath(conf, radius_path);  

		conf.setNumReduceTasks( 0 );		//This is essential for map-only tasks.

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

	// Configure Stage5
    protected JobConf configStage5 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), HadiBlock.class);
		conf.setJobName("HADIBlk_Stage5");
		
		// reuse maper and reducers from Hadi class.
		conf.setMapperClass(Hadi.MapStage5.class);        
		conf.setReducerClass(Hadi.RedStage5.class);
		conf.setCombinerClass(Hadi.RedStage5.class);

		FileInputFormat.setInputPaths(conf, radius_path);  
		FileOutputFormat.setOutputPath(conf, radius_summary_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }
}

