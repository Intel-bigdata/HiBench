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
File: Hadi.java
 - A main class for Hadi-plain.
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

class HadiResultInfo
{
	public float nh;
	public int converged_nodes;
	public int changed_nodes;
};


// Bit Shuffle Encoder/Decoder
class BitShuffleCoder
{
	// decode bitstrings
	public static int[] decode_bitmasks(String str, int K)
	{
		int i, j;
		int [] result = new int[K];

		int cur_value;
		int fill_value = 1;
		int cumulated_value = 0;

		for(i = 0; i < K; i++)
			result[i] = 0;

		int [] byte_buffer = new int[4];
		int byte_bufpos = 0;
		int cur_byte;
		byte [] str_bytes = str.getBytes();

		for(i = 0; i < str_bytes.length; i += 2) {
			cur_byte = Integer.parseInt(str.substring(i, i+2), 16);

			if( (cur_byte & 0x80) != 0 ) {
				byte_buffer[byte_bufpos++] = cur_byte & 0x7F;

				cur_value = 0;
				for(j = 0; j < byte_bufpos; j++) {
					cur_value += ( byte_buffer[j] << (7*(byte_bufpos - 1 - j)) );
				}

				// fill only one
				if( fill_value == 1 && cur_value > 0)
					fill_result( result, K, cur_value, cumulated_value );

				cumulated_value += cur_value;
				fill_value = 1 - fill_value;
				byte_bufpos = 0;
			} else {
				byte_buffer[byte_bufpos++] = cur_byte & 0x7F;
			} 
		}

		return result;
	}

	private static void fill_result(int[] result, int K, int cur_value, int cumulated_value) {
		int i, j;
		int start_i = cumulated_value / K ;	// i : bit position of each bitmask
		int start_j = cumulated_value % K;	// j : index of bitmask
		int count = 0;

		for(i = start_i; i < 32; i++) {
			if( i == start_i )
				j = start_j;
			else
				j = 0;

			for(; j < K; j++) {
					result[j] |= (1 << (31-i));
					if( ++count >= cur_value )
						return;
			}
		}

		return ;
	}

	// encode bitmask
	public static String encode_bitmasks( int [] bm_array, int K )
	{
		String result = "";
		int i, j;
		byte prev_bit = -1;
		int cur_count = 0;
		byte cur_bit;
		int cur_mask;

		for(i = 0; i < 32; i++) {		// i : bit position of each bitmask
			cur_mask = 1 << (31-i);
			for(j = 0; j < K; j++) {	// j : index of bitmask
				if( (cur_mask & bm_array[j]) != 0 )
					cur_bit = 1;
				else
					cur_bit = 0;
				
				if( prev_bit == -1 ) {
					if( cur_bit == 0 )
						result += encode_value(0);	// bit sequence start with 1.

					prev_bit = cur_bit;
					cur_count = 1;
					continue;
				}

				if( prev_bit == cur_bit ) {
					cur_count++;
				} else {
					result += encode_value (cur_count);

					prev_bit = cur_bit;
					cur_count = 1;
				}
			}
		}

		if( cur_count > 0 ) {
			result += encode_value(cur_count);
		}

		return result;
	}

	// encode bitmask
	public static String encode_bitmasks( long [] bm_array, int K )
	{
		String result = "";
		int i, j;
		byte prev_bit = -1;
		int cur_count = 0;
		byte cur_bit;
		long cur_mask;

		for(i = 0; i < 32; i++) {		// i : bit position of each bitmask
			cur_mask = 1 << (31-i);
			for(j = 0; j < K; j++) {	// j : index of bitmask
				if( (cur_mask & bm_array[j]) != 0 )
					cur_bit = 1;
				else
					cur_bit = 0;
				
				if( prev_bit == -1 ) {
					if( cur_bit == 0 )
						result += encode_value(0);	// bit sequence start with 1.

					prev_bit = cur_bit;
					cur_count = 1;
					continue;
				}

				if( prev_bit == cur_bit ) {
					cur_count++;
				} else { // prev_bit != cur_bit
					result += encode_value (cur_count);

					prev_bit = cur_bit;
					cur_count = 1;
				}
			}
		}

		if( cur_count > 0 ) {
			result += encode_value(cur_count);
		}

		return result;
	}


	private static String encode_value(int number)
	{
		if( number == 0 ) {
			return "80";
		}

		// find leftmost bit
		int i;
		int cur_mask;
		int result = 0;
		final int [] one_masks = { 0x7F, 0x3F80, 0x1FC000, 0xFE00000, 0xF0000000 };
	
		for(i = 31; i >= 0; i--) {
			cur_mask = 1 << i;
			if( (cur_mask & number) != 0 )
				break;
		}

		int nbytes = (int) Math.ceil( (float)(i+1)/ 7.0 );

		for(i = 0; i < nbytes; i++) {
			if( i == 0 ) {
				result = (1 << 7 ) | (number & one_masks[0]);
			} else {
				int added_value = ((number & (one_masks[i]))) >> (7 * i);
				result |= (added_value << (8*i));
			}
		}

		String temp_result = Integer.toHexString(result);
		if( temp_result.length() % 2 == 1 )
			temp_result = "0" + temp_result;

		return temp_result;
	}

};


// Flajolet - Martin bitmask(bitstring) class
class FMBitmask
{
	// generate K replicated bitmasks for one node
	public static String generate_bitmask(int number_node, int K, int encode_bitmask) 
	{
		int i;
		int size_bitmask=32;
		String bitmask = "bsi0:0:1";
		int bm_array[] = new int[K];

		for(i=0; i<K; i++) {
			if( encode_bitmask == 1 )
				bm_array[i]= create_random_bm( number_node, size_bitmask );
			else
				bitmask = bitmask + " " + Integer.toHexString(create_random_bm(number_node, size_bitmask));
		}

		if( encode_bitmask == 1 ) {
			String encoded_bitmask = BitShuffleCoder.encode_bitmasks(bm_array, K);

			bitmask += (" " + encoded_bitmask);
		}

		return bitmask;
	}


	// Create a Flajolet-Martin bitstring. The maximum number of nodes is currently 4 billion.
	public static int create_random_bm(int number_node, int size_bitmask)
	{
		int j;

		// cur_random is between 0 and 1.
		double cur_random = Math.random();

		double threshold = 0;
		for(j = 0; j < size_bitmask - 1; j++ ) {
			threshold += Math.pow(2, -1*j -1);

			if( cur_random < threshold ) {
				break;
			}
		}

		int bitmask = 0;

		if( j < size_bitmask - 1 ) {
			int small_bitmask = 1 << (size_bitmask - 1 - j) ;
			// move small_bitmask to MSB bits of bitmask;
			bitmask = small_bitmask << ( 32 - size_bitmask ) ;
		}

		return bitmask;
	}

	// calculate Neighborhood function N(h) from bitmasks.
	public static double nh_from_bitmask(long [] bitmask, int K)
	{
		int i;
		double avg_bitpos = 0;

		for(i = 0; i < K; i++) {
			avg_bitpos += (double) FMBitmask.find_least_zero_pos( bitmask[i] );
		}

		avg_bitpos = avg_bitpos / (double)K;

		return Math.pow(2, avg_bitpos)/0.77351;
	}

	// fine the least zero bit position in a number 
	public static int find_least_zero_pos(long number) 
	{
		int i;

		for(i = 0; i < 32; i++) {
			int mask = 1 << (31-i);
			
			if( (number & mask) == 0 ) {
				return i;
			}
		}

		return i;
	}
}

class HadiUtils
{
	// record the latest radius info, and 
	// delete previous radius info if not useful for effective radius calculation.
	public static String update_radhistory( long [] self_bitmask, String saved_rad_nh, int cur_radius, int nreplication) {
		double max_nh = FMBitmask.nh_from_bitmask(self_bitmask, nreplication);
		double ninety_maxnh = 0.9 * max_nh;
		String[] token = saved_rad_nh.split(":");
		int i;
		String result = "";
		boolean bAboveThreshold = false;
		int cur_hop=0, prev_hop=0;
		double cur_nh, prev_nh = 0;
		DecimalFormat df = new DecimalFormat("#.#");
		boolean bFirstAdd = true;

		for(i = 0; i < token.length; i += 2) {
			cur_hop = Integer.parseInt( token[i] );
			cur_nh = Double.parseDouble( token[i+1] );
			if( bAboveThreshold == false ) {
				if( cur_nh >= ninety_maxnh ) {
					bAboveThreshold = true;
					
					if( i > 0 ) {
						result = result + ":" + prev_hop + ":" + prev_nh;
					}
				}
			}

			if( bAboveThreshold ) {
				result = result + ":" + cur_hop + ":" + df.format(cur_nh);
			}
			prev_nh = cur_nh;
			prev_hop = cur_hop;
		}

		if( token.length > 0 && result.length() == 0 && cur_hop>0 ) {
			result = result + ":" + prev_hop + ":" + prev_nh;
		}

		result  = result + ":" + cur_radius + ":" + df.format(max_nh);
		//System.out.println("[DEBUG] update_radhistory result=" + result);

		return result;
	}

	// calculate the effective diameter of a graph, given neighborhood results.
	public static float effective_diameter( float []N, int max_radius )
	{
		float max_nh = N[ max_radius ];
		int i;
		float threshold = max_nh * 0.9f;

		for(i=1; i <= max_radius; i++) {
			if( N[i] >= threshold ) {
				float decimal = (threshold - N[i-1])/(N[i] - N[i-1]) ;
				return (i - 1 + decimal);
			}
		}

		return -1;
	}

	// calculate the average diameter of a graph, given neighborhood results.
	public static float average_diameter( float []N, int max_radius )
	{
		float min_nh = N[ 0 ];
		float max_nh = N[ max_radius ];
		int h;
		float sum = 0;

		for(h=1; h <= max_radius; h++) {
			sum += h * (N[h] - N[h-1]);
		}

		sum = sum / (max_nh - min_nh);

		return sum;
	}

	// read neighborhood number after each iteration.
	public static HadiResultInfo readNhoodOutput(String new_path) throws Exception
	{
		String output_path = new_path + "/part-00000";
		String str = "";
		try {
			BufferedReader in = new BufferedReader(
				new InputStreamReader(new FileInputStream( output_path ), "UTF8"));
			str = in.readLine();
		} catch (UnsupportedEncodingException e) {
		} catch (IOException e) {
		}

	    final String[] line = str.split("\t");

		HadiResultInfo ri = new HadiResultInfo();
		ri.nh = Float.parseFloat( line[1] );
		ri.converged_nodes = Integer.parseInt( line[2] );
		ri.changed_nodes = Integer.parseInt( line[3] );

		return ri;
	}
};


// Hadi Main Class
public class Hadi extends Configured implements Tool 
{
    public static int MAX_ITERATIONS = 2048;
	public static float N[] = new float[MAX_ITERATIONS];	// save N(h)
	static int iter_counter = 0;

    //////////////////////////////////////////////////////////////////////
    // STAGE 1: generate partial bitstrings.
	//  - Input: edge_file, bitstrings_from_the_last_iteration(or, bitstring generation command)
	//  - Output: partial bitstrings
    //////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		int make_symmetric = 0;		// Indicates whether to make reverse edges or not.

		public void configure(JobConf job) {
			make_symmetric = Integer.parseInt(job.get("make_symmetric"));

			System.out.println("MapStage1: make_symmetric = " + make_symmetric);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in the edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 2 )						// ignore ill-formated data.
				return;

			if( line[1].startsWith("b") ||				// bitmask from previous iterations
				line[1].startsWith("c") ) {				// bitmask creation command
				output.collect(new IntWritable(Integer.parseInt(line[0])), new Text(line[1]));
			} else {									// (src, dst) edge
				int dst_nodeid = Integer.parseInt(line[1]);
				output.collect(new IntWritable(dst_nodeid), new Text(line[0]));	// invert to and from

				if( make_symmetric == 1 ) {										// make the reverse edge
					int src_nodeid = Integer.parseInt(line[0]);

					if( src_nodeid != dst_nodeid)
						output.collect(new IntWritable(src_nodeid), new Text(line[1]));
				}
			}
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

			System.out.println("RedStage1: number_nodes = " + number_nodes + ", nreplication = " + nreplication + ", encode_bitmask="+encode_bitmask);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			String bitmask = "";
			Set<Integer> src_nodes_set = new HashSet<Integer>();
			boolean self_contained = false;
			String cur_value="";

			while (values.hasNext()) {
				cur_value = values.next().toString();

				if (cur_value.startsWith("b")) {		// bitmask line
					bitmask = cur_value;
				} else if (cur_value.startsWith("c")) {	// bitmask create command line
					bitmask = FMBitmask.generate_bitmask( number_nodes, nreplication, encode_bitmask );
				} else {								// edge line
					int src_node_int = Integer.parseInt(cur_value);
					src_nodes_set.add( src_node_int );
					if( key.get() == src_node_int)
						self_contained = true;
				}
			}

			if( self_contained == false )			// add self loop, if not exists.
				src_nodes_set.add(key.get());

			char complete_prefix='x';
			try {
				if( bitmask.charAt(2) == 'i' )
					complete_prefix = 'i';
				else
					complete_prefix = 'f';
			} catch(Exception ex) {
				System.out.println("Exception at bitmask.charAt(2). bitmask=["+bitmask+"],key="+key.get());
			}

			try {
				Iterator src_nodes_it = src_nodes_set.iterator();
				while (src_nodes_it.hasNext()) {
					String bitmask_new;
					int cur_key_int = ((Integer)src_nodes_it.next()).intValue();

					if( cur_key_int == key.get() ) {	// partial bitmask from 'self'
						bitmask_new = "bs" + complete_prefix + bitmask.substring(3);
						output.collect(new IntWritable(cur_key_int), new Text(bitmask_new));
					} else {							// partial bitmask from 'others'
						bitmask_new = "bo" + complete_prefix + bitmask.substring(3);
						output.collect(new IntWritable(cur_key_int), new Text(bitmask_new));
					}
				}
			} catch(Exception ex) {
				System.out.println("Exception at bitmask.substring(3). bitmask=["+bitmask+"],key="+key.get());
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
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
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

		public void configure(JobConf job) {
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));
			cur_radius = Integer.parseInt(job.get("cur_radius"));

			System.out.println("RedStage2: nreplication = " + nreplication + ", encode_bitmask = "+encode_bitmask +", cur_radius = " + cur_radius);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			long [] bitmask={0,};
			long [] self_bitmask={0,};
			int bitmask_len = -1;
			int i;
			String out_val ="bs";
			boolean bSelfChanged = false;
			char complete_prefix = 'x';
			String complete_bitstring = "";
			boolean bSelf;
			String saved_self_prefix="";

			while (values.hasNext()) {
				String cur_bm_string = values.next().toString();
				String cur_value = "";
				int bitmask_start_index = cur_bm_string.indexOf(' ');
				cur_value = cur_bm_string.substring(bitmask_start_index+1);	

				if( cur_bm_string.charAt(1) == 's' ) {	// current bitmask came from itself
					complete_prefix = cur_bm_string.charAt(2);
					bSelf = true;		
					int sp_pos = cur_bm_string.indexOf(' ');
					saved_self_prefix = cur_bm_string.substring(2, sp_pos);
				}
				else									// current bitmask came from adjacent nodes
					bSelf = false;		

				if( bitmask_len == -1 ) {
					bitmask_len = nreplication;
					bitmask = new long[nreplication];
					self_bitmask = new long[nreplication];
					for(i= 0; i < nreplication; i++)
						bitmask[i] = 0;
				}

				// update bitmasks using OR operations
				if( encode_bitmask == 1 ) {
					int [] cur_mask = BitShuffleCoder.decode_bitmasks( cur_value, nreplication);

					for(i = 0; i < nreplication; i++) {
						bitmask[i] = (bitmask[i] | cur_mask[i]);

						if( bSelf == true ) 
							self_bitmask[i] = cur_mask[i];
					}
				} else {
					String [] str_bitmasks = cur_value.split(" ");

					for(i = 0; i < nreplication; i++) {
						long cur_mask = Long.parseLong( str_bitmasks[i], 16 );
						bitmask[i] = (bitmask[i] | cur_mask);

						if( bSelf == true ) 
							self_bitmask[i] = cur_mask;
					}

				}
			}


			// check whether the self bitmask didn't change.
			for(i = 0; i< nreplication; i++) {
				if( self_bitmask[i] != bitmask[i] ) {
					bSelfChanged = true;
					break;
				}
			}

			if( bSelfChanged == true ) {	// if at least a bitmask changed
				if( saved_self_prefix.length() >= 1 ) {
					int colonPos = saved_self_prefix.indexOf(':');
					out_val += ("i" + (cur_radius-1) + HadiUtils.update_radhistory(self_bitmask, saved_self_prefix.substring(colonPos+1), cur_radius, nreplication) );//out_val += "i";
				} else
					out_val += ("i" + (cur_radius-1));
			} else {						// if all bitmasks didn't change
				if( complete_prefix == 'i' ) {
					out_val += ("c" + (cur_radius-1)) ;
					int colonPos = saved_self_prefix.indexOf(':');
					if( colonPos >= 0 )
						out_val += saved_self_prefix.substring(colonPos);
				} else						// complete_prefix == 'c' or 'f'
					out_val += saved_self_prefix;	// "f" + saved_radius
			}

			if(encode_bitmask == 1)
				out_val += ( " " + BitShuffleCoder.encode_bitmasks( bitmask, nreplication ) );
			else {
				for(i = 0; i < nreplication; i++)
					out_val = out_val + " " + Long.toHexString(bitmask[i]) ;
			}

			output.collect(key, new Text( out_val ) );
		}
    }

    public static class CombinerStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
    {
		int nreplication = 0;
		int encode_bitmask = 0;

		public void configure(JobConf job) {
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));

			System.out.println("CombinerStage2: nreplication = " + nreplication + ", encode_bitmask="+encode_bitmask);
		}

		public void reduce (final IntWritable key, final Iterator<Text> values, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
        {
			long [] bitmask={0,};
			int bitmask_len = -1;
			int i;
			String out_val ="boi";
			boolean bSelfChanged = false;
			char complete_prefix = 'x';
			boolean bStopWhileLoop = false;

			while (values.hasNext()) {
				Text cur_value_text = values.next();
				String cur_bm_string = cur_value_text.toString();
				int bitmask_start_index = cur_bm_string.indexOf(' ');
				String cur_value = cur_bm_string.substring(bitmask_start_index+1);	
				boolean bSelf;

				if( cur_bm_string.charAt(1) == 's' ) {			// for calculating individual diameter
					output.collect(key, new Text(cur_value_text) );
					continue;
				}

				if( bitmask_len == -1 ) {
					bitmask_len = nreplication;
					bitmask = new long[nreplication];
					for(i= 0; i < nreplication; i++)
						bitmask[i] = 0;
				}

				// update bitmasks using OR operations
				if( encode_bitmask == 1 ) {
					int [] cur_mask = BitShuffleCoder.decode_bitmasks( cur_value, nreplication);
					for(i = 0; i < nreplication; i++) {
						bitmask[i] = (bitmask[i] | cur_mask[i]);

					}
				} else {
					String [] str_bitmasks = cur_value.split(" ");
					for(i = 0; i < str_bitmasks.length; i++) {
						long cur_mask = Long.parseLong( str_bitmasks[i], 16 );
						bitmask[i] = (bitmask[i] | cur_mask);
					}
				}
			}

			// output partial bitmasks.
			if( bitmask_len != -1) {
				if( encode_bitmask == 1 )
					out_val += ( " " + BitShuffleCoder.encode_bitmasks( bitmask, nreplication ) );
				else {
					for(i = 0; i < nreplication; i++)
						out_val = out_val + " " + Long.toHexString(bitmask[i]) ;
				}
	
				output.collect(key, new Text( out_val ) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 3: Calculate N(h) and the number of changed nodes.
	//  - Input: the converged bitstrings
	//  - Output: Neighborhood(h)  TAB  number_of_converged_nodes   TAB  number_of_changed_nodes
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage3 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
    {
		private final IntWritable zero_id = new IntWritable(0);
		private Text output_val;

		int nreplication = 0;
		int encode_bitmask = 0;

		public void configure(JobConf job) {
			nreplication = Integer.parseInt(job.get("nreplication"));
			encode_bitmask = Integer.parseInt(job.get("encode_bitmask"));

			System.out.println("MapStage3: nreplication = " + nreplication + ", encode_bitmask="+encode_bitmask);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			if (value.toString().startsWith("#"))		// ignore comments line
				return;

			final String[] line = value.toString().split("\t");
			char complete_prefix = line[1].charAt(2);
			int i;
			double avg_bitpos = 0;
			int converged_count = 0;
			int changed_count = 0;

			int bitmask_start_index = line[1].indexOf(' ');
			String bitmask_str = line[1].substring(bitmask_start_index+1);	

			if( encode_bitmask == 1 ) {
				int [] bitmask = BitShuffleCoder.decode_bitmasks( bitmask_str, nreplication );

				for(i = 0; i < nreplication; i++)
					avg_bitpos += (double) FMBitmask.find_least_zero_pos( bitmask[i] );
			} else {
				String [] bitmasks = bitmask_str.split(" ");
				for(i = 0; i < bitmasks.length; i++)
					avg_bitpos += (double) FMBitmask.find_least_zero_pos( Long.parseLong( bitmasks[i], 16 ) );
			}

			avg_bitpos = avg_bitpos / nreplication;
			
			if( complete_prefix == 'c')
				converged_count = 1;

			if( complete_prefix == 'i')
				changed_count = 1;

			output_val = new Text( Double.toString(Math.pow(2, avg_bitpos)/0.77351 ) + "\t" + converged_count + "\t" + changed_count);

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
		// input sample :
		// 0       bsi1:1:1.8:2:2.6 8f81878...
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, Text> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");
			final String[] tokens = line[1].split(" ");
			int max_radius = 0;
			double eff_radius=0;//int eff_radius = 0;
			double eff_nh = 0;

			String radius_str = tokens[0].substring(3) ;
			if( radius_str.length() > 0 ) {
				String[] radius_info = radius_str.split(":");
				if( radius_info.length > 1 ) {
					max_radius = Integer.parseInt( radius_info[ radius_info.length -2] );
					eff_radius = max_radius;
					double max_nh = Double.parseDouble( radius_info[ radius_info.length -1] );
					eff_nh = max_nh;
					double ninety_th = max_nh * 0.9;
					for(int i = radius_info.length -4; i >=0; i -= 2) {
						int cur_hop = Integer.parseInt( radius_info[i] );
						double cur_nh = Double.parseDouble( radius_info[i+1] );

						if( cur_nh >= ninety_th ) {
							eff_radius = cur_hop;
							eff_nh = cur_nh;
						} else {
							eff_radius = cur_hop + (double)(ninety_th - cur_nh)/(eff_nh - cur_nh);
							break;
						}
					}
				}

				DecimalFormat df = new DecimalFormat("#.##");

				output.collect( new IntWritable(Integer.parseInt(line[0])), new Text("bsf" + max_radius + ":" + df.format(eff_radius)) );
			}
		}
    }

    //////////////////////////////////////////////////////////////////////
    // STAGE 5: Summarize radii information
	//  - Input: current bitstrings
	//  - Output: effective_radius TAB number_of_nodes_with_such_radius
    //////////////////////////////////////////////////////////////////////
    public static class	MapStage5 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, IntWritable>
    {
		public void map (final LongWritable key, final Text value, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			final String[] line = value.toString().split("\t");
			final String[] tokens = line[1].split(" ");

			String radius_str = tokens[0].substring(3) ;
			if( radius_str.length() > 0 ) {
				String[] radius_info = radius_str.split(":");
				double eff_radius = Double.parseDouble(radius_info[1]);
				output.collect( new IntWritable((int)Math.round(eff_radius)), new IntWritable(1) );
			}
		}
    }

    public static class	RedStage5 extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
		public void reduce (final IntWritable key, final Iterator<IntWritable> values, final OutputCollector<IntWritable, IntWritable> output, final Reporter reporter) throws IOException
		{
			int sum = 0;

			while (values.hasNext()) {
				int cur_count = values.next().get();

				sum += cur_count;
			}

			output.collect(key, new IntWritable(sum));
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
	enum EdgeType { Regular, Inverted };
	protected EdgeType edge_type;
	protected int encode_bitmask = 0;
	protected int cur_radius = 1;
	protected int start_from_newbm = 0;
	protected int resume_from_radius = 0;
	protected int make_symmetric = 0;		// convert directed graph to undirected graph

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new Hadi(), args);

		System.exit(result);
    }

    // Print the usage text.
    protected static int printUsage ()
    {
		System.out.println("hadi <edge_path> <curbm_path> <tempbm_path> <nextbm_path> <output_path> <# of vertices> <# of replication> <# of reducers> <enc or noenc> <newbm or contNN> <makesym or reg> <'max' or maximum_iteration>");

		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
    }

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
		radius_path = new Path("hadi_radius");
		radius_summary_path = new Path("hadi_radius_summary");
		nreplication = Integer.parseInt(args[6]);
		nreducer = Integer.parseInt(args[7]);

		if( args[8].compareTo("enc") == 0 )
			encode_bitmask = 1;

		if( args[9].compareTo("newbm") == 0 ) {
			start_from_newbm = 1;
		}
		else if( args[9].startsWith("cont") ){
			start_from_newbm = 0;
			cur_radius = Integer.parseInt(args[9].substring(4));
		}

		if( args[10].compareTo("makesym") == 0 )
			make_symmetric = 1;
		else
			make_symmetric = 0;

		if( args[11].compareTo("max") != 0 )
			max_iteration = Integer.parseInt(args[11]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Computing Radii/Diameter. Current hop: " + cur_radius + ", edge_path: " + args[0] + ", encode: " + encode_bitmask + ", # reducers: " + nreducer + ", makesym: " + make_symmetric + ", max_iteration: " + max_iteration + "\n");

		local_output_path = args[4] + number_nodes + "_temp";

		if( start_from_newbm == 1 ) {
			System.out.print("Generating initial bitstrings for " + number_nodes + " nodes ");
			
			// create bitmask generate command file, and copy to curbm_path
			gen_bitmask_cmd_file(number_nodes, nreplication, curbm_path);
			System.out.println(" done");
		} else {
			System.out.println("Resuming from current hadi_curbm which contains up to N(" + (cur_radius -1) + ")");
		}

		N[0] = number_nodes;

		boolean eff_diameter_computed = false;

		// Iteratively run Stage1 to Stage3.
		for (i = cur_radius; i <= max_iteration; i++) {
			JobClient.runJob(configStage1(edge_type));
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
				System.out.println("Calculating the effective diameter...");
				JobClient.runJob(configStage4());
				eff_diameter_computed = true;
				break;
			}

			// rotate directory. 
			fs.delete(curbm_path);
			fs.delete(tempbm_path);
			if(i < MAX_ITERATIONS - 1 )
				fs.delete(output_path);
			fs.rename(nextbm_path, curbm_path);

			cur_radius++;
		}

		if( eff_diameter_computed == false ) {
			System.out.println("Calculating the effective diameter...");
			JobClient.runJob(configStage4());
		}

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
		System.out.println("[PEGASUS] Radii summary is saved in the HDFS " + radius_summary_path.getName() + "\n");

		return 0;
    }

	// generate bitmask command file which is used in the 1st iteration.
	public void gen_bitmask_cmd_file(int number_nodes, int nreplication, Path curbm_path) throws IOException
	{
		int start_pos = 0;
		int i;
		int max_filesize = 10000000;

		for(i=0; i < number_nodes; i+=max_filesize)
		{
			int len=max_filesize;
			if(len > number_nodes-i)
				len = number_nodes - i;
			gen_bitmask_cmd_file(number_nodes, i, len, nreplication, curbm_path);
         }
	}

	// generate a part of the bitmask command file 
	public void gen_bitmask_cmd_file(int number_nodes, int start_pos,int len, int nreplication, Path curbm_path) throws IOException
	{
		// generate a temporary local bitmask command file
		int i, j = 0, threshold = 0, count=0;
		String file_name = "bitmask_cmd.hadi."+number_nodes+"."+start_pos;
		FileWriter file = new FileWriter(file_name);
		BufferedWriter out = new BufferedWriter (file);

		out.write("# bitmask command file for HADI\n");
		out.write("# number of nodes in graph = " + number_nodes+", start_pos="+start_pos+"\n");
		System.out.println("creating bitmask generation cmd for node " + start_pos + " ~ " + (start_pos+len));

		for(i=0; i < number_nodes; i++)
		{
			int cur_nodeid = start_pos + i;
			out.write(cur_nodeid + "\tc\n");
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

    // Configure Stage1
    protected JobConf configStage1(EdgeType edgeType) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), Hadi.class);
		conf.set("number_nodes", "" + number_nodes);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.set("make_symmetric", "" + make_symmetric);
		conf.setJobName("HADI_Stage1");

		conf.setMapperClass(MapStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, edge_path, curbm_path);  
		FileOutputFormat.setOutputPath(conf, tempbm_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		return conf;
    }

    // Configure Stage2
    protected JobConf configStage2 () throws Exception
    {
		final JobConf conf = new JobConf(getConf(), Hadi.class);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.set("cur_radius", "" + cur_radius);
		conf.setJobName("HADI_Stage2" );
		
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
		final JobConf conf = new JobConf(getConf(), Hadi.class);
		conf.set("nreplication", "" + nreplication);
		conf.set("encode_bitmask", "" + encode_bitmask);
		conf.setJobName("HADI_Stage3");
		
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
		final JobConf conf = new JobConf(getConf(), Hadi.class);
		conf.setJobName("HADI_Stage4");
		
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
		final JobConf conf = new JobConf(getConf(), Hadi.class);
		conf.setJobName("HADI_Stage5");
		
		conf.setMapperClass(MapStage5.class);        
		conf.setReducerClass(RedStage5.class);
		conf.setCombinerClass(RedStage5.class);

		FileInputFormat.setInputPaths(conf, radius_path);  
		FileOutputFormat.setOutputPath(conf, radius_summary_path);  

		conf.setNumReduceTasks( nreducer );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(IntWritable.class);

		return conf;
    }
}
