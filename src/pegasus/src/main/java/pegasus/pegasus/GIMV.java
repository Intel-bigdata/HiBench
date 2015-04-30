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
File: GIMV.java
 - A main class for Generalized Iterative Matrix-Vector multiplication.
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

class VectorElem<T>
{
	public short row;	
	public T val;
	public VectorElem(short in_row, T in_val) {
		row = in_row;
		val = in_val;
	}

	public double getDouble() {
		return ((Double)val).doubleValue();
	}
};

class BlockElem<T>
{
	public short row;	
	public short col;
	public T val;
	public BlockElem(short in_row, short in_col, T in_val) {
		row = in_row;
		col = in_col;
		val = in_val;
	}
};

enum EdgeType { Real, Binary };

public class GIMV
{
	// convert strVal to array of VectorElem<Integer>.
	// strVal is msu(ROW-ID   VALUE)s. ex) 0 0.5 1 0.3
	//            oc
	public static<T> ArrayList<VectorElem<T>> parseVectorVal(String strVal, Class<T> type) 
	{
		ArrayList arr = new ArrayList<VectorElem<T>>();
		final String[] tokens = strVal.split(" ");
		int i;

		for(i = 0; i < tokens.length; i += 2) {
			short row = Short.parseShort(tokens[i]);
			if( type.getSimpleName().equals("Integer") ) {
				int val = Integer.parseInt(tokens[i+1]);
				arr.add( new VectorElem(row, val) );
			} else if( type.getSimpleName().equals("Double") ) {
				double val = Double.parseDouble(tokens[i+1]);
				arr.add( new VectorElem(row, val) );
			}
		}

		return arr;
	}

	// parse HADI vector
	public static ArrayList<VectorElem<String>> parseHADIVector(String strVal) 
	{
		ArrayList arr = new ArrayList<VectorElem<Integer>>();
		final String[] tokens = strVal.substring(1).split(" ");

		for( int i = 0; i < tokens.length; i += 2 ) {
			short row = Short.parseShort(tokens[i]);
			String bitstring;

			bitstring = tokens[i+1];

			arr.add( new VectorElem<String>(row, bitstring) );
		}

		// the maximum length of arr is block_width.
		return arr;
	}


	// compute the dot product of two vector blocks.
	// strVal is msu(ROW-ID   VALUE)s. ex) 0 0.5 1 0.3
	//            oc
	public static ArrayList<VectorElem<Double>> multDiagVector(String strDiag, String strVec, int block_width) 
	{
		short i;
		ArrayList arr = new ArrayList<VectorElem<Double>>();

		if( strDiag.length() == 0 )
			return arr;

		double []dVal1 = new double[block_width];
		double []dVal2 = new double[block_width];

		String[] tokens1 = strDiag.split(" ");
		String[] tokens2 = strVec.split(" ");

		for(i = 0; i < block_width; i++) {
			dVal1[i] = 0;
			dVal2[i] = 0;
		}

		for(i = 0; i < tokens1.length; i += 2) {
			short row = Short.parseShort(tokens1[i]);
			double val = Double.parseDouble(tokens1[i+1]);
			dVal1[row] = 1.0 / val;
		}

		for(i = 0; i < tokens2.length; i += 2) {
			short row = Short.parseShort(tokens2[i]);
			double val = Double.parseDouble(tokens2[i+1]);
			dVal2[row] = val;
		}

		for(i = 0; i < block_width; i++) {
			if( dVal1[i] != 0 && dVal2[i] != 0 ) 
				arr.add( new VectorElem(i, (dVal1[i]*dVal2[i])) );
		}

		return arr;
	}


	// return value : true (if every VectorElem starts with 'f')
	//                false (otherwise)
	public static boolean IsCompleteHADIVector(ArrayList<VectorElem<String>> arr)
	{
		Iterator<VectorElem<String>> vector_iter = arr.iterator();

		while( vector_iter.hasNext() ) {
			VectorElem<String> cur_ve = vector_iter.next();
			if( cur_ve.val.charAt(0) != 'f' )
				return false;
		}

		return true;
	}

	public static ArrayList<VectorElem<Integer>> minBlockVector(ArrayList<BlockElem<Integer>> block, ArrayList<VectorElem<Integer>> vector, int block_width, int isFastMethod) 
	{
		int[] out_vals = new int[block_width];	// buffer to save output
		short i;

		for(i=0; i < block_width; i++)		
			out_vals[i] = -1;

		Iterator<VectorElem<Integer>> vector_iter;
		Iterator<BlockElem<Integer>> block_iter;
		Map<Short, Integer> vector_map = new HashMap<Short, Integer>();

		// initialize out_vals 
		if( isFastMethod == 1 ) {
			vector_iter = vector.iterator();
			while(vector_iter.hasNext() ) {
				VectorElem<Integer> v_elem = vector_iter.next();
				out_vals[ v_elem.row ] = v_elem.val;
			}
		}

		vector_iter = vector.iterator();
		block_iter = block.iterator();
		BlockElem<Integer> saved_b_elem = null;

		while( vector_iter.hasNext() ) {
			VectorElem<Integer> v_elem = vector_iter.next();
			vector_map.put(v_elem.row, v_elem.val);
		}


		BlockElem<Integer> b_elem;
		while(block_iter.hasNext() || saved_b_elem != null) {
			b_elem = block_iter.next();

			Integer vector_val = vector_map.get (b_elem.col);
			if( vector_val != null) {
				int vector_val_int = vector_val.intValue();
				if( out_vals[ b_elem.row ] == -1 )
					out_vals[ b_elem.row ] = vector_val_int;
				else if( out_vals[ b_elem.row ] > vector_val_int )
					out_vals[ b_elem.row ] = vector_val_int;
			}
		}
		
		ArrayList<VectorElem<Integer>> result_vector = null;
		for(i = 0; i < block_width; i++) {
			if( out_vals[i] != -1 ) {
				if( result_vector == null )
					result_vector = new ArrayList<VectorElem<Integer>>();
				result_vector.add( new VectorElem<Integer>(i, out_vals[i]) );
			}
		}

		return result_vector;
	}


	// Perform the BIT-OR() operation on one block and one vector.
	// return value : the result vector
	public static ArrayList<VectorElem<String>> bworBlockVector(ArrayList<BlockElem<Integer>> block, ArrayList<VectorElem<String>> vector, int block_width, int nreplication, int encode_bitmask) 
	{
		long[][] out_vals = new long[block_width][nreplication];	// buffer to save output
		short i;
		int j;

		for(i=0; i < block_width; i++)
			for(j=0; j < nreplication; j++)
				out_vals[i][j] = 0;

		Iterator<VectorElem<String>> vector_iter;
		Iterator<BlockElem<Integer>> block_iter;
		Map<Short, String> vector_map = new HashMap<Short, String>();

		vector_iter = vector.iterator();
		block_iter = block.iterator();
		BlockElem<Integer> saved_b_elem = null;

		while( vector_iter.hasNext() ) {
			VectorElem<String> v_elem = vector_iter.next();
			vector_map.put(v_elem.row, v_elem.val);
		}

		BlockElem<Integer> b_elem;
		while(block_iter.hasNext() || saved_b_elem != null) {
			b_elem = block_iter.next();

			String vector_str = vector_map.get (b_elem.col);
			if( vector_str != null ) {
				if( encode_bitmask == 1 ) {
					int startpos_bm = vector_str.indexOf('~');
					int [] cur_mask = BitShuffleCoder.decode_bitmasks( vector_str.substring(startpos_bm + 1), nreplication);
					for(j = 0; j < nreplication; j++)
						out_vals[b_elem.row][j] = (out_vals[b_elem.row][j] | cur_mask[j]);	
				} else {
					String[] tokens = vector_str.split("~");
					for(j = 0; j < nreplication; j++) {
						long cur_mask = Long.parseLong( tokens[j+1], 16 );
						out_vals[b_elem.row][j] = (out_vals[b_elem.row][j] | cur_mask);
					}
				}
			}
		}
		
		ArrayList<VectorElem<String>> result_vector = new ArrayList<VectorElem<String>>();

		int nonzero_count = 0;
		for(i = 0; i < block_width; i++) {
			String out_str = "i";

			for(j = 0; j < nreplication; j++) {
				if( out_vals[i][j] != 0 )
					break;
			}
			if( j == nreplication )
				continue;

			if( encode_bitmask == 1 ) {
				out_str += ( "~" + BitShuffleCoder.encode_bitmasks( out_vals[i], nreplication ) );
			} else {
				for(j = 0; j < nreplication; j++)
					out_str = out_str + "~" + Long.toHexString(out_vals[i][j]) ;
			}
			
			result_vector.add( new VectorElem<String>((short)i, out_str) );
		}

		return result_vector;
	}


	// multiply one block and one vector
	// return : result vector
	public static ArrayList<VectorElem<Double>> multBlockVector(ArrayList<BlockElem<Double>> block, ArrayList<VectorElem<Double>> vector, int i_block_width) 
	{
		double[] out_vals = new double[i_block_width];	// buffer to save output
		short i;

		for(i=0; i < i_block_width; i++)		
			out_vals[i] = 0;

		Iterator<VectorElem<Double>> vector_iter = vector.iterator();
		Iterator<BlockElem<Double>> block_iter = block.iterator();
		BlockElem<Double> saved_b_elem = null;

		while( vector_iter.hasNext() ) {
			VectorElem<Double> v_elem = vector_iter.next();

			BlockElem<Double> b_elem;
			while(block_iter.hasNext() || saved_b_elem != null) {
				if( saved_b_elem != null ) {
					b_elem = saved_b_elem;
					saved_b_elem = null;
				} else
					b_elem = block_iter.next();

				// compare v_elem.row and b_elem.col
				if( b_elem.col < v_elem.row )
					continue;
				else if( b_elem.col == v_elem.row ) {
					out_vals[ b_elem.row ] += b_elem.val * v_elem.val;
				} else {	// b_elem.col > v_elem.row
					saved_b_elem = b_elem;
					break;
				}
			}

		}
		
		ArrayList<VectorElem<Double>> result_vector = null;
		for(i = 0; i < i_block_width; i++) {
			if( out_vals[i] != 0 ) {
				if( result_vector == null )
					result_vector = new ArrayList<VectorElem<Double>>();
				result_vector.add( new VectorElem(i, out_vals[i]) );
			}
		}

		return result_vector;
	}


	// multiply one block and one vector, when the block is in the bit encoded format.
	// return : result vector
	public static ArrayList<VectorElem<Double>> multBlockVector(byte[] block, ArrayList<VectorElem<Double>> vector, int i_block_width) 
	{
		double[] out_vals = new double[i_block_width];	// buffer to save output
		short i;

		for(i=0; i < i_block_width; i++)		
			out_vals[i] = 0;

		Iterator<VectorElem<Double>> vector_iter = vector.iterator();

		while( vector_iter.hasNext() ) {
			VectorElem<Double> v_elem = vector_iter.next();

			int col = v_elem.row;
			for(int row = 0; row < i_block_width; row++) {
				int edge_elem = block[ (row*i_block_width + col)/8 ] & ( 1 << (col % 8) );
				if( edge_elem > 0 ) {
					out_vals[ row ] += v_elem.val;
				}
			}
		}

		ArrayList<VectorElem<Double>> result_vector = null;
		for(i = 0; i < i_block_width; i++) {
			if( out_vals[i] != 0 ) {
				if( result_vector == null )
					result_vector = new ArrayList<VectorElem<Double>>();
				result_vector.add( new VectorElem(i, out_vals[i]) );
			}
		}

		return result_vector;
	}


	// convert strVal to array of BlockElem<Integer>.
	// strVal is (COL-ID     ROW-ID   VALUE)s. ex) 0 0 1 1 0 1 1 1 1
	// note the strVal is tranposed. So we should tranpose it to (ROW-ID   COL-ID ...) format.
	public static<T> ArrayList<BlockElem<T>> parseBlockVal(String strVal, Class<T> type) 
	{
		ArrayList arr = new ArrayList<BlockElem<T>>();
		final String[] tokens = strVal.split(" ");
		int i;

		if( type.getSimpleName().equals("Double") ) {
			for(i = 0; i < tokens.length; i += 3) {
				short row = Short.parseShort(tokens[i+1]);
				short col = Short.parseShort(tokens[i]);
				double val = Double.parseDouble(tokens[i+2]);

				BlockElem<T> be = new BlockElem(row, col, val);
				arr.add( be );
			}
		} else if ( type.getSimpleName().equals("Integer") ) {
			for(i = 0; i < tokens.length; i += 2) {
				short row = Short.parseShort(tokens[i+1]);
				short col = Short.parseShort(tokens[i]);

				BlockElem<T> be = new BlockElem(row, col, 1);
				arr.add( be );
			}
		}

		return arr;
	}

	// make Text format output by combining the prefix and vector elements.
	public static<T> Text formatVectorElemOutput( String prefix, ArrayList<VectorElem<T>> vector) {
		String cur_block_output = prefix;
		int isFirst = 1;
		if( vector != null && vector.size() > 0 ) {
			Iterator<VectorElem<T>> cur_mult_result_iter = vector.iterator();

			while( cur_mult_result_iter.hasNext() ) {
				VectorElem<T> elem = cur_mult_result_iter.next();
				if( cur_block_output != "" && isFirst == 0)
					cur_block_output += " ";
				cur_block_output += ("" + elem.row + " " + elem.val);
				isFirst = 0;
			}

			return new Text( cur_block_output );
		}

		return new Text("");
	}

	// make Text format HADI output by combining the prefix and vector elements.
	public static Text formatHADIVectorElemOutput( String prefix, ArrayList<VectorElem<String>> vector ) {
		String cur_block_output = prefix;
		int isFirst = 1;
		if( vector != null && vector.size() > 0 ) {
			Iterator<VectorElem<String>> cur_mult_result_iter = vector.iterator();

			while( cur_mult_result_iter.hasNext() ) {
				VectorElem<String> elem = cur_mult_result_iter.next();
				if( cur_block_output != "" && isFirst == 0)
					cur_block_output += " ";
				if( elem.val.charAt(0) == 'i' )
					cur_block_output += ("" + elem.row + " " + elem.val);
				else
					cur_block_output += ( "" + elem.row + " " + "f" + elem.val.substring(1) );
				isFirst = 0;
			}
			
			return new Text( cur_block_output );
		}

		return new Text("");
	}

	// compare two vectors.
	// return value : 0 (same)
	//                1 (different)
	public static<T> int compareVectors( ArrayList<VectorElem<T>> v1, ArrayList<VectorElem<T>> v2 ) {
		if( v1.size() != v2.size() )
			return 1;

		Iterator<VectorElem<T>> v1_iter = v1.iterator();
		Iterator<VectorElem<T>> v2_iter = v2.iterator();

		while( v1_iter.hasNext() ) {
			VectorElem<T> elem1 = v1_iter.next();
			VectorElem<T> elem2 = v2_iter.next();

			if( elem1.row != elem2.row || ((Comparable)(elem1.val)).compareTo(elem2.val) != 0 )
				return 1;
		}

		return 0;
	}

	// print the content of the input vector.
	public static<T> int printVector( ArrayList<VectorElem<T>> vector ) {
		Iterator<VectorElem<T>> v_iter = vector.iterator();

		System.out.print("vector : ");
		while( v_iter.hasNext() ) {
			VectorElem<T> elem = v_iter.next();

			System.out.print(" v[" + elem.row + "] = " + elem.val );
		}

		System.out.println("");

		return 0;
	}

	// make an integer vector
	public static ArrayList<VectorElem<Integer>> makeIntVectors( int[] int_vals, int block_width ) {
		int i;
		ArrayList<VectorElem<Integer>> result_vector = new ArrayList<VectorElem<Integer>>();

		for(i = 0; i < block_width; i++) {
			if( int_vals[i] != -1 ) {
				result_vector.add( new VectorElem<Integer>((short)i, int_vals[i]) );
			}
		}

		return result_vector;
	}

	// parse a hadi bitstring( k replication )
	public static long[] parseHADIBitString( String in_str, int nreplication, int encode_bitmask ) {
		long[] cur_bm = new long[nreplication];

		if( encode_bitmask == 1 ) {
			int tilde_pos = in_str.indexOf('~');
			int [] cur_mask = BitShuffleCoder.decode_bitmasks( in_str.substring(tilde_pos+1), nreplication);

			for(int i = 0; i < nreplication; i++) {
				cur_bm[i] = cur_mask[i];
			}


		} else {
			String[] tokens = in_str.split("~");

			for(int i = 0; i < nreplication; i++) {
				cur_bm[i] = Long.parseLong( tokens[i+1], 16 );
			}
		}

		return cur_bm;
	}

	// update HADI bitstring
	public static long[] updateHADIBitString( long [] cur_bm, String in_str, int nreplication, int encode_bitmask ) {
		if(encode_bitmask == 1) {
			int tilde_pos = in_str.indexOf('~');
			int [] cur_mask = BitShuffleCoder.decode_bitmasks( in_str.substring(tilde_pos+1), nreplication);

			for(int i = 0; i < nreplication; i++) {
				cur_bm[i] = (cur_bm[i] | cur_mask[i]);
			}
		} else {
			String[] tokens = in_str.split("~");

			for(int i = 0; i < nreplication; i++) {
				long cur_mask = Long.parseLong( tokens[i+1], 16 );
				cur_bm[i] = (cur_bm[i] | cur_mask);
			}
		}

		return cur_bm;
	}

	// compare two bitstrings.
	// return value : 1 (if the two are different), 0(if they are same)
	public static int isDifferent( long[] left_bm, long[] right_bm, int nreplication ) {
		for(int i = 0; i< nreplication; i++)
			if( left_bm[i] != right_bm[i] )
				return 1;

		return 0;
	}

	// Make a block vector using out_vals[][].
	// The parameter self_bm[][] is used to set the first byte of second component of elements.
	// return value: ArrayList containing elements of the block vector.
	public static ArrayList<VectorElem<String>> makeHADIBitString( long[][] out_vals, int block_width, long [][] self_bm, char []prefix, /*short []radius,*/ String[] saved_rad_nh, int nreplication, int cur_radius, int encode_bitmask) {
		int i, j;
		ArrayList<VectorElem<String>> result_vector = new ArrayList<VectorElem<String>>();

		for(i = 0; i < block_width; i++) {
			String out_str = "";

			int diff = isDifferent(out_vals[i], self_bm[i], nreplication );
			if( diff == 1 ) {	// changed
				if( saved_rad_nh[i] != null && saved_rad_nh[i].length() >= 1 ) {
					int colonPos = saved_rad_nh[i].indexOf(':');
					out_str += ("i" + (cur_radius-1) + HadiUtils.update_radhistory(self_bm[i], saved_rad_nh[i].substring(colonPos+1), cur_radius, nreplication) );//out_str = "i";
				} else
					out_str += ("i" + (cur_radius-1));
			} else {			// unchanged => completed.
				if( prefix[i] == 'i' ) {	// incomplete
					out_str += ("c" + (cur_radius-1)) ;
					if( saved_rad_nh[i] != null ) {
						int colonPos = saved_rad_nh[i].indexOf(':');
						if( colonPos >= 0 )
							out_str += saved_rad_nh[i].substring(colonPos);
					}
				} else						// complete_prefix == 'c' or 'f'
					out_str += "f" + saved_rad_nh[i];	// "f" + saved_radius
			}

			if( encode_bitmask == 1 ) {
				out_str += ( "~" + BitShuffleCoder.encode_bitmasks( out_vals[i], nreplication ) );
			} else {
				for(j = 0; j < nreplication; j++)
					out_str = out_str + "~" + Long.toHexString(out_vals[i][j]) ;
			}

			result_vector.add( new VectorElem<String>((short)i, out_str) );
		}

		return result_vector;
	}

	// Make a block vector using out_vals[][].
	// The parameter self_bm[][] is used to set the first byte of second component of elements.
	// return value: ArrayList containing elements of the block vector.
	public static ArrayList<VectorElem<String>> makeHADICombinerBitString( long[][] out_vals, int block_width, int nreplication, int cur_radius, int encode_bitmask) {
		int i, j;
		ArrayList<VectorElem<String>> result_vector = new ArrayList<VectorElem<String>>();

		for(i = 0; i < block_width; i++) {
			String out_str = "i0:0:1";

			for(j = 0; j < nreplication; j++) {
				if( out_vals[i][j] != 0 )
					break;
			}
			if( j == nreplication )
				continue;

			if( encode_bitmask == 1 ) {
				out_str += ( "~" + BitShuffleCoder.encode_bitmasks( out_vals[i], nreplication ) );
			} else {
				for(j = 0; j < nreplication; j++)
					out_str = out_str + "~" + Long.toHexString(out_vals[i][j]) ;
			}

			result_vector.add( new VectorElem<String>((short)i, out_str) );
		}

		return result_vector;
	}

};

