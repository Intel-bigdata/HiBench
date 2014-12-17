/***********************************************************************
    PEGASUS: Peta-Scale Graph Mining System
    Copyright (c) 2009
    U Kang and Christos Faloutsos
    All Rights Reserved

You may use this code without fee, for educational and research purposes.
Any for-profit use requires written consent of the copyright holders.

-------------------------------------------------------------------------
File: PegasusUtils.java
 - Common utility classes and functions
Version: 0.9
Author Email: U Kang(ukang@cs.cmu.edu), Christos Faloutsos(christos@cs.cmu.edu)
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

// common utility functions
public class MatvecUtils
{
	// convert Vector string to array of VectorElem.
	// strVal is (ROW-ID   VALUE)s. ex) 0 0.5 1 0.3
	public static double[] decodeBlockVector(String strVal, int block_width) 
	{
		int i;
		double [] vector = new double[block_width];
		for(i=0; i< block_width; i++)
			vector[i] = 0;

		//ArrayList arr = new ArrayList<VectorElem>();
		final String[] tokens = strVal.split(" ");


		for(i = 0; i < tokens.length; i += 2) {
			short row = Short.parseShort(tokens[i]);
			double val = Double.parseDouble(tokens[i+1]);
			
			vector[row] = val;
		}

		return vector;
	}


	// convert double[] to String
	// strVal is (ROW-ID   VALUE)s. ex) 0 0.5 1 0.3
	public static String encodeBlockVector(double[] vec, int block_width) 
	{
		int i;

		String result = "";

		for(i=0; i< block_width; i++) {
			if( vec[i] != 0 ) {
				if( result.length() > 0 )
					result += " ";

				result += ("" + i  + " " + vec[i]);
			}
		}

		return result;
	}

}
