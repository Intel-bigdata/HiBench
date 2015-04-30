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
File: NormalizeVector.java
 - Normalize a vector so that the sum of the elements is 1.
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

public class NormalizeVector extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path input_path = null;
	protected Path output_path = null;
	protected int nreducers = 1;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new NormalizeVector(), args);

		System.exit(result);
    }


    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("NormalizeVector <input vector_path> <output_path> <# of reducers> <additional multiplier>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 4 ) {
			return printUsage();
		}

		int i;
		input_path = new Path(args[0]);
		output_path = new Path(args[1]);				
		nreducers = Integer.parseInt(args[2]);
		double additional_multiplier = Double.parseDouble(args[3]);

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Normalizing a vector. input_path=" + args[0] + ", output_path=" + args[1] + "\n");

		final FileSystem fs = FileSystem.get(getConf());
		FileSystem lfs = FileSystem.getLocal(getConf());

		// compute l1 norm
		String[] new_args = new String[1];
		new_args[0] = args[0];

		ToolRunner.run(getConf(), new L1norm(), new_args);
		double scalar = PegasusUtils.read_l1norm_result(getConf());
		lfs.delete(new Path("l1norm"), true);

		System.out.println("L1norm = " + scalar );

		// multiply by scalar
		new_args = new String[2];
		new_args[0] = args[0];
		new_args[1] = new String("" + additional_multiplier/scalar);
		ToolRunner.run(getConf(), new ScalarMult(), new_args);

		fs.delete(output_path, true);
		fs.rename(new Path("smult_output"), output_path );

		System.out.println("\n[PEGASUS] Normalization completed. The normalized vecotr is saved in HDFS " + args[1] + ".\n");

		return 0;
    }
}

