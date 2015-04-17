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
File: MatvecPrep.java
 - convert matrix(edges) or vectors into block form. 
   This program is used for converting data to be used in the block version of HADI, HCC, and PageRank.
Version: 2.0
***********************************************************************/

package pegasus.matvec;

import java.io.*;
import java.util.*;
import java.text.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class MatvecPrep extends Configured implements Tool 
{
    //////////////////////////////////////////////////////////////////////
    // STAGE 1: convert vectors and edges to block format
	//		(a) (vector)  ROWID		vVALUE    =>    BLOCKID	IN-BLOCK-INDEX VALUE
	//      (b) (real matrix)  ROWID		COLID		VALUE    
	//            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
	//      (c) (0-1 matrix)  ROWID		COLID
	//            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
	//////////////////////////////////////////////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
    {
		int block_size;
		int matrix_row;
		int makesym;

		public void configure(JobConf job) {
			block_size = Integer.parseInt(job.get("block_size"));
			matrix_row = Integer.parseInt(job.get("matrix_row"));
			makesym = Integer.parseInt(job.get("makesym"));

			System.out.println("MapStage1: block_size = " + block_size + ", matrix_row=" + matrix_row + ", makesym = " + makesym);
		}

		public void map (final LongWritable key, final Text value, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException
		{
			String line_text = value.toString();
			if (line_text.startsWith("#"))				// ignore comments in edge file
				return;

			final String[] line = line_text.split("\t");
			if(line.length < 2 )
				return;

			if( line[1].charAt(0) == 'v') {
				// (vector)  ROWID		vVALUE    =>    BLOCKID	IN-BLOCK-INDEX VALUE
				int row_id = Integer.parseInt(line[0]);
				int block_id = row_id / block_size;
				int in_block_index = row_id % block_size;

				output.collect( new Text("" + block_id), new Text("" + in_block_index + " " + line[1].substring(1)) );
			} else {
				int row_id = Integer.parseInt(line[0]);
				int col_id = Integer.parseInt(line[1]);
				int block_rowid = row_id / block_size;
				int block_colid = col_id / block_size;
				int in_block_row = col_id % block_size;	// trick : transpose
				int in_block_col = row_id % block_size; // trick : transpose

				if( line.length == 3 ) {
					//      (real matrix)  ROWID		COLID		VALUE    
					//            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL VALUE
					String elem_val;
					if(line[2].charAt(0) == 'v')
						 elem_val = line[2].substring(1);
					else
						elem_val = line[2];

					output.collect( new Text("" + block_rowid + "\t" + block_colid), new Text("" + in_block_row + " " + in_block_col + " " + line[2]) );
				} else {
					//      (0-1 matrix)  ROWID		COLID		
					//            =>  BLOCK-ROW		BLOCK-COL		IN-BLOCK-ROW IN-BLOCK-COL
					output.collect( new Text("" + block_rowid + "\t" + block_colid), new Text("" + in_block_row + " " + in_block_col) );

					if( makesym == 1 )	// output transposed entry
						output.collect( new Text("" + block_colid + "\t" + block_rowid), new Text("" + in_block_col + " " + in_block_row) );
				}
			}
		}

	}


	static class MvPrepComparator implements Comparator {   
		public int compare(Object o1, Object o2) {
			String s1 = o1.toString();   
			String s2 = o2.toString();   

			int pos1 = s1.indexOf(' ');
			int pos2 = s2.indexOf(' ');

			int val1 = Integer.parseInt(s1.substring(0,pos1));
			int val2 = Integer.parseInt(s2.substring(0,pos2));

			return (val1-val2);
		}

		public boolean equals(Object o1, Object o2) {
			String s1 = o1.toString();   
			String s2 = o2.toString();   

			int pos1 = s1.indexOf(' ');
			int pos2 = s2.indexOf(' ');

			int val1 = Integer.parseInt(s1.substring(0,pos1));
			int val2 = Integer.parseInt(s2.substring(0,pos2));

			if( val1 == val2 )
				return true;
			else
				return false;
		}   
	}   

    public static class RedStage1 extends MapReduceBase	implements Reducer<Text, Text, Text, Text>
    {
		String out_prefix = "";
		MvPrepComparator mpc = new MvPrepComparator();

		public void configure(JobConf job) {
			out_prefix = job.get("out_prefix");

			System.out.println("RedStage1: out_prefix = " + out_prefix);
		}

		public void reduce (final Text key, final Iterator<Text> values, final OutputCollector<Text, Text> output, final Reporter reporter) throws IOException
        {
			String out_value = "";
			ArrayList<String> value_al = new ArrayList<String>();

			while (values.hasNext()) {
				// vector: key=BLOCKID, value= IN-BLOCK-INDEX VALUE
				// matrix: key=BLOCK-ROW		BLOCK-COL, value=IN-BLOCK-ROW IN-BLOCK-COL VALUE

				String value_text = values.next().toString();
				value_al.add( value_text );
			}

			Collections.sort(value_al, mpc );

			Iterator<String> iter = value_al.iterator();
			while( iter.hasNext() ){
				String cur_val = iter.next();

				if( out_value.length() != 0 )
					out_value += " ";
				out_value += cur_val;
			}

			value_al.clear();

			if( out_prefix != null )
				output.collect(key, new Text(out_prefix + out_value));
			else
				output.collect(key, new Text(out_value));
		}
    }

    //////////////////////////////////////////////////////////////////////
    // command line interface
    //////////////////////////////////////////////////////////////////////
    protected Path edge_path = null;
	protected Path output_path = null;
	protected int number_nodes = 0;
	protected int block_size = 1;
	protected int nreducer = 1;
	protected String output_prefix;
	protected int makesym = 0;

    // Main entry point.
    public static void main (final String[] args) throws Exception
    {
		final int result = ToolRunner.run(new Configuration(), new MatvecPrep(), args);

		System.exit(result);
    }

    // Print the command-line usage text.
    protected static int printUsage ()
    {
		System.out.println("MatvecPrep <edge_path> <outputedge_path> <# of row> <block width> <# of reducer> <out_prefix or null> <makesym or nosym>");

		ToolRunner.printGenericCommandUsage(System.out);

		return -1;
    }

	// submit the map/reduce job.
    public int run (final String[] args) throws Exception
    {
		if( args.length != 7 ) {
			return printUsage();
		}

		edge_path = new Path(args[0]);
		output_path = new Path(args[1]);				
		number_nodes = Integer.parseInt(args[2]);	// number of row of matrix
		block_size = Integer.parseInt(args[3]);
		nreducer = Integer.parseInt(args[4]);

		if( args[5].compareTo("null") == 0 )
			output_prefix = "";
		else
			output_prefix = args[5];

		if( args[6].compareTo("makesym") == 0 )
			makesym = 1;
		else
			makesym = 0;

		System.out.println("\n-----===[PEGASUS: A Peta-Scale Graph Mining System]===-----\n");
		System.out.println("[PEGASUS] Converting the adjacency matrix to block format. Output_prefix = " + output_prefix + ", makesym = " + makesym + ", block width=" + block_size + "\n");

		// run job
		JobClient.runJob(configStage1(output_prefix));

		System.out.println("\n[PEGASUS] Conversion finished.");
		System.out.println("[PEGASUS] Block adjacency matrix is saved in the HDFS " + args[1] + "\n");


		return 0;
    }

	// Configure pass1
    protected JobConf configStage1 (String out_prefix) throws Exception
    {
		final JobConf conf = new JobConf(getConf(), MatvecPrep.class);
		conf.set("block_size", "" + block_size);
		conf.set("matrix_row", "" + number_nodes);
		conf.set("out_prefix", "" + out_prefix);
		conf.set("makesym", "" + makesym);
		conf.setJobName("MatvecPrep_Stage1");
		
		conf.setMapperClass(MapStage1.class);        
		conf.setReducerClass(RedStage1.class);

		FileSystem fs = FileSystem.get(getConf());
		fs.delete(output_path, true);

		FileInputFormat.setInputPaths(conf, edge_path);  
		FileOutputFormat.setOutputPath(conf, output_path);  

		int num_reduce_tasks = nreducer;

		conf.setNumReduceTasks( num_reduce_tasks );

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		return conf;
    }
}

