/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hibench;
import java.io.IOException;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class DataOptions {

	public static enum DataType {
		HIVE, PAGERANK, BAYES, NUTCH, NONE
	}

//	public int agents;
	public int maps;
	public int reds;
	public long pages;
	public long words;
	public long visits;
	public DataType type;

	public static double lratio = 0.1;
	public static double wratio = 0.1;

	public static double LINK_ZIPF_EXPONENT = 0.5;
	public static double WORD_ZIPF_EXPONENT = 0.9;

	public String base = "hidata";
	public String dname = null;

	public static String DELIMITER = ",";
	public static String JOIN_TAG = "!";

	public static boolean KEEP_ORDER = false;
	public static boolean SKIP_WORDS = true;
	public static boolean SKIP_LINKS = false;

	public boolean OPEN_DEBUG = true;
	public boolean SEQUENCE_OUT = false;
	public boolean COMPRESS = false;
	
	public boolean PAGERANK_NODE_BALANCE = true;

	Class<? extends CompressionCodec> codecClass = null;

	DataOptions (String[] args) throws Exception {
		maps = -1;
		reds = -1;
//		agents = -1;
		pages = -1;
		words = -1;
		visits = -1;
		type = DataType.NONE;

		parseOptions(args);
	}

	public static int getMaxNumReduce () throws IOException {
		JobConf job = new JobConf(WebDataGen.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		int maxReduce = cluster.getMaxReduceTasks();
		int runnings = cluster.getReduceTasks();
		return maxReduce - runnings;
	}
	
	public static int getMaxNumMap () throws IOException {
		JobConf job = new JobConf(WebDataGen.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		return cluster.getMaxMapTasks();
/*		
		int maxMap = cluster.getMaxMapTasks();
		int runnings = cluster.getMapTasks();
		return maxMap - runnings;
*/
	}
	
	public String defaultDataName () {
		switch (type) {
			case HIVE:		return "hive";
			case PAGERANK:	return "pagerank";
			case BAYES:		return "bayes";
			case NUTCH:		return "nutch";
			default:
				return null;
		}
	}

	public boolean parseHiveOptions (String[] args) throws Exception {
		type = DataType.HIVE;
		dname = "hive";
		KEEP_ORDER = false;
		SKIP_WORDS = true;
		SKIP_LINKS = false;
		for (int i=2; i<args.length; i++) 
		{
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = Long.parseLong(args[++i]);
			} else if ("-v".equals(args[i])) {
				visits = Long.parseLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-d".equals(args[i])) {
				DELIMITER = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					SEQUENCE_OUT = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else {
				return false;
			}
		}
		
		if (pages<=0 || visits<=0) {
			return false;
		}

		return true;
	}
	
	public boolean parsePageRankOptions (String[] args) throws Exception {
		type = DataType.PAGERANK;
		dname = "pagerank";
		KEEP_ORDER = false;
		SKIP_WORDS = true;
		SKIP_LINKS = false;
		for (int i=2; i<args.length; i++) 
		{
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = Long.parseLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-d".equals(args[i])) {
				DELIMITER = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					SEQUENCE_OUT = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else {
				return false;
			}
		}
		
		if (pages<=0) {
			return false;
		}
		return true;
	}
	
	public boolean parseBayesOptions (String[] args) throws Exception {
		type = DataType.BAYES;
		dname = "bayes";
		KEEP_ORDER = false;
		SKIP_WORDS = false;
		SKIP_LINKS = true;
		for (int i=2; i<args.length; i++) 
		{
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = Long.parseLong(args[++i]);
			} else if ("-w".equals(args[i])) {
				words = Long.parseLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-d".equals(args[i])) {
				DELIMITER = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					SEQUENCE_OUT = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else {
				return false;
			}
		}
		
		if (pages<=0 || words<=0) {
			return false;
		}
		return true;
	}

	public boolean parseNutchOptions (String[] args) throws Exception {
		type = DataType.NUTCH;
		dname = "nutch";
		KEEP_ORDER = false;
		SKIP_WORDS = false;
		SKIP_LINKS = false;
		for (int i=2; i<args.length; i++) 
		{
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = Long.parseLong(args[++i]);
			} else if ("-w".equals(args[i])) {
				words = Long.parseLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-d".equals(args[i])) {
				DELIMITER = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					SEQUENCE_OUT = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else {
				return false;
			}
		}

		if (pages<=0 || words<=0) {
			return false;
		}
		return true;
	}
	
	public void parseOptions (String[] args) throws Exception {
		if (args.length < 2) {
			System.exit(printUsage());
		}

		boolean pass = false;
		if ("-u".equals(args[0])) {
			if ("hive".equalsIgnoreCase(args[1])) {
				pass = parseHiveOptions(args);
			} else if ("pagerank".equalsIgnoreCase(args[1])) {
				pass = parsePageRankOptions(args);
			} else if ("bayes".equalsIgnoreCase(args[1])) {
				pass = parseBayesOptions(args);
			} else if ("nutch".equalsIgnoreCase(args[1])) {
				pass = parseNutchOptions(args);
			}
		}
		if (!pass) {
			System.exit(printUsage());
		}
		
		if (maps<=0) {
			maps = getMaxNumMap();
			if (0==maps) {
				System.out.println("No map slot left for data preparation");
				System.exit(-1);
			}
		}
	}

	public int printUsage () {
		System.out.println("WebDataGen -u hive -p <pages> -v <visits> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>] [-d <delimiter>]");
		System.out.println("WebDataGen -u pagerank -p <pages> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>] [-d <delimiter>]");
/*
		System.out.println("WebDataGen -u bayes [-b <base path>] [-n <data name>] "
				+ "-a <agents> -p <pages> -w <words> " +
				"[-o sequence] [-c <codec>] [-d <delimiter>]");
		System.out.println("WebDataGen -u hive [-b <base path>] [-n <data name>] "
				+ "-a <agents> -p <pages> -w <words> " +
				"[-o sequence] [-c <codec>] [-d <delimiter>]");
*/
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}
}
