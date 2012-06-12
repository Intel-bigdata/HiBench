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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DataPaths {

	public String base = "hidata";
	public String dname = null;
	public Path working = null;
	public Path result = null;

	public static final String DUMMY = "dummy";
	public static final String LINKS = "links";
	public static final String WORDS = "words";

	public static final String T_LINK_PAGE = "t_link_page";
	public static final String T_WORD_PAGE = "t_word_page";
	
	public static final String T_PAGE_ZLINK = "t_page_zlink";
	public static final String T_PAGE_ZWORD = "t_page_zword";
	
	public static final String T_PAGE_TLINK = "t_page_tlink";
	public static final String T_PAGE_TWORD = "t_page_tword";

	public static final String ZLINK_SUM = "zlink_sum";
	public static final String ZWORD_SUM = "zword_sum";

	public static final String ZLINK_DIST = "zlink_dist";
	public static final String ZWORD_DIST = "zword_dist";

	// Hive specific
	public static final String RANKINGS = "rankings";
	public static final String USERVISITS = "uservisits";
	public static final String uagentf = "user_agents";
	public static final String countryf = "country_codes";
	public static final String searchkeyf = "search_keys";

	// Bayes specific
	public static final String TRAINING_SET = "training_set";

	// Pagerank specific
	public static final String VERTICALS = "vertices";
	public static final String EDGES = "edges";	

	// Nutch specific
	public static final String LINKDB = "linkdb";
	public static final String KEYDB = "keydb";	
	public static final String METADB = "metadb";	

	DataPaths (DataOptions options) {
		base = options.base;
		dname = options.dname;
		
		result = new Path(base + "/" + dname);
		working = new Path(result, "working");
	}

	public Path getPath(String file) {
		return new Path(working, file);
	}

	public Path getResult(String file) {
		return new Path(result, file);
	}

	public static void checkHdfsFile(Path path, boolean mkdir)
			throws IOException {

		FileSystem fs = path.getFileSystem(new Configuration());
		
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		
		if (mkdir) {
			fs.mkdirs(path);
		}
		fs.close();
	}

	public static void moveFilesToParent(Path src) throws IOException {
		FileSystem fs = src.getFileSystem(new Configuration());
		Path parent = src.getParent();
		
		FileStatus[] flist = fs.listStatus(src);
		for (FileStatus file : flist) {
			if (null != file) {
				fs.rename(file.getPath(), new Path(parent, file.getPath().getName()));
			}
		}
		fs.delete(src, true);
		fs.close();
	}

	public void cleanWorkDir() throws IOException {
		FileSystem fs = working.getFileSystem(new Configuration());
		fs.delete(working, true);
		fs.close();
	}
	
	public void cleanTempFiles (Path file) throws IOException {
		Path ftemp = new Path(file, "_logs");
		FileSystem fs = ftemp.getFileSystem(new Configuration());
		fs.delete(ftemp, true);
		fs.close();
	}
}
