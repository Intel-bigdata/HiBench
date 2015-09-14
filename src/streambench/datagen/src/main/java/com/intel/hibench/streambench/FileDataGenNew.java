/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class FileDataGenNew {
    Configuration fsConf = new Configuration();

    FileDataGenNew (String HDFSMaster){
        fsConf.set("fs.default.name", HDFSMaster);
    }

	public BufferedReader loadDataFromFile(String filepath, long offset){
		try {
            Path pt = new Path(filepath);
            FileSystem fs = FileSystem.get(fsConf);
            FSDataInputStream fileHandler = fs.open(pt);
            if (offset>0) fileHandler.seek(offset);
			BufferedReader reader = new BufferedReader(new InputStreamReader(fileHandler));
            if (offset>0) reader.readLine();
			return reader;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
