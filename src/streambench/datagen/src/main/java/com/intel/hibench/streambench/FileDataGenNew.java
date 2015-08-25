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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;

public class FileDataGenNew {

	
	public static ArrayList<byte[]> loadDataFromFile(String filepath){
		ArrayList<byte[]> contents=new ArrayList<byte[]>();
		try {
            Path pt = new Path(filepath);
            FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader reader=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while((line = reader.readLine())!=null){
				contents.add(line.getBytes());
			}
			reader.close();
			return contents;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
