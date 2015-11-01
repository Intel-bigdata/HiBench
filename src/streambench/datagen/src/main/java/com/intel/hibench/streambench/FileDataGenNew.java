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
import java.util.Enumeration;
import java.util.Vector;

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
            InputStreamReader isr;
            if (fs.isDirectory(pt)) { // multiple parts
                isr = new InputStreamReader(OpenMultiplePartsWithOffset(fs, pt, offset));
            } else {  // single file
                FSDataInputStream fileHandler = fs.open(pt);
                if (offset>0) fileHandler.seek(offset);
                isr = new InputStreamReader(fileHandler);
            }

			BufferedReader reader = new BufferedReader(isr);
            if (offset>0) reader.readLine(); // skip first line in case of seek
			return reader;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
        assert false: "Should not reach here!";
        return null;
	}

    private InputStream OpenMultiplePartsWithOffset(FileSystem fs, Path pt, long offset) throws IOException {
        RemoteIterator<LocatedFileStatus> rit = fs.listFiles(pt, false);
        Vector<FSDataInputStream> fileHandleList= new Vector<FSDataInputStream>();
        while (rit.hasNext()) {
            Path path = rit.next().getPath();
            String filename = path.toString().substring(path.getParent().toString().length(), path.toString().length());

            if (filename.startsWith("/part-")) {
                long filesize = fs.getFileStatus(path).getLen();
                if (offset < filesize) {
                    FSDataInputStream handle = fs.open(path);
                    if (offset>0) {
                        handle.seek(offset);
                    }
                    fileHandleList.add(handle);
                }
                offset -= filesize;
            }
        }
        if (fileHandleList.size()==1) return fileHandleList.get(0);
        else if (fileHandleList.size()>1){
            Enumeration<FSDataInputStream> enu = fileHandleList.elements();
            return new SequenceInputStream(enu);
        } else {
            System.err.println("Error, no source file loaded. run genSeedDataset.sh fisrt!");
            return null;
        }
    }
}
