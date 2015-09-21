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

import com.intel.hibench.streambench.utils.ConfigLoader;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.ArrayList;

//Data generators are deployed in different nodes and run by launching them near simultaneously in different nodes.
public class StartNew {

	public static void main(String[] args){

        if(args.length < 5){
            System.err.println("args: <ConfigFile> <DATA_FILE1> <DATA_FILE1_OFFSET> <DATA_FILE2> <DATA_FILE2_OFFSET> need to be specified!");
            System.exit(1);
        }

        ConfigLoader cl = new ConfigLoader(args[0]);

        String benchName  = cl.getPropertiy("hibench.streamingbench.benchname").toLowerCase();
        String topic      = cl.getPropertiy("hibench.streamingbench.topic_name");
        String brokerList = cl.getPropertiy("hibench.streamingbench.brokerList");
        String HDFSMaster = cl.getPropertiy("hibench.hdfs.master");
		long totalCount   = Long.parseLong(cl.getPropertiy("hibench.streamingbench.prepare.push.records"));
        String dataFile1        = args[1];
        long dataFile1Offset    = Long.parseLong(args[2]);
        String dataFile2        = args[3];
        long dataFile2Offset    = Long.parseLong(args[4]);

		BufferedReader reader;
        boolean isNumericData = false;
        FileDataGenNew files = new FileDataGenNew(HDFSMaster);

        if(benchName.contains("statistics")){
            isNumericData = true;
            reader = files.loadDataFromFile(dataFile2, dataFile1Offset);
		}else
			reader = files.loadDataFromFile(dataFile1, dataFile2Offset);
		
		NewKafkaConnector con = new NewKafkaConnector(brokerList, cl);
		
		con.publishData(reader, topic, totalCount, isNumericData);
        con.close();
	}
}
