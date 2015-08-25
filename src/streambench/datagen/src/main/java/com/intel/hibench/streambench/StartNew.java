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

import java.util.ArrayList;

//Data generators are deployed in different nodes and run by launching them near simultaneously in different nodes.
public class StartNew {

	public static void main(String[] args){

        if(args.length < 3){
            System.err.println("args: <ConfigFile> <DATA_FILE1> <<DATA_FILE2> need to be specified!");
            System.exit(1);
        }

        ConfigLoader cl = new ConfigLoader(args[0]);

        String benchName  = cl.getPropertiy("hibench.streamingbench.benchname").toLowerCase();
        String topic      = cl.getPropertiy("hibench.streamingbench.topic_name");
        String brokerList = cl.getPropertiy("hibench.streamingbench.broker_list_with_quote");
		long totalCount   = Long.parseLong(cl.getPropertiy("hibench.streamingbench.prepare.push.records"));
        String dataFile1    = args[2];
        String dataFile2    = args[3];

		ArrayList<byte[]> contents = null;

        if(benchName.contains("statistics")){
			contents = FileDataGenNew.loadDataFromFile(dataFile1);
		}else
			contents = FileDataGenNew.loadDataFromFile(dataFile2);
		
		NewKafkaConnector con = new NewKafkaConnector(brokerList);
		
		con.publishData(contents, totalCount, topic);
	}
	
	
}
