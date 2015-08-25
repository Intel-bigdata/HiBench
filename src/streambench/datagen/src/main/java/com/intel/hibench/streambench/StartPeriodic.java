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
import java.util.Timer;
import java.util.TimerTask;


public class StartPeriodic {

	public static void main(String[] args){

        if (args.length < 3){
            System.err.println("args: <ConfigFile> <DATA_FILE1> <<DATA_FILE2> need to be specified!");
            System.exit(1);
        }

        ConfigLoader cl = new ConfigLoader(args[0]);

		String benchName  = cl.getPropertiy("hibench.streamingbench.benchname").toLowerCase();
		String topic      = cl.getPropertiy("hibench.streamingbench.topic_name");
		String brokerList = cl.getPropertiy("hibench.streamingbench.broker_list_with_quote");
		int recordPerInterval = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.prepare.periodic.recordPerInterval"));
		int intervalSpan = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.prepare.periodic.intervalSpan"));
		int totalRound   = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.prepare.periodic.totalRound"));
        String dataFile1    = args[1];
        String dataFile2    = args[2];

        Reader reader = null;

        if(benchName.contains("statistics")){
            reader = FileDataGenNew.loadDataFromFile(dataFile1);
        }else
            reader = FileDataGenNew.loadDataFromFile(dataFile2);

        NewKafkaConnector con=new NewKafkaConnector(brokerList);
		
		Timer timer=new Timer();
		timer.schedule(new SendTask(totalRound, recordPerInterval, con, reader, topic), 0,intervalSpan);
	}
	
	static class SendTask extends TimerTask{
		int leftTimes;
		int recordCount;
		int totalTimes;
		NewKafkaConnector kafkaCon;
		BufferedReader reader;
		String topic;
		long totalBytes;
		
		public SendTask(int times,int count,NewKafkaConnector con, BufferedReader reader,String topic){
			leftTimes=times;
			recordCount=count;
			totalTimes=times;
			kafkaCon=con;
			this.reader = reader;
			this.topic=topic;
			totalBytes=0;
		}
		@Override
		public void run() {
			if(leftTimes>0){
				long thisSize=kafkaCon.publishData(reader, (totalTimes-leftTimes)*recordCount, recordCount, topic);
				totalBytes += thisSize;
				leftTimes--;
			}else{
				System.out.println("Time's up! Total bytes sent:"+totalBytes);
				kafkaCon.close();
				System.exit(0);
			}
		}
	}
	
	
}
