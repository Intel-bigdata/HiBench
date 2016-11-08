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

package com.intel.hibench.streambench.storm.metrics;

import com.intel.hibench.streambench.storm.util.*;

public class Reporter implements Runnable{
	private String nimbus;
	private int port;
	private String benchName;
	private long recordCount;
	private int interval;
	
	public Reporter(String nimbus,int port,String benchName,long recordCount,int interval){
	  this.nimbus=nimbus;
	  this.port=port;
	  this.benchName=benchName;
	  this.recordCount=recordCount;
	  this.interval=interval;
	}
	
	public void run(){
		StatFacade statsUtil=new StatFacade(nimbus,port,benchName,interval);
		
		//Get throughput		
		double runtime=statsUtil.getRunTimeInSec();
		if(runtime==0){
			BenchLogUtil.handleError("Runtime is less than collect time!");
		}
		double throughput=(double)recordCount/runtime;
		System.out.println("Runtime is: "+runtime+"  throughput is: "+throughput);
		//Get latency. Currently the metric is from Storm UI
	}

	//public static void main(String[] args){
		//run(new String[]{"sr119","6627","microbench_sketch","1000000"});
	//}
	
	
}
