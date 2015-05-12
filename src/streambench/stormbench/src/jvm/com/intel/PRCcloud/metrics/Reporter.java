package com.intel.PRCcloud.metrics;

import com.intel.PRCcloud.util.*;

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
