package com.intel.PRCcloud;

import java.util.ArrayList;

//Data generators are deployed in different nodes and run by lauching them near simultaneously in different nodes.
public class StartNew {

	public static void main(String[] args){
		if(args.length<4){
			System.err.println("args:<BENCHNAME> <TOPICNAME> <BROKER_HOSTS> <RECORD_COUNT> need to be specified");
			System.exit(1);
		}
			
		
		String benchName=args[0].toLowerCase();
		String topic=args[1];
		String brokerList=args[2];
		long totalCount=Long.parseLong(args[3]);
		ArrayList<byte[]> contents=null;

		if(benchName.equals("micro/statistics")){
			contents=FileDataGenNew.loadDataFromFile("test2.data");
		}else
			contents=FileDataGenNew.loadDataFromFile("test1.data");
		
		NewKafkaConnector con=new NewKafkaConnector(brokerList);
		
		con.publishData(contents, totalCount, topic);
	}
	
	
}
