package com.intel.PRCcloud;

import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;


public class StartPeriodic {

	public static void main(String[] args){
		if(args.length<6){
			System.err.println("args:<BENCHNAME> <TOPICNAME> <BROKER_HOSTS> <RECORD_COUNT_PERINTEVAL>"
					+ "<INTERVAL_SPAN> <TOTAL_ROUND>  need to be specified");
			System.exit(1);
		}
		
		String benchName=args[0].toLowerCase();
		String topic=args[1];
		String brokerList=args[2];
		int recordPerInterval=Integer.parseInt(args[3]);
		int intervalSpan=Integer.parseInt(args[4]);
		int totalRound=Integer.parseInt(args[5]);
		
		ArrayList<byte[]> contents=null;
		
		if(benchName.equals("micro/statistics")){
			contents=FileDataGenNew.loadDataFromFile("test2.data");
		}else
			contents=FileDataGenNew.loadDataFromFile("test1.data");
		
		NewKafkaConnector con=new NewKafkaConnector(brokerList);
		
		Timer timer=new Timer();
		timer.schedule(new SendTask(totalRound,recordPerInterval,con,contents,topic), 0,intervalSpan);
	}
	
	static class SendTask extends TimerTask{
		int leftTimes;
		int recordCount;
		int totalTimes;
		NewKafkaConnector kafkaCon;
		ArrayList<byte[]> contents;
		String topic;
		long totalBytes;
		
		public SendTask(int times,int count,NewKafkaConnector con,ArrayList<byte[]> contents,String topic){
			leftTimes=times;
			recordCount=count;
			totalTimes=times;
			kafkaCon=con;
			this.contents=contents;
			this.topic=topic;
			totalBytes=0;
		}
		@Override
		public void run() {
			if(leftTimes>0){
				long thisSize=kafkaCon.publishData(contents, (totalTimes-leftTimes)*recordCount, recordCount, topic);
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
