package com.intel.PRCcloud.metrics;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.intel.PRCcloud.thrift.generated.ClusterSummary;
import com.intel.PRCcloud.thrift.generated.ExecutorSummary;
import com.intel.PRCcloud.thrift.generated.Nimbus;
import com.intel.PRCcloud.thrift.generated.NotAliveException;
import com.intel.PRCcloud.thrift.generated.TopologyInfo;
import com.intel.PRCcloud.thrift.generated.GlobalStreamId;
import com.intel.PRCcloud.thrift.generated.TopologySummary;

public class StatFacade {
	Nimbus.Client client;
	boolean finished=false;
	String topologyId;
	long finishedTime=0;
	Object mutex=new Object();
	long startTime=0;
	
	int COLLECT_INTERVAL=3000; //3s

	public StatFacade(String host,int port,String topologyName,int interval){
		COLLECT_INTERVAL=interval*1000;
		TSocket tsocket = new TSocket(host, port);
		TFramedTransport tTransport = new TFramedTransport(tsocket);
		TBinaryProtocol tBinaryProtocol = new TBinaryProtocol(tTransport);
		client = new Nimbus.Client(tBinaryProtocol);
		try {
			tTransport.open();
			topologyId=getTopologyId(topologyName);
		} catch (TTransportException e) {
			e.printStackTrace();
		}
	}
	
	private String getTopologyId(String topoName){
		ClusterSummary clusterSummary;
		try {
			clusterSummary = client.getClusterInfo();
			List<TopologySummary> summary=clusterSummary.getTopologies();
			for(TopologySummary topo:summary){
				if(topo.getName().equals(topoName))
					return topo.getId();
			}
		} catch (TException e) {
			e.printStackTrace();
		}
		return  null;
	}
	
	//The logic: if the processed tuples count remains same in 5 seconds, then the bench is finished
	private void waitToFinish(){
		Timer timer=new Timer();
		timer.schedule(new CheckTask(timer), 0 ,COLLECT_INTERVAL);
		try {
		 synchronized(mutex){
			mutex.wait();
		 }
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		finished=true;
	}
	
	private long prevTransfer=0;
	class CheckTask extends TimerTask{
		Timer timer;
		public CheckTask(Timer timer){
			this.timer=timer;
		}
		
		public void run(){
			long curTransfer=getAckedSize();
			
			System.out.println("Prev transfer: "+prevTransfer+" curTransfer: "+curTransfer);
			//If no data transfered during the period, then the benchmark finishes
			if(curTransfer==prevTransfer && curTransfer!=0){
				synchronized(mutex){
					mutex.notify();
				}
				
				finishedTime=System.currentTimeMillis()-COLLECT_INTERVAL-COLLECT_INTERVAL/2;  //Last interval has ended
				timer.cancel();
				timer.purge();				
			}else if(prevTransfer==0&&curTransfer!=0){   //This marks the beginning
				prevTransfer=curTransfer;
			    startTime=System.currentTimeMillis()-COLLECT_INTERVAL/2;  //An average modify of the start
			}else{
				prevTransfer=curTransfer;
			}
		}
	}
	
	private long getTransferSize(){
		try {
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			long transfferedAll=0;
			for(ExecutorSummary executorInfo:topologyInfo.executors){
				if(executorInfo==null || executorInfo.stats==null) return 0;
				Map<String,Map<String,Long>> sent=executorInfo.stats.transferred;
				if(sent==null) return 0;
				Set<Map.Entry<String,Long>> transferSet10min=sent.get(":all-time").entrySet();
			    for(Map.Entry<String, Long> subEntry:transferSet10min){
			    	transfferedAll+=subEntry.getValue();
				}
			}
			return transfferedAll;
		} catch (NotAliveException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	private long getAckedSize(){
		try {
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			long ackTotal=0;
			for(ExecutorSummary executorInfo:topologyInfo.executors){
			    if(executorInfo==null || executorInfo.stats==null) return 0;
                            String id = executorInfo.component_id;
			    if(id.equals("spout") || id.equals("$mastercoord-bg0") || id.equals("$spoutcoord-spout0") || id.equals("spout0") || id.equals("__acker"))
			      continue;
			    if(executorInfo.stats.specific == null) return 0;
			    if(executorInfo.stats.specific.getBolt() == null) return 0;
			    Map<String, Map<GlobalStreamId, Long>> acked = executorInfo.stats.specific.getBolt().acked;
			    Map<GlobalStreamId, Long> map = acked.get(":all-time");
			    if(map == null) return 0;
			    Set<Map.Entry<GlobalStreamId, Long>> allTimeAck = map.entrySet();
			    for(Map.Entry<GlobalStreamId, Long> subEntry:allTimeAck){
			      if (id.contains("b-")) {
				String ikey = subEntry.getKey().toString();
			        if (!ikey.contains("streamId:s")) {
			          continue;
				}
			      }
			      ackTotal += subEntry.getValue();
			    }
			}
			return ackTotal;
		} catch (NotAliveException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	private int getTopologyUpTime(){
		try {
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			return topologyInfo.getUptime_secs();
		} catch (NotAliveException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public void checkFinished(){
		if(!finished)
			waitToFinish();
	}
	
	//This shall be called first to ensure the benchmark finishes
	public double getRunTimeInSec(){
		checkFinished();
		double time=(double)(finishedTime-startTime)/(double)1000;
		//If data is consumed in one interval, in average we estimate the consumption time to be half of the interval
		return time>0?time:((double)COLLECT_INTERVAL)/((double)2000);
	}
	
//	public long getAvgLatencySpout(){
//		
//	}
//	
//	public long getAvgLatencyProcessBolt(){
//		
//	}
//	
//	public long getAvgLatencyExecuteBolt(){
//		
//	}
//	
//	public long getAvgLatency(){
//		
//	}
}
