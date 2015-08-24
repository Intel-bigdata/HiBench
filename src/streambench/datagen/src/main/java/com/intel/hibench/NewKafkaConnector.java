package com.intel.hibench.streambench;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.Records;

public class NewKafkaConnector {

	KafkaProducer producer;
	
	public NewKafkaConnector(String brokerList) {
			Properties props = new Properties();
	        props.setProperty(ProducerConfig.REQUIRED_ACKS_CONFIG, "1");
	        props.setProperty(ProducerConfig.BROKER_LIST_CONFIG, brokerList);
	        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
	        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_CONFIG, Integer.toString(Integer.MAX_VALUE));
	        producer = new KafkaProducer(props);
	}
	
	public void publishData(ArrayList<byte[]> contents,long size,String topic){
		int contentsSize=contents.size();
		long start=System.currentTimeMillis();
		long bytes=0;
		
		 Callback callback = new Callback() {
	            public void onCompletion(RecordMetadata metadata, Exception e) {
	                if (e != null)
	                    e.printStackTrace();
	            }
	        };
	    
		for(int i=0;i<size;i++){
			byte[] payload=contents.get((int)i%contentsSize);
			
			//byte[] payloadold=contents.get((int)i%contentsSize).getBytes();
			//Last line is the previous way to get payload and the getBytes method is time consuming,
			//which harms the kafka data publish rate. 
			//Improve by calculates all byte[] in advance
			
			bytes+=payload.length;
			ProducerRecord record = new ProducerRecord(topic, payload);
			producer.send(record,callback);
		}
		long end=System.currentTimeMillis();
		System.out.println("Bytes sent:"+bytes+" after change");
		System.out.println("Time consumed:"+(end-start));
		double seconds=(double)(end-start)/(double)1000;
		double throughput=((double)bytes/seconds)/1000000;
		System.out.println("Throughput:"+throughput+"MB/s");
		producer.close();
	}
	
	public long publishData(ArrayList<byte[]> contents, int start, int size,String topic){
		int contentsSize=contents.size();
		long startTime=System.currentTimeMillis();
		long bytes=0;
		 Callback callback = new Callback() {
	            public void onCompletion(RecordMetadata metadata, Exception e) {
	                if (e != null)
	                    e.printStackTrace();
	            }
	        };
		for(int i=0;i<size;i++){
			byte[] payload = contents.get((i + start) %contentsSize);
			bytes+=payload.length;
			ProducerRecord record = new ProducerRecord(topic, payload);
			producer.send(record,callback);
		}
		
		long endTime=System.currentTimeMillis();
		System.out.println(size+" records sent.  "+"Time consumed:"+(endTime-startTime)+" size:"+bytes);
		return bytes;
	}
	
	public void close(){
		producer.close();
	}
	
	

}
