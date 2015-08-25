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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

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
	
	public void publishData(BufferedReader reader,long size,String topic){
		long start = System.currentTimeMillis();
		long bytes = 0;
		
		 Callback callback = new Callback() {
             public void onCompletion(RecordMetadata metadata, Exception e) {
                 if (e != null)
                     e.printStackTrace();
             }
         };
        ByteArrayOutputStream ous = new ByteArrayOutputStream();
        try {
            while (true) {
                for (int i=0; i<1000; i++) {  // read and accumulate 1000 lines top
                    String line = reader.readLine();
                    if (line == null) break;
                    ous.write(line.getBytes());
                }
                if (ous.size() == 0) break; // no more data got, let's break
                ProducerRecord record = new ProducerRecord(topic, ous.toByteArray());
                bytes += ous.size();
                producer.send(record, callback);
                ous.reset();
            }
            ous.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

		long end = System.currentTimeMillis();
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
