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

import com.intel.hibench.streambench.utils.ConfigLoader;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class NewKafkaConnector {

    KafkaProducer producer;
    private static final int MAXIMUM_NUMERIC_COLUMNS = 2048; // assume maximum dimension of k means data is 2048. Should be large enough.
    private Integer[] NumericData = new Integer[MAXIMUM_NUMERIC_COLUMNS];
    private int Data1Length;

	public NewKafkaConnector(String brokerList, ConfigLoader cl) {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.REQUIRED_ACKS_CONFIG, "1");
        props.setProperty(ProducerConfig.BROKER_LIST_CONFIG, brokerList);
        props.setProperty(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, Integer.toString(5 * 1000));
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_CONFIG, Integer.toString(Integer.MAX_VALUE));
        producer = new KafkaProducer(props);
        Data1Length = Integer.parseInt(cl.getPropertiy("hibench.streamingbench.datagen.data1.length"));
    }
	
	public long publishData(BufferedReader reader, String topic, long size, boolean isNumericData){
        // size=0: read all from reader
        // size>0: read upto size bytes
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

                    if (isNumericData){
                        ous.write(parseNumeric(line).getBytes());
                    } else {
                        ous.write(parseUserVisitTable(line, Data1Length).getBytes());
                    }
                    if ((size>0) && (ous.size()>=size)) { // reach the size threshold, let's sent
                        break;
                    }
                }
                if (ous.size() == 0) break; // no more data got, let's break
                ProducerRecord record = new ProducerRecord(topic, ous.toByteArray());
                bytes += ous.size();
                producer.send(record, callback);
                ous.reset();
                if (size>0) break;  // size=0: endless mode until all datas got; size>0: send maximum size bytes
            }
            ous.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

		long end = System.currentTimeMillis();
		System.out.println("Bytes sent: "+bytes+" after change");
		System.out.println("Time consumed(ms):"+(end-start));
		double seconds=(double)(end-start)/(double)1000;
		double throughput=((double)bytes/seconds)/1000000;
		System.out.println("Throughput: "+throughput+"MB/s");

        return bytes;
	}

    private String parseUserVisitTable(String line, int MaximumLength) {
        // raw uservisit table format:
        // 0	227.209.164.46,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1991-06-10,0.115967035,Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3,YEM,YEM-AR,snowdrops,1
        // 0  	35.143.225.164,nbizrgdziebsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,1996-05-31,0.8792629,Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13,PRT,PRT-PT,fraternally,8
        // 0 	34.57.45.175,nbizrgdziebtsaecsecujfjcqtvnpcnxxwiopmddorcxnlijdizgoi,2001-06-29,0.14202267,Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1),DOM,DOM-ES,Gaborone's,7

        String [] elements = line.split("[\\s,]+");
        StringBuffer sb = new StringBuffer();
        sb.append(elements[0]); sb.append(elements[1]); sb.append(elements[3] + " 00:00:00");
        sb.append(elements[4]); sb.append(elements[2]);

        String result = sb.toString();
        return (result.length() < MaximumLength)?result:result.substring(0, MaximumLength);
    }

    private String parseNumeric(String line) {
        // raw numeric data format:
        //  8     {0:-60.196392992004334,5:620.4421901009101,14:420.4220612785746,13:185.21083185702275,15:483.72692251215295,1:594.7827813502976,3:140.3239790342253,16:3.104707691856035,9:635.8535653005378,19:322.0711157700041,11:87.66295667498484,18:857.7858889856491,17:101.49594891724111,2:921.839749304954,6:697.4655671122938,7:367.3720748762538,8:855.4795500704753,10:564.4074585413068,4:913.7870598326768,12:275.71369666459043}
        //  9     {0:53.780307992655864,5:670.9608085434543,14:427.8278718060577,13:-42.1599560546298,15:509.38987065684455,1:575.0478527061222,3:111.01989708300927,16:48.39876690814693,9:546.0244129369196,19:344.88758399392515,11:35.63727678698427,18:826.8387868256459,17:100.39105575653751,2:972.7568962232599,6:743.3101817500838,7:367.5321255830725,8:897.5852428056947,10:705.1143980643583,4:891.1293114411877,12:364.63401807787426}

        String [] elements = line.split("[{}:,\\s]+");
        int idx = -1;
        int maxidx = -1;
        for (int count=0; count<elements.length; count++ ){
            if (count == 0) continue; //omit first element
            if (count % 2 == 1) idx = Integer.parseInt(elements[count]);
            else {
                int val = (int)Float.parseFloat(elements[count]);
                assert idx >=0: String.format("index value should be greater than zero!, got:%d", idx);
                assert idx < MAXIMUM_NUMERIC_COLUMNS: String.format("index value %d exceed range of %d", idx, MAXIMUM_NUMERIC_COLUMNS);
                NumericData[idx] = val;
                if (maxidx<idx) maxidx = idx;
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i=0; i<NumericData.length; i++){
            int val = NumericData[i];
            sb.append(val + " ");
        }
        String result = sb.toString();
        return result.substring(0, result.length() - 1);
    }

    public void publishData(BufferedReader reader, String topic, boolean isNumericData){
        publishData(reader, topic, 0, isNumericData);
        producer.close();
    }

    public long publishDataSlice(BufferedReader reader, String topic, long size, boolean isNumericData){
        return publishData(reader, topic, size, isNumericData);
    }

	public void close(){
		producer.close();
	}

}
