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

import com.intel.hibench.streambench.common.ConfigLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Syncable;
import org.apache.kafka.clients.producer.*;

/**
 * KafkaSender hold an kafka producer. It gets content from input parameter, generates records and
 * sends records to kafka.
 */
public class KafkaSender {
  String sourcePath;
  Configuration dfsConf;
  KafkaProducer producer;

  // offset of file input stream. Currently it's fixed, which means same records will be sent
  // out on very batch.
  long offset;

  // constructor
  public KafkaSender(String sourcePath, long startOffset, ConfigLoader configLoader) {
    String brokerList = configLoader.getProperty("hibench.streamingbench.brokerList");
    String dfsMaster = configLoader.getProperty("hibench.hdfs.master");

    // Details of KafkaProducerConfig could be find from:
    //   http://kafka.apache.org/documentation.html#producerconfigs
    Properties props = new Properties();
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.setProperty(ProducerConfig.ACKS_CONFIG, "1");
    props.getProperty(ProducerConfig.CLIENT_ID_CONFIG, "hibench_data_generator");

    this.producer = new KafkaProducer(props);
    this.sourcePath = sourcePath;
    this.dfsConf = new Configuration();
    dfsConf.set("fs.default.name", dfsMaster);
    this.offset = startOffset;
  }

  // The callback function will be triggered when receive ack from kafka.
  // Print error message if exception exist.
  Callback callback = new Callback() {
    public void onCompletion(RecordMetadata metadata, Exception e) {
      if (e != null)
        e.printStackTrace();
    }
  };

  // send content to Kafka
  public long send (String topic, long totalRecords) {
    long sentRecords = 0L;
    BufferedReader reader = SourceFileReader.getReader(dfsConf, sourcePath, offset);

    try {
      while (sentRecords < totalRecords) {
        String line = reader.readLine();
        if (line == null) {
          break; // no more data from source files
        }
        String currentTime = Long.toString(System.currentTimeMillis());
        ProducerRecord record = new ProducerRecord(topic, currentTime, line);
        producer.send(record, callback);

        //update counter
        sentRecords ++;
      }
    } catch (IOException e) {
      System.err.println("Failed read records from Path: " + sourcePath);
      e.printStackTrace();
    }
    System.out.println("sent " + sentRecords + " records to Kafka topic: " + topic);
    return sentRecords;
  }

  // close kafka producer
  public void close() {
    producer.close();
  }
}
