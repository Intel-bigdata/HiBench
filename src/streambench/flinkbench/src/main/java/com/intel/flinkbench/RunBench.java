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

package com.intel.flinkbench;

import com.intel.flinkbench.microbench.*;
import com.intel.flinkbench.util.BenchLogUtil;
import com.intel.hibench.streambench.common.ConfigLoader;
import com.intel.flinkbench.util.FlinkBenchConfig;
import com.intel.hibench.streambench.common.metrics.MetricsUtil;
import com.intel.hibench.streambench.common.metrics.KafkaReporter;
import com.intel.hibench.streambench.common.StreamBenchConfig;

import com.intel.hibench.streambench.common.Platform;

public class RunBench {
    public static void main(String[] args) throws Exception {
        runAll(args);
    }

    public static void runAll(String[] args) throws Exception {

        if (args.length < 1)
            BenchLogUtil.handleError("Usage: RunBench <ConfigFile>");

        ConfigLoader cl = new ConfigLoader(args[0]);

        // Prepare configuration
        FlinkBenchConfig conf = new FlinkBenchConfig();
        conf.brokerList = cl.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST);
        conf.zkHost = cl.getProperty(StreamBenchConfig.ZK_HOST);
        conf.testCase = cl.getProperty(StreamBenchConfig.TESTCASE);
        conf.topic = cl.getProperty(StreamBenchConfig.KAFKA_TOPIC);
        conf.consumerGroup = cl.getProperty(StreamBenchConfig.CONSUMER_GROUP);
        conf.bufferTimeout = Long.parseLong(cl.getProperty(StreamBenchConfig.FLINK_BUFFERTIMEOUT));
        conf.offsetReset = cl.getProperty(StreamBenchConfig.KAFKA_OFFSET_RESET);

        long recordsPerInterval = Long.parseLong(cl.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL));
        int intervalSpan = Integer.parseInt(cl.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN));
        conf.reportTopic = MetricsUtil.getTopic(Platform.FLINK, conf.testCase, recordsPerInterval, intervalSpan);

        // Main testcase logic
        String testCase = conf.testCase;

        if (testCase.equals("wordcount")) {
            conf.separator = "\\s+";
            WordCount wordCount = new WordCount();
            wordCount.processStream(conf);
        } else if (testCase.equals("identity")) {
            Identity identity = new Identity();
            identity.processStream(conf);
        } else if (testCase.equals("sample")) {
            conf.prob = Double.parseDouble(cl.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY));
            Sample sample = new Sample();
            sample.processStream(conf);
        } else if (testCase.equals("project")) {
            conf.separator = "\\s+";
            conf.fieldIndex = 1;
            Projection project = new Projection();
            project.processStream(conf);
        } else if (testCase.equals("grep")) {
            conf.pattern = "abc";
            Grep grep = new Grep();
            grep.processStream(conf);
        } else if (testCase.equals("distinctcount")) {
            DistinctCount distinct = new DistinctCount();
            distinct.processStream(conf);
        } else if (testCase.equals("statistics")) {
            Statistics numeric = new Statistics();
            numeric.processStream(conf);
        }
    }
}
