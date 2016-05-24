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

package com.intel.flinkbench.microbench;

import com.intel.flinkbench.datasource.StreamBase;
import com.intel.flinkbench.util.FlinkBenchConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.intel.flinkbench.util.BenchLogUtil;

public class Statistics extends StreamBase{
    private static Long max = Long.MIN_VALUE;
    private static Long min = Long.MAX_VALUE;
    private static Long sum = 0L;
    private static Long count = 0L;

    public void reduce(Long value) {
        if (value > max) {
            max = value;
        }
        if (value < min) {
            min = value;
        }
        sum += value;
        count++;
    }
    @Override
    public void processStream(FlinkBenchConfig config) throws Exception {
        createDataStream(config);
        DataStream<String> dataStream = getDataStream();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        dataStream
                .map(new MapFunction<String, Long>() {
                    @Override
                    public Long map(String value) {
                        Long longValue = Long.parseLong(value);
                        reduce(longValue);
                        BenchLogUtil.logMsg("current max record: " + max);
                        BenchLogUtil.logMsg("current min record: " + min);
                        BenchLogUtil.logMsg("current sum of records: " + sum);
                        BenchLogUtil.logMsg("current count of records: " + count);
                        return longValue;
                    }
                });
                
    }
}
