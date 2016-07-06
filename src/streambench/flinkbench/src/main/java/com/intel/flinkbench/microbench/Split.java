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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Split extends StreamBase {
    
    @Override
    public void processStream(FlinkBenchConfig config) throws Exception{
        final String seperator = config.separator;
        createDataStream(config);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());
        dataStream.flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
            @Override
            public void flatMap(Tuple2<String, String> value, Collector<Tuple2<String, String>> out)
                    throws Exception {
                for(String word: value.f1.split(seperator)){
                    out.collect(new Tuple2<String, String>(value.f0, word));
                }
            }
        });
        env.execute("stream split job.");
    }
}
