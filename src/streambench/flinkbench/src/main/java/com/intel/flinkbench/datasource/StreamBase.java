package com.intel.flinkbench.datasource;


import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.intel.flinkbench.util.FlinkBenchConfig;
import com.intel.flinkbench.util.Utils;

import java.util.Map;

public abstract class StreamBase {
    private DataStream<String> dataStream;
    
    public DataStream<String> getDataStream() {
        return this.dataStream;
    }
    
    public void createDataStream(FlinkBenchConfig config) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> conf = Utils.readAndParseConfig(config);
        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(conf);

        this.dataStream = env.addSource(new FlinkKafkaConsumer08<String>(
                        config.topic,
                        new SimpleStringSchema(),
                        flinkBenchmarkParams.getProperties()));
    }
    public abstract void processStream(FlinkBenchConfig config) throws Exception;
    
}
