package com.intel.flinkbench.datasource;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.*;

import com.intel.flinkbench.util.FlinkBenchConfig;
import com.intel.flinkbench.util.Utils;

import java.util.Map;

public abstract class StreamBase {
    private DataStream<Tuple2<String, String>> dataStream;
    
    public DataStream<Tuple2<String, String>> getDataStream() {
        return this.dataStream;
    }
    
    public void createDataStream(FlinkBenchConfig config) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Map<String, String> conf = Utils.readAndParseConfig(config);
        ParameterTool flinkBenchmarkParams = ParameterTool.fromMap(conf);
        KeyedDeserializationSchema<Tuple2<String, String>> schema = new TypeInformationKeyValueSerializationSchema<String, String>(String.class, String.class, env.getConfig());

        this.dataStream = env.addSource(new FlinkKafkaConsumer08<Tuple2<String, String>>(
                        config.topic,
                        schema,
                        flinkBenchmarkParams.getProperties()));
    }
    public abstract void processStream(FlinkBenchConfig config) throws Exception;
    
}
