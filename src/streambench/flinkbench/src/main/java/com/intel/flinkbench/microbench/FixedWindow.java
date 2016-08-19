package com.intel.flinkbench.microbench;

import com.intel.flinkbench.datasource.StreamBase;
import com.intel.flinkbench.util.FlinkBenchConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.intel.hibench.streambench.common.metrics.KafkaReporter;
import com.intel.hibench.streambench.common.UserVisitParser;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FixedWindow extends StreamBase{

    @Override
    public void processStream(final FlinkBenchConfig config) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setBufferTimeout(config.bufferTimeout);

        createDataStream(config);
        DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());

        dataStream.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<String, Integer>>>() {

            @Override
            public Tuple2<String, Tuple2<String, Integer>> map(Tuple2<String, String> value) throws Exception {
                
                String browser = UserVisitParser.parse(value.f1).getBrowser();
                return new Tuple2<String, Tuple2<String, Integer>>(browser, new Tuple2<String, Integer>(value.f0, 1));
            }
        })
                .keyBy(0)
                .timeWindow(Time.minutes(1), Time.seconds(30))
                .reduce(new ReduceFunction<Tuple2<String, Tuple2<String, Integer>>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Integer>> reduce(Tuple2<String, Tuple2<String, Integer>> v1, Tuple2<String, Tuple2<String, Integer>> v2) throws Exception {
                        KafkaReporter kafkaReporter = new KafkaReporter(config.reportTopic, config.brokerList);

                        kafkaReporter.report(Long.parseLong(v1.f1.f0), System.currentTimeMillis());
                        return new Tuple2<String, Tuple2<String, Integer>>(v1.f0, new Tuple2<String, Integer>(v1.f1.f0, v1.f1.f1 + v2.f1.f1));
                    }
                });

        env.execute("Fixed Window Job");
    }
}
