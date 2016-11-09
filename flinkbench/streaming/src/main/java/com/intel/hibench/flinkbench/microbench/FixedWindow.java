package com.intel.hibench.flinkbench.microbench;

import com.intel.hibench.flinkbench.datasource.StreamBase;
import com.intel.hibench.flinkbench.util.FlinkBenchConfig;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.intel.hibench.common.streaming.metrics.KafkaReporter;
import com.intel.hibench.common.streaming.UserVisitParser;

import org.apache.flink.streaming.api.windowing.time.Time;

public class FixedWindow extends StreamBase {

  @Override
  public void processStream(final FlinkBenchConfig config) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setBufferTimeout(config.bufferTimeout);
    env.enableCheckpointing(config.checkpointDuration);

    createDataStream(config);
    DataStream<Tuple2<String, String>> dataStream = env.addSource(getDataStream());
    long windowDuration = Long.parseLong(config.windowDuration);
    long windowSlideStep = Long.parseLong(config.windowSlideStep);

    dataStream.map(new MapFunction<Tuple2<String, String>, Tuple2<String, Tuple2<Long, Integer>>>() {

      @Override
      public Tuple2<String, Tuple2<Long, Integer>> map(Tuple2<String, String> value) throws Exception {

        String ip = UserVisitParser.parse(value.f1).getIp();
        return new Tuple2<String, Tuple2<Long, Integer>>(ip, new Tuple2<Long, Integer>(Long.parseLong(value.f0), 1));
      }
    })
        .keyBy(0)
        .timeWindow(Time.milliseconds(windowDuration), Time.milliseconds(windowSlideStep))
        .reduce(new ReduceFunction<Tuple2<String, Tuple2<Long, Integer>>>() {
          @Override
          public Tuple2<String, Tuple2<Long, Integer>> reduce(Tuple2<String, Tuple2<Long, Integer>> v1, Tuple2<String, Tuple2<Long, Integer>> v2) throws Exception {
            return new Tuple2<String, Tuple2<Long, Integer>>(v1.f0, new Tuple2<Long, Integer>(Math.min(v1.f1.f0, v2.f1.f0), v1.f1.f1 + v2.f1.f1));
          }
        }).map(new MapFunction<Tuple2<String, Tuple2<Long, Integer>>, String>() {

      @Override
      public String map(Tuple2<String, Tuple2<Long, Integer>> value) throws Exception {
        KafkaReporter kafkaReporter = new KafkaReporter(config.reportTopic, config.brokerList);
        for (int i = 0; i < value.f1.f1; i++) {
          kafkaReporter.report(value.f1.f0, System.currentTimeMillis());
        }
        return value.f0;
      }
    });

    env.execute("Fixed Window Job");
  }
}
