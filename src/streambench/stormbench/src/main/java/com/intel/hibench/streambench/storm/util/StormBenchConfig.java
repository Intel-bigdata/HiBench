package com.intel.hibench.streambench.storm.util;

import java.io.Serializable;

public class StormBenchConfig implements Serializable {
  public String zkHost;
  public int workerCount;
  public int spoutThreads;
  public int boltThreads;
  public String benchName;
  public String topic;
  public String consumerGroup;
  public boolean ackon;
  public double prob;
  public String brokerList;
  public String reporterTopic;

  public long windowDuration;
  public long windowSlideStep;

  //Following are fields that are benchmark specific
  public String separator = "\\s+";
  public int fieldIndex = 1;
  public String pattern = " ";
}