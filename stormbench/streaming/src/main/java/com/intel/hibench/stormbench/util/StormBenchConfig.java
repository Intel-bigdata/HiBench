package com.intel.hibench.stormbench.util;

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
  public boolean localShuffle;
  public String brokerList;
  public String reporterTopic;

  public long windowDuration;
  public long windowSlideStep;

  //Following are fields that are benchmark specific
  public String separator = "\\s+";
  public int fieldIndex = 1;
  public String pattern = " ";
}