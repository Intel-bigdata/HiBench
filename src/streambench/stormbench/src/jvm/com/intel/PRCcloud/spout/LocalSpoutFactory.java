package com.intel.PRCcloud.spout;

import backtype.storm.topology.base.BaseRichSpout;

public class LocalSpoutFactory{
  public static BaseRichSpout getSpout(String datafile,long recordCount){
    return new LocalSpout(datafile,recordCount);
  }
}