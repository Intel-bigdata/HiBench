package com.intel.PRCcloud.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.*;

import java.util.*;
import java.io.*;
import com.intel.PRCcloud.util.*;

public class LocalSpout extends BaseRichSpout{
  SpoutOutputCollector _collector;
  String fileName="";
  ArrayList<String> contents=new ArrayList<String>();
  long i=0;
  long recordCount=0;

  public LocalSpout(String file,long recordCount){
    this.fileName=file;
	this.recordCount=recordCount;
  }
  
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
	File file=new File(fileName);
	BenchLogUtil.logMsg("Data file path"+file.getAbsolutePath());
	try{
	  BufferedReader reader=new BufferedReader(new FileReader(file));
	  String line=null;
	  while((line=reader.readLine())!=null){
		contents.add(line);
	  }

	  reader.close();
	} catch (FileNotFoundException e) {
	  e.printStackTrace();
	} catch (IOException e) {
	  e.printStackTrace();
	}
  }
  
  public void nextTuple() {
    if(contents.size()!=0 && i<recordCount){
	  _collector.emit(new Values(contents.get((int)((i++)%contents.size()))));
	}
      
  }
  
   public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}
