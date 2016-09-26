/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.dfsioe;

import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Base mapper class for IO operations.
 * <p>
 * Two abstract method {@link #doIO(Reporter, String, long)} and 
 * {@link #collectStats(OutputCollector,String,long,Object)} should be
 * overloaded in derived classes to define the IO operation and the
 * statistics data to be collected by subsequent reducers.
 * 
 */
public abstract class IOMapperBase extends Configured
    implements Mapper<Text, LongWritable, Text, Text> {
  
  protected byte[] buffer;
  protected int bufferSize;
  protected FileSystem fs;
  protected String hostName;

  public IOMapperBase(Configuration conf) { 
    super(conf); 
    bufferSize = conf.getInt("test.io.file.buffer.size", 4096);
    buffer = new byte[bufferSize];
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch(Exception e) {
      hostName = "localhost";
    }
  }

  public void configure(JobConf job) {
    setConf(job);
    try {
      fs = FileSystem.get(job);
    } catch (Exception e) {
      throw new RuntimeException("Cannot create file system.", e);
    }
  }

  public void close() throws IOException {
  }
  
  /**
   * Perform io operation, usually read or write.
   * 
   * @param reporter
   * @param name file name
   * @param value offset within the file
   * @return object that is passed as a parameter to 
   *          {@link #collectStats(OutputCollector,String,long,Object)}
   * @throws IOException
   */
  abstract Object doIO(Reporter reporter, 
                       String name, 
                       long value) throws IOException;

  /**
   * Collect stat data to be combined by a subsequent reducer.
   * 
   * @param output
   * @param name file name
   * @param execTime IO execution time
   * @param doIOReturnValue value returned by {@link #doIO(Reporter,String,long)}
   * @throws IOException
   */
  abstract void collectStats(OutputCollector<Text, Text> output, 
                             String name, 
                             long execTime, 
                             Object doIOReturnValue) throws IOException;
  
  /**
   * Map file name and offset into statistical data.
   * <p>
   * The map task is to get the 
   * <tt>key</tt>, which contains the file name, and the 
   * <tt>value</tt>, which is the offset within the file.
   * 
   * The parameters are passed to the abstract method 
   * {@link #doIO(Reporter,String,long)}, which performs the io operation, 
   * usually read or write data, and then 
   * {@link #collectStats(OutputCollector,String,long,Object)} 
   * is called to prepare stat data for a subsequent reducer.
   */
  public void map(Text key, 
                  LongWritable value,
                  OutputCollector<Text, Text> output, 
                  Reporter reporter) throws IOException {
    String name = key.toString();
    long longValue = value.get();
    
    reporter.setStatus("starting " + name + " ::host = " + hostName);
    
    long tStart = System.currentTimeMillis();
    Object statValue = doIO(reporter, name, longValue);
    long tEnd = System.currentTimeMillis();
    long execTime = tEnd - tStart;
    collectStats(output, name, execTime, statValue);
    
    reporter.setStatus("finished " + name + " ::host = " + hostName);
  }
}

