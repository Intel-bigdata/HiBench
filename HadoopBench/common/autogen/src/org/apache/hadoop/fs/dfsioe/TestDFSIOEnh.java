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

import java.io.*;

import java.util.Date;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.logging.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.dfsioe.Analyzer._Mapper;
import org.apache.hadoop.fs.dfsioe.Analyzer._Reducer;

/**
 * Enhanced Distributed i/o benchmark.
 * This is an enhanced version of original DFSIO benchmark.
 * The result report include the original DFSIO benchmark report,
 * as well as the aggregated HDFS throughput information.
 * <p>
 * This test writes into or reads from a specified number of files.
 * File size is specified as a parameter to the test. 
 * Each file is accessed in a separate map task.
 * During the period of each map task, we sampled several timestamps 
 * and  total written/read bytes is sampled. 
 * <p>
 * The reducer collects the following statistics:
 * <ul>
 * <li>number of tasks completed</li>
 * <li>number of bytes written/read</li>
 * <li>execution time</li>
 * <li>io rate</li>
 * <li>io rate squared</li>
 * <li>the throughput samples while writing/reading each file (new)</li>
 * <li>the start, end time of writing/reading each file (new) </li>
 * <li>the total time spent on throughput sampling (new)</li>
 * </ul>
 *    
 * Finally, for each file, the throughput samples are interpolated and sampled again 
 * at the rate the plotInterval. This re-sampling result will be output to a csv file, 
 * if "-tputReportEach" is in commandline arguments.      
 * Then, the re-sampled throughput of each file is aggregated to a total  and output to a file.
 * if "-tputResultTotal"
 * and the following information is appended to a local file
 * <ul>
 * <li>read or write test</li>
 * <li>date and time the test finished</li>   
 * <li>number of files</li>
 * <li>total number of bytes processed</li>
 * <li>throughput in mb/sec (total number of bytes / sum of processing times)</li>
 * <li>average i/o rate in mb/sec per file</li>
 * <li>standard deviation of i/o rate </li>
 * <li>average of aggregated</li>
 * </ul>
 * 
 */

public class TestDFSIOEnh extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(TestDFSIOEnh.class);
  private static final int TEST_TYPE_READ = 0;
  private static final int TEST_TYPE_WRITE = 1;
  private static final int TEST_TYPE_CLEANUP = 2;
  private static final int DEFAULT_BUFFER_SIZE = 1000000;
  private static final String BASE_FILE_NAME = "test_io_";
  private static final String DEFAULT_RES_FILE_NAME = "TestDFSIOEnh_results.log";

  private static String TEST_ROOT_DIR = System.getProperty("test.build.data","/benchmarks/TestDFSIO-Enh");
  private static Configuration fsConfig = new Configuration();
  private static Path CONTROL_DIR = new Path(TEST_ROOT_DIR, "io_control");
  protected static Path WRITE_DIR = new Path(TEST_ROOT_DIR, "io_write");
  protected static Path READ_DIR = new Path(TEST_ROOT_DIR, "io_read");
  private static Path DATA_DIR = new Path(TEST_ROOT_DIR, "io_data");
  private static Path REPORT_DIR = new Path(TEST_ROOT_DIR, "reports");
  private static Path REPORT_TMP = new Path(TEST_ROOT_DIR, "_merged_reports.txt");


  static{
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
  }

  // Constants
  private static final long KILO = 1024;
  private static final long MEGA = 1024*1024;
  private static final long GIGA = 1024*1024*1024; 

  //default values for aggregated throughput parameters
  private static final String DEFAULT_TPUT_RESFILE_NAME = "TestDFSIOEnh_Tput.csv";  //result file name
  private static final int    DEFAULT_TPUT_SAMPLING_INTERVAL = 500; //sample interval in milliseconds.
  private static final long   DEFAULT_TPUT_SAMPLE_UNIT = MEGA; // the unit of sample
  private static final int    DEFAULT_TPUT_PLOT_INTERVAL = 1000; //plot interval in milliseconds


  
  protected static class IOStatistics {
    public Long objSize = new Long(0);
    public ArrayList<Long> statHdfs = new ArrayList<Long> ();
    public int loggingTime = 0;
  }
  
  
  protected abstract static class IOStatMapperEnh extends IOMapperBase {
    IOStatMapperEnh() {
      super(fsConfig);
    }

    protected IOStatistics stats = new IOStatistics();    
    protected int samplingInterval;

    protected  void init(Configuration conf){
        samplingInterval = conf.getInt("test.io.sampling.interval",DEFAULT_TPUT_SAMPLING_INTERVAL);
        
    }

    void collectStats(OutputCollector<Text, Text> output,
                      String name,
                      long execTime,
                      Object statistics) throws IOException {
        
        IOStatistics stats = (IOStatistics) statistics;

        //original TestDFSIO benchmark report
        //super.collectStats(output, name, execTime,stats.objSize);
        long totalSize = ((Long)stats.objSize).longValue();
        float ioRateMbSec = (float)totalSize * 1000 / (execTime * MEGA);
        LOG.info("Number of bytes processed = " + totalSize);
        LOG.info("Exec time = " + execTime);
        LOG.info("IO rate = " + ioRateMbSec);

        output.collect(new Text("l:tasks"), new Text(String.valueOf(1)));
        output.collect(new Text("l:size"), new Text(String.valueOf(totalSize)));
        output.collect(new Text("l:time"), new Text(String.valueOf(execTime)));
        output.collect(new Text("f:rate"), new Text(String.valueOf(ioRateMbSec*1000)));
        output.collect(new Text("f:sqrate"), new Text(String.valueOf(ioRateMbSec*ioRateMbSec*1000)));
 
        //enhanced report for real-time throughput
        int size = stats.statHdfs.size();
        StringBuilder statsSum = new StringBuilder();
        int serialNo = 0;
        int lastLen = 0;
        for(int i = 0; i< size; i += 2) {
            long timeStamp = stats.statHdfs.get(i);
            long bytes = stats.statHdfs.get(i+1);
            statsSum.append(timeStamp);
            statsSum.append(":");
            statsSum.append(bytes);
            statsSum.append(";");
            if (i%100 == 0){
                int len = statsSum.length();
                output.collect(new Text("s:"+name+":"+serialNo+":tput_samples"),new Text(statsSum.substring(lastLen,len)+"EoR"));
                serialNo ++;
                lastLen = len;
            }
        }
        statsSum.append("EoR");
        output.collect(new Text("s:"+name+":"+serialNo+":tput_samples"), new Text(statsSum.substring(lastLen,statsSum.length())));
        //add start and end time stamp
        output.collect(new Text("g:"+name+":io_start_end"), new Text(String.valueOf(stats.statHdfs.get(0))+";"+String.valueOf(stats.statHdfs.get(size-2))));
        output.collect(new Text("f:logging_time"), new Text(String.valueOf(stats.loggingTime)));
    }
  }


  public static class WriteMapperEnh extends IOStatMapperEnh {
  
    public WriteMapperEnh(){
        super();
        for(int i=0; i < bufferSize; i++)
            buffer[i] = (byte)('0' + i % 50);
    }     


    public Object doIO(Reporter reporter,
                       String name,
                       long totalSize
                       ) throws IOException{

        totalSize *= MEGA;
        OutputStream out;
        out = fs.create(new Path(DATA_DIR, name), true, bufferSize);
        stats.objSize = totalSize;        
        
        long lastTimeStamp=0;
        int time = 0;
        //log start time
        long startTime = System.currentTimeMillis();
        stats.statHdfs.add(new Long(startTime));
        stats.statHdfs.add(new Long(0));
        
        try {
            long nrRemaining;
            for (nrRemaining = totalSize; nrRemaining > 0; nrRemaining -= bufferSize) {
                int curSize = (bufferSize < nrRemaining)? bufferSize : (int)nrRemaining;
                out.write(buffer, 0, curSize);
                reporter.setStatus("writing " + name + "@" + 
                                    (totalSize - nrRemaining) + "/" + totalSize
                                    + " ::host = " + hostName);
                //add statistics logging
                long currentTime = System.currentTimeMillis();
                if( lastTimeStamp==0 || (currentTime-lastTimeStamp) >= samplingInterval) {
                    stats.statHdfs.add(new Long(currentTime));
                    stats.statHdfs.add(new Long(totalSize-nrRemaining));
                    lastTimeStamp=currentTime;
                }
                long end = System.currentTimeMillis();
                time += (end-currentTime);
            }        
  
        } finally {
            out.close();
        }

        //log end time
        long endTime = System.currentTimeMillis();        
        stats.statHdfs.add(new Long(endTime));
        stats.statHdfs.add(new Long(totalSize));

        stats.loggingTime = time;
        return stats;
    }

  }
 
  private static String getFileName(int fIdx) {
    return BASE_FILE_NAME + Integer.toString(fIdx);
  }

  protected static void writeTest(FileSystem fs, Configuration fsConfig)
    throws IOException {

    fs.delete(DATA_DIR, true);
    fs.delete(WRITE_DIR, true);
  
    //set write specific configurations
    JobConf job = new JobConf(fsConfig, TestDFSIOEnh.class);
    //should turn off speculative execution to avoid two attemps writing to the same file
    job.setMapSpeculativeExecution(false);
  
    runIOTest(WriteMapperEnh.class, WRITE_DIR,job);
  }
 
 

  /**
   * Read mapper class.
   */
  public static class ReadMapperEnh extends IOStatMapperEnh {

    public ReadMapperEnh() { 
        super(); 
    }

    public Object doIO(Reporter reporter, 
                       String name, 
                       long totalSize 
                       ) throws IOException {

        totalSize *= MEGA;
        // open file
        DataInputStream in = fs.open(new Path(DATA_DIR, name));
        stats.objSize = totalSize;

        long lastTimeStamp = 0;
        int time = 0;

        //log start time 
        long startTime = System.currentTimeMillis();
        stats.statHdfs.add(new Long(startTime));
        stats.statHdfs.add(new Long(0));

        
        long actualSize = 0;
        try {
            while (actualSize < totalSize) {
                int curSize = in.read(buffer, 0, bufferSize);
                if (curSize < 0) break;
                actualSize += curSize;
                reporter.setStatus("reading " + name + "@" + 
                             actualSize + "/" + totalSize 
                             + " ::host = " + hostName);
                //add statistics samples        
                long currentTime = System.currentTimeMillis();
                if( lastTimeStamp==0 || (currentTime-lastTimeStamp) >= samplingInterval) {
                    stats.statHdfs.add(new Long(currentTime));
                    stats.statHdfs.add(new Long(actualSize));
                    lastTimeStamp=currentTime;
                }
                long end = System.currentTimeMillis();
                time += (end-currentTime);
            }
        } finally {
            in.close();
        }
        //log end time
        long endTime = System.currentTimeMillis();
        stats.statHdfs.add(new Long(endTime));
        stats.statHdfs.add(new Long(actualSize));
   
        stats.loggingTime = time;
        return stats;
    }
  }

  protected static void readTest(FileSystem fs,Configuration fsConfig) throws IOException {
    fs.delete(READ_DIR, true);
    //set read job specific configurations 
    JobConf job = new JobConf(fsConfig, TestDFSIOEnh.class);
    //turn on speculative execution to potentially improve performance.
    //job.setMapSpeculativeExecution(true);
    runIOTest(ReadMapperEnh.class, READ_DIR,job);
  }

  protected static void sequentialTest(
                                     FileSystem fs, 
                                     int testType, 
                                     int fileSize, 
                                     int nrFiles
                                     ) throws Exception {
    IOStatMapperEnh ioer = null;
    if (testType == TEST_TYPE_READ)
        ioer = new ReadMapperEnh();
    else if (testType == TEST_TYPE_WRITE)
        ioer = new WriteMapperEnh();
    else
        return;
    for(int i=0; i < nrFiles; i++)
        ioer.doIO(Reporter.NULL,
                BASE_FILE_NAME+Integer.toString(i), 
                MEGA*fileSize);
  }

  protected static void runIOTest( @SuppressWarnings("rawtypes") Class<? extends Mapper> mapperClass,
                                 Path outputDir,
                                 JobConf job
                                 ) throws IOException {
    FileInputFormat.setInputPaths(job, CONTROL_DIR);
    job.setInputFormat(SequenceFileInputFormat.class);

    job.setMapperClass(mapperClass);
    job.setReducerClass(AccumulatingReducer.class);

    FileOutputFormat.setOutputPath(job, outputDir);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    JobClient.runJob(job);
  }

 /**
  * A linear interpolator.
  *
  * Add a series of points (x,y) and then 
  * get interpolated value y at specific x
  */
  protected static class LinearInterpolator {

    private ArrayList<Point> points = new ArrayList<Point>();
    boolean sorted = false;

    /**
     * 2-D sample Point definition.
     */
    protected static class Point {
        private double x;
        private double y;
        Point (double x, double y) {
            this.x = x;
            this.y = y;
        }
        public double x () { return x;}
        public double y () { return y;}

        public static class ComparatorX implements Comparator<Point> {
            public int compare(Point p1, Point p2){
                double p1X = p1.x();
                double p2X = p2.x();
                if (p1X > p2X) 
                    return 1;
                else if(p1X < p2X)
                    return -1;
                else
                    return 0;
            }
       }
    }

    /**
     * Add Sample Points.
     */
    public void add (double x, double y){
        points.add(new Point(x,y));
        sorted = false;
    }

    protected int searchNearest(double x){
        //check out of bounds
        if(x > points.get(points.size()-1).x() || x < points.get(0).x()) {
            return -1;
        }
        //find nearest
        for (int index = 0; index<points.size(); index++){
            if(points.get(index).x() >= x)
                return index;
        }
        return -1;
    }

    /**
     * Get Interpolated value.
     */
    public double get (double x) {
        if(!sorted){
            Collections.sort(points, new Point.ComparatorX());
            sorted = true;
        }
        //find the nearest points
        int index = searchNearest(x);
        if(-1 == index) { 
            LOG.error("point out of bound of available interpolation");
            return -1;
        }
        // if index = 0, x must be equal to the first element.
        if(0 == index ) {
            return points.get(0).y();
        } else {
            //do interpolation
            double x_0 = points.get(index-1).x();
            double x_1 = points.get(index).x();
            double y_0 = points.get(index-1).y();
            double y_1 = points.get(index).y();
            double y = (y_1-y_0)/(x_1-x_0)*(x-x_0)+y_0;
            return y;   
        }
    }
    /**
     * Dump Sample Points. 
     * For debug use.
     */
    public void dumpPoints(){
        for(int i = 0; i<points.size(); i++){
            double x = points.get(i).x();
            double y = points.get(i).y();
            LOG.info("i="+i+" ("+x+","+y+")");
        }
    }
  }

  /**
   * Main function.
   */
  public int run(String[] args) throws Exception {

    int testType = TEST_TYPE_READ;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    boolean skipAnalyze=false;
    int fileSize = 1;
    int nrFiles = 1;
    String resFileName = DEFAULT_RES_FILE_NAME;
    String tputFileName = DEFAULT_TPUT_RESFILE_NAME;
    boolean isSequential = false;
    boolean tputReportEach = false;
    boolean tputReportTotal = false;
    int tputSampleInterval = DEFAULT_TPUT_SAMPLING_INTERVAL;
    int tputPlotInterval = DEFAULT_TPUT_PLOT_INTERVAL;
    long tputSampleUnit = DEFAULT_TPUT_SAMPLE_UNIT;
    float threshold = 0.5f;

    String version="TestFDSIO.0.0.4 Enhanced Version";
    String usage = "Usage: TestFDSIOEnh -read | -write | -skipAnalyze | -clean [-nrFiles N] [-fileSize MB] [-resFile resultFileName] [-bufferSize Bytes] [-tputFile AggregatedThroughputCSVFileName] [-sampleInterval Miliseconds] [-plotInterval Miliseconds] [-sampleUnit g|m|k|b] [-sumThreshold 0.0~1.0] [-tputReportEach] [-tputReportTotal]";
    
    System.out.println(version);
    if (args.length == 0) {
        System.err.println(usage);
        return -1;
    }

    //parse arguments
    for (int i = 0; i < args.length; i++) {       
        if (args[i].startsWith("-read")) {
            testType = TEST_TYPE_READ;
        } else if (args[i].equals("-write")) {
            testType = TEST_TYPE_WRITE;
        } else if (args[i].equals("-clean")) {
            testType = TEST_TYPE_CLEANUP;
        } else if (args[i].equals("-skipAnalyze")){
	    skipAnalyze = true;
        } else if (args[i].startsWith("-seq")) {
            isSequential = true;
        } else if (args[i].equals("-nrFiles")) {
            nrFiles = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-fileSize")) {
            fileSize = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-bufferSize")) {
            bufferSize = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-resFile")) {
            resFileName = args[++i];
        } else if (args[i].equals("-tputFile")) {
            tputFileName = args[++i];
            tputReportTotal = true;
        } else if (args[i].equals("-tputReportEach")){
            tputReportEach = true;
        } else if (args[i].equals("-tputReportTotal")){
            tputReportTotal = true;
        } else if (args[i].equals("-sampleInterval")){
            tputSampleInterval = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-plotInterval")){
            tputPlotInterval = Integer.parseInt(args[++i]);
        } else if (args[i].equals("-sumThreshold")){
            threshold = Float.parseFloat(args[++i]);
            if(threshold > 1){
                LOG.warn("Summary threshold is larger than 1.0 ! Value should be within [0.0,1.0] !");
                threshold = 1.0f;
            }
            else if (threshold < 0){ 
                LOG.warn("Summary threshold is smaller than 0.0 ! Value should be within [0.0,1.0] !");
                threshold = 0.0f;
            }
        } else if (args[i].equals("-sampleUnit")){
            String unit = args[++i];
            if (unit.equalsIgnoreCase("k")) 
                tputSampleUnit = KILO;
            else if(unit.equalsIgnoreCase("m"))
                tputSampleUnit = MEGA;
            else if (unit.equalsIgnoreCase("b"))
                tputSampleUnit = 1;
            else if (unit.equalsIgnoreCase("g"))
                tputSampleUnit = GIGA;
            else {
                LOG.warn("Illegal format of parameter \"sampleUnit\", Ignored.");
            }
        }
    }

    LOG.info("nrFiles = " + nrFiles);
    LOG.info("fileSize (MB) = " + fileSize);
    LOG.info("bufferSize = " + bufferSize);
  
    try {

        Configuration fsConfig = new Configuration(getConf());

        fsConfig.setInt("test.io.file.buffer.size", bufferSize);
        fsConfig.setInt("test.io.sampling.interval",tputSampleInterval);
 
        FileSystem fs = FileSystem.get(fsConfig);

        //get the configuration of max number of concurrent maps
        JobConf dummyConf = new JobConf(fsConfig, TestDFSIOEnh.class);
        JobClient jc = new JobClient(dummyConf);
        int maxreduces = jc.getDefaultReduces();
        int reducesPerNode = fsConfig.getInt("mapred.tasktracker.reduce.tasks.maximum",2);
        int nodenum = maxreduces/reducesPerNode;
        int mapPerNum = fsConfig.getInt("mapred.tasktracker.map.tasks.maximum",2);
        int mapSlots = mapPerNum * nodenum; 
        LOG.info("maximum concurrent maps = "+String.valueOf(mapSlots));

        if (isSequential) {
            long tStart = System.currentTimeMillis();
            sequentialTest(fs, testType, fileSize, nrFiles);
            long execTime = System.currentTimeMillis() - tStart;
            String resultLine = "Seq Test exec time sec: " + (float)execTime / 1000;
            LOG.info(resultLine);
            return 0;
        }
        if (testType == TEST_TYPE_CLEANUP) {
            cleanup(fs);
            return 0;
        }
        createControlFile(fs, fileSize, nrFiles,fsConfig);
        long tStart = System.currentTimeMillis();
        if (testType == TEST_TYPE_WRITE)
            writeTest(fs,fsConfig);
        if (testType == TEST_TYPE_READ)
            readTest(fs,fsConfig);
        long execTime = System.currentTimeMillis() - tStart;
    
        if (skipAnalyze == false){
            /*analyzeResult(fs, testType, execTime, resFileName, nrFiles, fileSize*MEGA, 
                    tStart, tputPlotInterval, tputSampleUnit,(int)(mapSlots*threshold),
                    tputFileName, tputReportEach, tputReportTotal);*/
            runAnalyse(fs, fsConfig, testType, execTime, resFileName, nrFiles, fileSize*MEGA, 
                    tStart, tputPlotInterval, tputSampleUnit,(int)(mapSlots*threshold),
                    tputFileName, tputReportEach, tputReportTotal);
        }
    } catch(Exception e) {
        System.err.print(StringUtils.stringifyException(e));
        return -1;
    }

    return 0;

  }

  public static void main(String[] args) throws Exception{
    int res = ToolRunner.run(new TestDFSIOEnh(), args);
    System.exit(res);
  }

  private static void createControlFile(
                                        FileSystem fs,
                                        int fileSize, // in MB 
                                        int nrFiles,
                                        Configuration fsConfig
                                        ) throws IOException {
    LOG.info("creating control file: "+fileSize+" mega bytes, "+nrFiles+" files");

    fs.delete(CONTROL_DIR, true);

    for(int i=0; i < nrFiles; i++) {
      String name = getFileName(i);
      Path controlFile = new Path(CONTROL_DIR, "in_file_" + name);
      SequenceFile.Writer writer = null;
      try {
        writer = SequenceFile.createWriter(fs, fsConfig, controlFile,
                                           Text.class, LongWritable.class,
                                           CompressionType.NONE);
        writer.append(new Text(name), new LongWritable(fileSize));
      } catch(Exception e) {
        throw new IOException(e.getLocalizedMessage());
      } finally {
        if (writer != null)
          writer.close();
        writer = null;
      }
    }
    LOG.info("created control files for: "+nrFiles+" files");
  }
  
 /**
  * Analyze Aggregated throughput samples.
  */
  @Deprecated
  protected static String[] analyzeTputSamples(ArrayList<String> wrSamples, 
                                           int nrFiles, 
                                           long fileSize,  
                                           long tStart, 
                                           long execTime, 
                                           int[] concurrency,
                                           int plotInterval, 
                                           long sampleUnit,
                                           int threshold, 
                                           String tputResFile, 
                                           boolean writeReportTotal, 
                                           boolean writeReportEach
                                        ) throws IOException {

    int maxslot = (int)(execTime/plotInterval)+1;

    double[]  bytesTotal = new double[maxslot+1];
    double[]  bytesChanged = new double[maxslot+1]; 

    double[] resultValue = new double [maxslot+1];
    int[]  resultSample = new int [maxslot+1];

    //initialize the arrays
    for(int i = 0; i<=maxslot; i++){
        bytesTotal[i] = 0;
        bytesChanged[i] = 0;
    }

    for (int f = 0; f<nrFiles; f++) {
      // clear up the arrays
        for(int j = 0; j<maxslot+1; j++){
            resultValue[j] = 0;
            resultSample[j] = 0;
        }
        // add to interpolator
        LinearInterpolator processor = new LinearInterpolator();
 
        double min_timeSpot = Double.MAX_VALUE; 
        double max_timeSpot = 0;
        for(int i = 0; i < wrSamples.size(); i += 2){
            String wrFileName = wrSamples.get(i);
            if (!wrFileName.equals(getFileName(f))) continue;
            String sampleLog = wrSamples.get(i+1);
            String[] samples = sampleLog.split(";");
            //collect the samples
            for(int j=0; !samples[j].startsWith("EoR"); j++){
                String[] items = samples[j].split(":");
                long timeStamp = Long.parseLong(items[0]);
                long bytes = Long.parseLong(items[1]);    
                double timePassed = (timeStamp-tStart)/(double)plotInterval;
                if(timePassed > max_timeSpot) max_timeSpot = timePassed;
                if(timePassed < min_timeSpot) min_timeSpot = timePassed;
                processor.add((double)timePassed,(double)bytes);
            }
        }
     
        processor.add(0,0);
        processor.add(maxslot+0.1,fileSize);
             
        //get value for each time slot
        for (int k = 0; k<=maxslot; k++){
            resultValue[k]=processor.get(k);
        } 
        
        if(writeReportEach) {
            PrintStream res = new PrintStream(
                            new FileOutputStream (
                                    new File(tputResFile+""+"test_io_"+String.valueOf(f)+".csv"),true));
            for (int i = 0; i<=maxslot-1; i++)
                 bytesChanged[i] = resultValue[i+1]-resultValue[i];
            bytesChanged[maxslot] = 0;
            for (int ri = 0; ri<=maxslot; ri++)
                res.println(ri+","+resultValue[ri]/(double)sampleUnit+","+bytesChanged[ri]/(double)sampleUnit);
            res.close();
        }
        //add into total bytes
        for (int k=0; k<=maxslot; k++)
            bytesTotal[k] += resultValue[k];
    }

    //change unit
    for (int i = 0; i<=maxslot; i++)
        bytesTotal[i] /= (double)sampleUnit ;   

    //calculate the aggregated throughput
    for (int i = 0; i<=maxslot-1; i++)
        bytesChanged[i] = bytesTotal[i+1]-bytesTotal[i];
    bytesChanged[maxslot] = 0;

    if(writeReportTotal) {
        PrintStream res = new PrintStream(
                                new FileOutputStream (
                                        new File(tputResFile),true));
        for (int ri = 0; ri<=maxslot; ri++)
            res.println(ri+","+bytesTotal[ri]+","+bytesChanged[ri]);
        res.close();
    }

   String unit = "";
   if (sampleUnit == KILO) 
        unit = "kb";
   else if (sampleUnit == MEGA)
        unit = "mb";
   else if (sampleUnit == 1)
        unit = "b";
   else if (sampleUnit == GIGA)
        unit = "gb";

   return calcSummary(bytesChanged,concurrency,threshold,unit);

 } 
  
/**
 * A concise summary of the Aggregated throughput.
 * Find the mean and the standard deviation of aggregated throughput along job execution time.
 * Note: We do not count in all the time slot when calculating mean and standard deviation.
 * We have a <code>threshold</code> indicating the criteria whether to include/exclude each point.
 * The time slots are excluded if at that time the number of concurrent mappers is less than threshold.
 * At present, the threshold is set to half of maximum map slots (mapred.tasktracker.map.maximum * number of slaves).  
 */
  
 protected static String[] calcSummary(final double[] bytesChanged,
                                       int[] concurrency, 
                                       int threshold, 
                                       String unit)
                                      throws IOException {

    double sum = 0;
    double sumSquare = 0;
    int count = 0;    
 
    char[] counted = new char[bytesChanged.length];
    for(int i=0; i< counted.length; i++)
        counted[i] = '.';
    StringBuffer concurrStr = new StringBuffer();
    for (int i = 0; i<bytesChanged.length; i++){
        concurrStr.append(String.valueOf(concurrency[i])+",");
        if(concurrency[i] >= threshold){
            sum += bytesChanged[i];
            sumSquare += bytesChanged[i]*bytesChanged[i];
            count++;
            counted[i] = '1';
        }
    }  
    concurrStr.deleteCharAt((concurrStr.length()-1));

    if (count != 0) {
        double mean = sum/count;
        double stdDev = Math.sqrt(Math.abs(sumSquare/count - mean*mean)); 

        String[] output = {
            concurrStr.toString(),    
            "Average of Aggregated Throughput : " + mean+" "+unit+"/sec",
            "              Standard Deviation : " + stdDev+" "+unit+"/sec",
            "   Time spots Counted in Average : "+ String.valueOf(counted) + " ", 
        };
        return output;

    } else {
        String[] output = {    
            "Aggregated throughput results are unavailable, because the concurrency of Mappers is too low and consequently the aggregated throughput measurement is not very meaningful",
            "Please adjust your test workload and try again.",
        };
        return output;
    }

  }

 
	 protected static void runAnalyse(FileSystem fs, Configuration fsConfig,
								         int testType, long execTime,
								         String resFileName, int nrFiles,
								         long fileSize, long tStart,
								         int plotInterval, long sampleUnit,
								         int threshold, String tputResFileName,
								         boolean tputReportEach, boolean tputReportTotal) throws IOException {
		 long t1 = System.currentTimeMillis();
		 Path reduceFile;
		 if (testType == TEST_TYPE_WRITE)
			 reduceFile = new Path(WRITE_DIR, "part-00000");
		 else
			 reduceFile = new Path(READ_DIR, "part-00000");		
		
		 int maxslot = (int)(execTime/plotInterval)+1;
		 int[] concurrency = new int[maxslot+1];
		 double[]  bytesTotal = new double[maxslot+1];
		 for (int i=0; i<maxslot+1; i++) {
			 bytesTotal[i] = 0;
			 concurrency[i] = 0; 
		 }
		 
		 BufferedReader rd = null;
		 long tasks = 0;
		 long size = 0;
		 long time = 0;
		 float rate = 0;
		 float sqrate = 0;
		 float loggingTime = 0;
		 try {
			 rd = new BufferedReader(new InputStreamReader(new DataInputStream(fs.open(reduceFile))));
			 String s = null;
			 while ((s = rd.readLine()) != null) {
				 StringTokenizer tokens = new StringTokenizer(s, " \t\n\r\f%");
				 String lable = tokens.nextToken(); 
				 if (lable.endsWith(":tasks")) {
					 tasks = Long.parseLong(tokens.nextToken());
				 } else if (lable.endsWith(":size")) {
					 size = Long.parseLong(tokens.nextToken());
				 } else if (lable.endsWith(":time")) {
					 time = Long.parseLong(tokens.nextToken());
				 } else if (lable.endsWith(":rate")) {
					 rate = Float.parseFloat(tokens.nextToken());
				 } else if (lable.endsWith(":sqrate")) {
					 sqrate = Float.parseFloat(tokens.nextToken());
				 } else if (lable.endsWith(":io_start_end")) {
					 String[] t = tokens.nextToken().split(";");
					 int start = (int)((Long.parseLong(t[0])-tStart)/plotInterval) + 1;
					 int end = (int)((Long.parseLong(t[1])-tStart)/plotInterval) - 1;
					 if(start < 0)
						 start = 0;
					 for (int i=start; i<=end; i++){
						 if(i > concurrency.length-1)
							 break;
						 concurrency[i]++;
					 }
				 } else if (lable.endsWith(":logging_time")) {
					 loggingTime = Float.parseFloat(tokens.nextToken());
				 } else if (lable.endsWith(":tput_samples")) {
					 break;
				 }
			 }
		 } finally {
			 rd.close();
		 }
		 double med = rate / 1000 / tasks;
		 double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med*med));
		 String resultLines[] = {
				 "----- TestDFSIO ----- : " + ((testType == TEST_TYPE_WRITE) ? "write" : (testType == TEST_TYPE_READ) ? "read" : "unknown"),
				 "           Date & time: " + new Date(System.currentTimeMillis()),
				 "       Number of files: " + tasks,
				 "Total MBytes processed: " + size/MEGA,
				 "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
				 "Average IO rate mb/sec: " + med,
				 " IO rate std deviation: " + stdDev,
				 "    Test exec time sec: " + (float)execTime / 1000, "" };
		 String enhResultLines[] = {
			        "-- Extended Metrics --   : " + ((testType == TEST_TYPE_WRITE) ? "write" :
			                                        (testType == TEST_TYPE_READ) ? "read" : 
			                                    "unknown"),
			        "Result file name         : " + tputResFileName,
			        "Sampling overhead        : " + (loggingTime/time)*100 + "%",
			        "Reference Start Time     : " + String.valueOf(tStart) };
		 
		 PrintStream res = new PrintStream(new FileOutputStream(new File(resFileName), true));
		 for(int i = 0; i < resultLines.length; i++) {
			 LOG.info(resultLines[i]);
			 res.println(resultLines[i]);
		 }
		 for(int i = 0; i < enhResultLines.length; i++) {
			 LOG.info(enhResultLines[i]);
			 res.println(enhResultLines[i]);
		 }
		 
		 try {
			 fs.delete(REPORT_DIR, true);
			 //set up env
			 Configuration conf2 = new Configuration(fsConfig);
			 conf2.setLong("ana_tStart", tStart);
			 conf2.setInt("ana_plotInterval", plotInterval);
			 conf2.setLong("ana_sampleUnit", sampleUnit);
			 conf2.setLong("ana_execTime", execTime);
			 conf2.setLong("ana_fileSize", fileSize);
			 
			 Job job = new Job(conf2, "Result Analyzer");
			 job.setJarByClass(Analyzer.class);
			 job.setMapperClass(_Mapper.class);
			 job.setReducerClass(_Reducer.class);
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(Text.class);
//			 job.setNumReduceTasks(1);
			 org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, reduceFile);
			 org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, REPORT_DIR);
			 job.waitForCompletion(true);
		 } catch (InterruptedException e) {
			 e.printStackTrace();
		 } catch (ClassNotFoundException e) {
			 e.printStackTrace();
		 } finally {
			 fs.delete(REPORT_TMP, true);
			 FileUtil.copyMerge(fs, REPORT_DIR, fs, REPORT_TMP, false, fsConfig, null);
			 LOG.info("remote report file " + REPORT_TMP + " merged.");
			 BufferedReader lines = new BufferedReader(new InputStreamReader(new DataInputStream(fs.open(REPORT_TMP))));
			 String line = null;
			 while((line = lines.readLine()) != null) {
				 StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				 tokens.nextToken();
				 String ss = tokens.nextToken();
				 String[] str = ss.split(",");
				 int idx = Integer.parseInt(str[0]);
				 double val = Double.parseDouble(str[1]);
				 assert idx <= maxslot;
				 bytesTotal[idx] += val;
			 }
			 lines.close();
			 if (tputReportEach) {
				 FileUtil.copy(fs, REPORT_TMP, new File(tputResFileName.split(".")[0]+"test_io_.csv"), false, fsConfig);
				 LOG.info("*test_io_.csv fetched to local fs.");
			 }
			 //calculate the aggregated throughput
			 double[]  bytesChanged = new double[maxslot+1]; 
			 for (int i = 0; i<=maxslot-1; i++)
				 bytesChanged[i] = bytesTotal[i+1]-bytesTotal[i];
			 bytesChanged[maxslot] = 0;

			 if(tputReportTotal) {
				 PrintStream res2 = new PrintStream(new FileOutputStream (new File(tputResFileName),true));
				 for (int ri = 0; ri<=maxslot; ri++)
					 res2.println(ri+","+bytesTotal[ri]+","+bytesChanged[ri]);
				 res2.close();
			 }
			 String unit = "";
			 if (sampleUnit == KILO) 
				 unit = "kb";
			 else if (sampleUnit == MEGA)
				 unit = "mb";
			 else if (sampleUnit == 1)
				 unit = "b";
			 else if (sampleUnit == GIGA)
				 unit = "gb";

			 String[] tputResultLines = calcSummary(bytesChanged,concurrency,threshold,unit);
			 for (int j = 0; j < tputResultLines.length; j++) {
				 LOG.info(tputResultLines[j]);
				 res.println(tputResultLines[j]);
			 }
		 }
		 res.println("\n-- Result Analyse -- : " + ((System.currentTimeMillis() - t1)/1000) + "s");
		 res.close();
	 }
	 
	 @Deprecated
	 protected static void analyzeResult( FileSystem fs, 
                                     int testType,
                                     long execTime,
                                     String resFileName,
                                     int nrFiles,
                                     long fileSize,
                                     long tStart,
                                     int plotInterval,
                                     long sampleUnit,
                                     int threshold,
                                     String tputResFileName,
                                     boolean tputReportEach,
                                     boolean tputReportTotal 
                                    ) throws IOException {
    

		 //the original report
		 //TestDFSIO.analyzeResult(fs,testType,execTime,resFileName);
	
	
		 long tasks = 0;
		 long size = 0;
		 long time = 0;
		 float rate = 0;
		 float sqrate = 0;
	
		 Path reduceFile;
		 if (testType == TEST_TYPE_WRITE)
			 reduceFile = new Path(WRITE_DIR, "part-00000");
		 else
			 reduceFile = new Path(READ_DIR, "part-00000");
	    
	
		 //long time = 0;
		 float loggingTime = 0;
		 String line;
		 ArrayList<String> wrSamples = new ArrayList<String> (); 
	
		 int maxslot = (int)(execTime/plotInterval)+1;
		 int[] concurrency = new int[maxslot+1];
		 for (int i=0; i<maxslot+1; i++)
			 concurrency[i] = 0;
	
		 DataInputStream in = null;
		 BufferedReader lines = null;
		 try {
			 in = new DataInputStream(fs.open(reduceFile));
			 lines = new BufferedReader(new InputStreamReader(in));  
			 while((line = lines.readLine()) != null) {
				 StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f%");
				 String attr = tokens.nextToken(); 
				 if (attr.endsWith(":time")) {
					 time = Long.parseLong(tokens.nextToken());
				 } else if (attr.endsWith(":logging_time")) {
					 loggingTime = Float.parseFloat(tokens.nextToken());
				 } else if (attr.endsWith(":tput_samples")){
					 String[] tags=attr.split(":");
					 wrSamples.add(tags[1]);
					 wrSamples.add(tokens.nextToken());
				 } else if (attr.endsWith(":io_start_end")){
					 String[] t=tokens.nextToken().split(";");
					 int start = (int)((Long.parseLong(t[0])-tStart)/plotInterval) + 1;
					 int end = (int)((Long.parseLong(t[1])-tStart)/plotInterval) - 1;
					 if(start < 0)
						 start = 0;
					 for (int i=start; i<=end; i++){
						 if(i > concurrency.length-1)
							 break;
						 concurrency[i]++;
					 }
				 } else if (attr.endsWith(":tasks")){
					 tasks = Long.parseLong(tokens.nextToken());
				 } else if (attr.endsWith(":size")){
					 size = Long.parseLong(tokens.nextToken());
				 } else if (attr.endsWith(":rate")) {
					 rate = Float.parseFloat(tokens.nextToken());
				 } else if (attr.endsWith(":sqrate")) {
					 sqrate = Float.parseFloat(tokens.nextToken());
				 }
			 }
		 } finally {
			 if(in != null) in.close();
			 if(lines != null) lines.close();  
		 }
	
		 double med = rate / 1000 / tasks;
		 double stdDev = Math.sqrt(Math.abs(sqrate / 1000 / tasks - med*med));
		 String resultLines[] = {
				 "----- TestDFSIO ----- : " + ((testType == TEST_TYPE_WRITE) ? "write" : (testType == TEST_TYPE_READ) ? "read" : "unknown"),
				 "           Date & time: " + new Date(System.currentTimeMillis()),
				 "       Number of files: " + tasks,
				 "Total MBytes processed: " + size/MEGA,
				 "     Throughput mb/sec: " + size * 1000.0 / (time * MEGA),
				 "Average IO rate mb/sec: " + med,
				 " IO rate std deviation: " + stdDev,
				 "    Test exec time sec: " + (float)execTime / 1000, "" };
	
		 String[] tputResultLines = analyzeTputSamples(wrSamples, nrFiles, fileSize,  
	                                                    tStart, execTime, concurrency,
	                                                      plotInterval, sampleUnit, threshold,
	                                                    tputResFileName, tputReportTotal, tputReportEach);
	    
		 String enhResultLines[] = {
	        "-- Extended Metrics --   : " + ((testType == TEST_TYPE_WRITE) ? "write" :
	                                        (testType == TEST_TYPE_READ) ? "read" : 
	                                    "unknown"),
	        "Result file name         : " + tputResFileName,
	        "Sampling overhead        : " + (loggingTime/time)*100 + "%",
	        "Reference Start Time     : " + String.valueOf(tStart)
		 };
	
		 PrintStream res = new PrintStream(new FileOutputStream(new File(resFileName), true)); 
	   
		 for(int i = 0; i < resultLines.length; i++) {
			 LOG.info(resultLines[i]);
			 res.println(resultLines[i]);
		 } 
	
		 for(int i = 0; i < enhResultLines.length; i++) {
			 LOG.info(enhResultLines[i]);
			 res.println(enhResultLines[i]);
		 }
	    
		 for (int j = 0; j < tputResultLines.length; j++) {
			 LOG.info(tputResultLines[j]);
			 res.println(tputResultLines[j]);
		 }
		 res.close();
	 }

	 private static void cleanup(FileSystem fs) throws IOException {
		 LOG.info("Cleaning up test files");
		 fs.delete(new Path(TEST_ROOT_DIR), true);
	 }

}//end 


