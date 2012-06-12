package com.intel.sh.cloud.compression;

import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Compression extends Configured implements Tool {

 public static int printUsage(){
   	System.out.println("Usage: <inputDir> <outputDir>");
    ToolRunner.printGenericCommandUsage(System.out);
	return -1; 
 }


 @Override
 public int run(String[] args) throws Exception {


	int numMaps = 0;
	boolean compress = true;
    Class<? extends InputFormat> inputFormatClass =
      KeyValueTextInputFormat.class;
    Class<? extends OutputFormat> outputFormatClass =
      SequenceFileOutputFormat.class;
    Class<? extends WritableComparable> outputKeyClass = Text.class;
    Class<? extends Writable> outputValueClass = Text.class;
	Class<? extends CompressionCodec> codecClass = DefaultCodec.class;    

 	List<String> otherArgs = new ArrayList<String>();
    for(int i=0; i < args.length; ++i) {
      try {
        if ("-numMaps".equals(args[i])) {
          numMaps = Integer.parseInt(args[++i]);
	    } else if ("-nocompress".equals(args[i])) {
		  compress = false;
		} else if ("-codec".equals(args[i])){
		  codecClass =
            Class.forName(args[++i]).asSubclass(CompressionCodec.class);
        } else if ("-inFormat".equals(args[i])) {
          inputFormatClass =
            Class.forName(args[++i]).asSubclass(InputFormat.class);
        } else if ("-outFormat".equals(args[i])) {
          outputFormatClass =
            Class.forName(args[++i]).asSubclass(OutputFormat.class);
        } else if ("-outKey".equals(args[i])) {
          outputKeyClass =
            Class.forName(args[++i]).asSubclass(WritableComparable.class);
        } else if ("-outValue".equals(args[i])) {
          outputValueClass =
            Class.forName(args[++i]).asSubclass(Writable.class);
        } else {
          otherArgs.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from " +
            args[i-1]);
        return printUsage(); // exits
      }
    }

  if (otherArgs.size() < 2) {
 	 System.out.println("ERROR: Wrong number of parameters: " +
          otherArgs.size() + " instead of 2.");
  	 return printUsage();
  }
 
  Configuration conf = getConf();
  conf.set("mapred.output.compression.type","BLOCK");
  JobConf compressionJob = new JobConf(conf, Compression.class);
  compressionJob.setJobName("compression");
  
  FileInputFormat.setInputPaths(compressionJob, otherArgs.get(0));
  FileOutputFormat.setOutputPath(compressionJob, new Path(otherArgs.get(1)));
	
  if (compress) {
  	FileOutputFormat.setCompressOutput(compressionJob, true);
  	FileOutputFormat.setOutputCompressorClass(compressionJob, codecClass);
  }
  //compressionJob.setMapperClass(IdentityMapper.class);
  
  compressionJob.setInputFormat(inputFormatClass);
  compressionJob.setOutputFormat(outputFormatClass);
  compressionJob.setOutputKeyClass(outputKeyClass);
  compressionJob.setOutputValueClass(outputValueClass);
  
  JobClient client = new JobClient(compressionJob);
  
  if (numMaps != 0 ){
	if (numMaps < 0) {
 	  numMaps = client.getClusterStatus().getMaxMapTasks();
    }
	compressionJob.setNumMapTasks(numMaps);
  } 
  
  compressionJob.setNumReduceTasks(0);
  
  JobClient.runJob(compressionJob);

  return 0;
 }

 public static void main(String[] args) throws Exception {
  int res = ToolRunner.run(new Configuration(), new Compression(), args);
  System.exit(res);
 }

}



