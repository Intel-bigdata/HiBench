/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hibench;

import static org.apache.hadoop.mapred.Task.Counter.MAP_OUTPUT_RECORDS;

import hibench.MapReduceSet.ConcatTextCombiner;
import hibench.MapReduceSet.CreateHtmlPagesMapper;
import hibench.MapReduceSet.CreateZipfDistrMapper;
import hibench.MapReduceSet.HtmlMultipleTextOutputFormat;
import hibench.MapReduceSet.JoinContentWithZipfReducer;
import hibench.MapReduceSet.ReverseContentMapper;
import hibench.MapReduceSet.SumUpZipfMapper;
import hibench.MapReduceSet.SumUpZipfReducer;
import hibench.MapReduceSet.TagRecordsMapper;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public abstract class DataGenerator {
	public abstract void initGenerator() throws IOException;
	public abstract void genHtmlPages() throws IOException;
	public abstract void genResultData() throws Exception;
	public abstract void closeGenerator() throws IOException;

	protected static Log LOG;

	DataGenerator () {
		LOG = LogFactory.getLog(WebDataGen.class.getName());
	}

	public void generateData () throws Exception {

		initGenerator();
		genHtmlPages();
		genResultData();
		closeGenerator();
	}

	public void createDummy(Path dummy, int agents) throws IOException {

		LOG.info("Creating dummy file " + dummy + "with " + agents + " agents...");

		DataPaths.checkHdfsFile(dummy, false);
		
		FileSystem fs = dummy.getFileSystem(new Configuration());
		FSDataOutputStream out = fs.create(dummy);

		String dummyContent = "";
		for (int i=1; i<=agents; i++) {
			dummyContent = dummyContent.concat(Integer.toString(i) + "\n");
		}

		out.write(dummyContent.getBytes("UTF-8"));
		out.close();
	}
	
	public void sumUpZipf (Path fin, Path fout, ZipfRandom zipf) throws IOException
	{
		LOG.info("Summing up Zipfian Id Distirubtion...");

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = fin.getName() + " -> " + fout.getName();
		job.setJobName(jobname);
		
		zipf.setJobConf(job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(SumUpZipfMapper.class);
		job.setReducerClass(SumUpZipfReducer.class);

		job.setNumReduceTasks(1);	// Important to sequentially accumulate the required space

		job.setInputFormat(NLineInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		DataPaths.checkHdfsFile(fout, false);

		FileInputFormat.setInputPaths(job,  fin);
		FileOutputFormat.setOutputPath(job, fout);

		LOG.info("Running Job: " +jobname);
		LOG.info("Dummy file: " + fin);
		LOG.info("Zipfian sum up file as Ouput: " + fout);
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);
	}

	public void createZipf(Path fin, Path fout, ZipfRandom zipf) throws IOException
	{
		LOG.info("Creating Zipfian Id Distirubtion...");

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = fin.getName() + " -> " + fout.getName();

		job.setJobName(jobname);

		zipf.setJobConf(job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CreateZipfDistrMapper.class);

		job.setNumReduceTasks(0);

		job.setInputFormat(NLineInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, fin);
		FileOutputFormat.setOutputPath(job, fout);

		DataPaths.checkHdfsFile(fout, false);

		LOG.info("Running Job: " + jobname);
		LOG.info("Zipfian Sum File: " + fin);
		LOG.info("Zipfian Id distribution as Ouput: " + fout);
		RunningJob jobCreateZipf = JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);
		
		long vElems = jobCreateZipf.getCounters().getCounter(MAP_OUTPUT_RECORDS);
		LOG.info("Created " + vElems + " virtual zipfian elements");
		zipf.setVirtElems(vElems);
	}

	public void genZipfDist (Path dummy, Path zipfSum, Path zipfDist, ZipfRandom zipf) throws IOException {
		sumUpZipf(dummy, zipfSum, zipf);
		createZipf(zipfSum, zipfDist, zipf);
	}

	public void replaceIds(Path fcontent, Path fids, Path fjoin, ZipfRandom zipf)
			throws IOException {
		
		LOG.info("Replace Virtual Zipfian Ids with real Ids...");
		
		JobConf job = new JobConf(WebDataGen.class);
		String jobname = fcontent.getName() + " JOIN " +
				fids.getName() + " -> " + fjoin.getName();
		
		job.setJobName(jobname);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, fids, TextInputFormat.class, TagRecordsMapper.class);
		MultipleInputs.addInputPath(job, fcontent, TextInputFormat.class, ReverseContentMapper.class);
		job.setOutputFormat(TextOutputFormat.class);

		// use combiner to avoid too many inputs for reducer
		job.setCombinerClass(ConcatTextCombiner.class);
		job.setReducerClass(JoinContentWithZipfReducer.class);
		
		if (zipf.reds>0) {
			job.setNumReduceTasks(zipf.reds);
		} else {
			job.setNumReduceTasks(DataOptions.getMaxNumReduce());
		}

		FileOutputFormat.setOutputPath(job, fjoin);
		
		LOG.info("Running Job: " + jobname);
		LOG.info("Zipfian Id distribution: " + fids);
		LOG.info("Content file with virtual Ids: " + fcontent);
		LOG.info("Joint result file: " + fjoin);
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);
	}

	public void createHtmlPages(Path dummy, HtmlConf html) throws IOException {

		LOG.info("Creating Html Pages...");

		Path fout = new Path(dummy.getParent(), "tmp");

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = "Create html pages to " + fout.getName();

		job.setJobName(jobname);

		html.setJobConf(job);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(CreateHtmlPagesMapper.class);

		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, dummy);

		// first create result files under tmp folder
		FileOutputFormat.setOutputPath(job, fout);

		// begin from dummy file
		job.setInputFormat(NLineInputFormat.class);

		// use MultipleTextOutputFormat to produce three out files defined
		// in PathConf, i.e., LINK, PAGE_ZLINK_TABLE, PAGE_ZWORD_TABLE
		job.setOutputFormat(HtmlMultipleTextOutputFormat.class);

		LOG.info("Running Job: " + jobname);
		LOG.info("Dummy file: " + dummy);
		LOG.info("Multiple result Html files as <links, words, urls>");
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);

		// Move result files under tmp into parent path
		// and remove the empty tmp path finally 
		DataPaths.moveFilesToParent(fout);
	}
}
