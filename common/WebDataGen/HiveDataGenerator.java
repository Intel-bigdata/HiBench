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

import hibench.MapReduceSet.*;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class HiveDataGenerator extends DataGenerator {

	public DataOptions options;
	public DataPaths paths;
	public HtmlConf html;
	public VisitConf visit;

	HiveDataGenerator(DataOptions hiveOptions) {

		options = hiveOptions;
		paths = new DataPaths(options);
		html = new HtmlConf(options);
		visit = new VisitConf(options);
	}
	
	private void createRankingsTable() throws IOException {

		LOG.info("Creating table rankings...");

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = "Create " + paths.dname + " rankings";

		job.setJobName(jobname);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setCombinerClass(ConcatTextCombiner.class);
		job.setReducerClass(CountRankingAndReplaceIdReducer.class);

		if (options.reds>0) {
			job.setNumReduceTasks(options.reds);
		} else {
			job.setNumReduceTasks(DataOptions.getMaxNumReduce());
		}

//		job.setNumReduceTasks(options.agents/2);

		/***
		 * need to join result with LINK table so that to replace
		 * url ids with real contents
		 */
		MultipleInputs.addInputPath(job, paths.getPath(DataPaths.T_LINK_PAGE),
				TextInputFormat.class, MyIdentityMapper.class);
		MultipleInputs.addInputPath(job, paths.getPath(DataPaths.LINKS),
				TextInputFormat.class, TagRecordsMapper.class);

		if (options.SEQUENCE_OUT) {
			job.setOutputFormat(SequenceFileOutputFormat.class);
		} else {
			job.setOutputFormat(TextOutputFormat.class);
		}
		
		if (null != options.codecClass) {
			job.set("mapred.output.compression.type","BLOCK");
			
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, options.codecClass);
		}
		
		FileOutputFormat.setOutputPath(job, paths.getResult(DataPaths.RANKINGS));

		LOG.info("Running Job: " +jobname);
		LOG.info("Table link-page file " + paths.getPath(DataPaths.T_LINK_PAGE) + " as input");
		LOG.info("Links file " + paths.getResult(DataPaths.LINKS) + " as output");
		LOG.info("Ouput file " + paths.getResult(DataPaths.RANKINGS));
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);

		LOG.info("Cleaning temp files...");
		paths.cleanTempFiles(paths.getResult(DataPaths.RANKINGS));
	}

	private void createUserVisitsTable() throws IOException, URISyntaxException {

		LOG.info("Creating user visits...");

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = "Create " + paths.dname + " uservisits";
		job.setJobName(jobname);

		/***
		 * Set distributed cache file for table generation,
		 * cache files include:
		 * 1. user agents
		 * 2. country code and language code
		 * 3. search keys
		 */

		DistributedCache.addCacheFile(paths.getPath(DataPaths.uagentf).toUri(), job);
		DistributedCache.addCacheFile(paths.getPath(DataPaths.countryf).toUri(), job);
		DistributedCache.addCacheFile(paths.getPath(DataPaths.searchkeyf).toUri(), job);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		visit.setJobConf(job);
		
		job.setInputFormat(TextInputFormat.class);

		MultipleInputs.addInputPath(job, paths.getPath(DataPaths.DUMMY),
				NLineInputFormat.class, CreateRandomAccessMapper.class);
		MultipleInputs.addInputPath(job, paths.getPath(DataPaths.LINKS),
				TextInputFormat.class, TagRecordsMapper.class);

		job.setCombinerClass(CreateUserVisitsCombiner.class);
		job.setReducerClass(CreateUserVisitsReducer.class);
		
		if (options.reds>0) {
			job.setNumReduceTasks(options.reds);
		} else {
			job.setNumReduceTasks(DataOptions.getMaxNumReduce());
		}

//		job.setNumReduceTasks(options.agents/2);

		if (options.SEQUENCE_OUT) {
			job.setOutputFormat(SequenceFileOutputFormat.class);
		} else {
			job.setOutputFormat(TextOutputFormat.class);
		}
		
		if (null != options.codecClass) {
			job.set("mapred.output.compression.type","BLOCK");
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, options.codecClass);
		}
		
		FileOutputFormat.setOutputPath(job, paths.getResult(DataPaths.USERVISITS));
		
		LOG.info("Running Job: " +jobname);
		LOG.info("Dummy file " + paths.getPath(DataPaths.DUMMY) + " as input");
		LOG.info("Links file " + paths.getResult(DataPaths.LINKS) + " as output");
		LOG.info("Ouput file " + paths.getResult(DataPaths.USERVISITS));
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);

		LOG.info("Cleaning temp files...");
		paths.cleanTempFiles(paths.getResult(DataPaths.USERVISITS));
	}

	@Override
	public void genResultData() throws Exception {
		
		LOG.info("Generating Hive data files...");
		
		createRankingsTable();
		createUserVisitsTable();
	}
	
	public void loadFiles() throws IOException {
		SourceGenerator.createSouceSearchKeys(paths.getPath(DataPaths.searchkeyf));
		SourceGenerator.createSourceUserAgents(paths.getPath(DataPaths.uagentf));
		SourceGenerator.createSourceCCodes(paths.getPath(DataPaths.countryf));
	}

	@Override
	public void initGenerator() throws IOException {

		LOG.info("Initializing Hive date generator...");

		DataPaths.checkHdfsFile(paths.result, true);
		loadFiles();
		createDummy(paths.getPath(DataPaths.DUMMY), options.maps);
		
		genZipfDist(
				paths.getPath(DataPaths.DUMMY),
				paths.getPath(DataPaths.ZLINK_SUM),
				paths.getPath(DataPaths.ZLINK_DIST),
				html.linkZipf);

		LOG.info("---Html Conf---\n" + html.debugInfo());
		LOG.info("---Visit Conf---\n" + visit.debugInfo());

	}

	@Override
	public void genHtmlPages() throws IOException {

		LOG.info("Creating Html pages...");

		createHtmlPages (paths.getPath(DataPaths.DUMMY), html);
		replaceIds(
				paths.getPath(DataPaths.T_PAGE_ZLINK),
				paths.getPath(DataPaths.ZLINK_DIST),
				paths.getPath(DataPaths.T_LINK_PAGE),
				html.linkZipf);
	}

	@Override
	public void closeGenerator() throws IOException {

		LOG.info("Closing Hive data generator...");
		
		if (!options.OPEN_DEBUG) {
			paths.cleanWorkDir();
		}
	}
}
