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

import hibench.MapReduceSet.BalancedLinkNodesMapper;
import hibench.MapReduceSet.BalancedLinkNodesReducer;
//import hibench.MapReduceSet.ModulusPartitioner;
import hibench.MapReduceSet.OutputLinkEdgesMapper;
import hibench.MapReduceSet.OutputLinkNodesMapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class PageRankDataGenerator extends DataGenerator {

	public DataOptions options;
	public DataPaths paths;
	public HtmlConf html;

	PageRankDataGenerator(DataOptions hiveOptions) {

		options = hiveOptions;
		paths = new DataPaths(options);
		html = new HtmlConf(options);
	}

	@Override
	public void initGenerator() throws IOException {
		
		LOG.info("Initializing PageRank data generator...");
		
		DataPaths.checkHdfsFile(paths.result, true);
		createDummy(paths.getPath(DataPaths.DUMMY), options.maps);
		genZipfDist(
				paths.getPath(DataPaths.DUMMY),
				paths.getPath(DataPaths.ZLINK_SUM),
				paths.getPath(DataPaths.ZLINK_DIST),
				html.linkZipf);

		LOG.info("---Html Conf---\n" + html.debugInfo());
		
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

	private void createPageRankNodes() throws IOException {

		LOG.info("Creating PageRank nodes...", null);

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = "Create " + paths.dname + " pagerank nodes";

		job.setJobName(jobname);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, paths.getPath(DataPaths.LINKS));
		job.setInputFormat(TextInputFormat.class);
		
		if (options.PAGERANK_NODE_BALANCE) {
			/***
			 * Balance the output order of nodes, to prevent the running
			 * of pagerank bench from potential data skew
			 */
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setMapperClass(BalancedLinkNodesMapper.class);
			job.setReducerClass(BalancedLinkNodesReducer.class);
//			job.setPartitionerClass(ModulusPartitioner.class);

			if (options.reds>0) {
				job.setNumReduceTasks(options.reds);
			} else {
				job.setNumReduceTasks(DataOptions.getMaxNumReduce());
			}
		} else {
			job.setMapOutputKeyClass(Text.class);
			
			job.setMapperClass(OutputLinkNodesMapper.class);
			job.setNumReduceTasks(0);
		}

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
		
		FileOutputFormat.setOutputPath(job, paths.getResult(DataPaths.VERTICALS));

		LOG.info("Running Job: " +jobname);
		LOG.info("Links file " + paths.getPath(DataPaths.LINKS) + " as input");
		LOG.info("Vertices file " + paths.getResult(DataPaths.VERTICALS) + " as output");
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);

		LOG.info("Cleaning temp files...");
		paths.cleanTempFiles(paths.getResult(DataPaths.VERTICALS));
	}


	/***
	 * Create pagerank edge table, output link A->B as <A, B> pairs
	 * @throws IOException
	 */
	private void createPageRankLinks() throws IOException {

		LOG.info("Creating PageRank links", null);

		JobConf job = new JobConf(WebDataGen.class);
		String jobname = "Create " + paths.dname + " pagerank links";

		job.setJobName(jobname);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, paths.getPath(DataPaths.T_LINK_PAGE));
		job.setInputFormat(TextInputFormat.class);

		job.setMapperClass(OutputLinkEdgesMapper.class);

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
		
		FileOutputFormat.setOutputPath(job, paths.getResult(DataPaths.EDGES));
		
		LOG.info("Running Job: " +jobname);
		LOG.info("Table link-page " + paths.getPath(DataPaths.T_LINK_PAGE) + " as input");
		LOG.info("Edges file " + paths.getResult(DataPaths.EDGES) + " as output");
		JobClient.runJob(job);
		LOG.info("Finished Running Job: " + jobname);

		LOG.info("Cleaning temp files...");
		paths.cleanTempFiles(paths.getResult(DataPaths.EDGES));
	}

	@Override
	public void genResultData() throws Exception {
		
		LOG.info("Generating PageRank data files...");
		createPageRankNodes();
		createPageRankLinks();
	}

	@Override
	public void closeGenerator() throws IOException {

		LOG.info("Closing PageRank data generator...");
		if (!options.OPEN_DEBUG) {
			paths.cleanWorkDir();
		}
	}
}
