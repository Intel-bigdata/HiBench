package HiBench;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class PagerankData {

	private static final Log log = LogFactory.getLog(PagerankData.class.getName());
	
	private DataOptions options;

	private static final String VERTICALS_DIR_NAME = "vertices";
	private static final String EDGES_DIR_NAME = "edges";	
	private boolean balance = false;  //	original PAGERANK_NODE_BALANCE

	private String cdelim = "\t";

	private Dummy dummy;

	PagerankData(DataOptions options) {
		this.options = options;
		parseArgs(options.getRemainArgs());
	}

	private void parseArgs(String[] args) {
		
		for (int i=0; i<args.length; i++) {

			if ("-d".equals(args[i])) {
				cdelim = args[++i];
			} else if ("-pbalance".equals(args[i])) {
				balance = true;
			} else {
				DataOptions.printUsage("Unknown pagerank data arguments -- " + args[i] + "!!!");
			}
		}
	}
	
	public void init() throws IOException {
		
		log.info("Initializing PageRank data generator...");
		
		Utils.checkHdfsPath(options.getResultPath(), true);
		Utils.checkHdfsPath(options.getWorkPath(), true);

		Utils.serialLinkZipf(options);

		dummy = new Dummy(options.getWorkPath(), options.getNumMaps());
	}

	private void setPageRankNodesOptions(JobConf job) {
		job.setLong("pages", options.getNumPages());
		job.setLong("slotpages", options.getNumSlotPages());
	}
	
	private void setPageRankLinksOptions(JobConf job) throws URISyntaxException {
		job.setLong("pages", options.getNumPages());
		job.setLong("slotpages", options.getNumSlotPages());
		job.set("delimiter", cdelim);
		
		Utils.shareLinkZipfCore(options, job);
	}
	
	public static class BalancedLinkNodesMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, NullWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {
	
			String delimiter = "[ \t]";
			String[] pair = value.toString().split(delimiter);
			
			output.collect(
					new LongWritable(Long.parseLong(pair[0])),
					NullWritable.get()
					);
		}
	}

	public static class BalancedLinkNodesReducer extends MapReduceBase implements
	Reducer<LongWritable, NullWritable, NullWritable, Text> {

		@Override
		public void reduce(LongWritable key, Iterator<NullWritable> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
						throws IOException {
	
			output.collect(NullWritable.get(), new Text(key.toString()));
		}
	}

	public static class DummyToNodesMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {
		
		private long pages, slotpages;

		private void getOptions(JobConf job) {
			pages = job.getLong("pages", 0);
			slotpages = job.getLong("slotpages", 0);
		}

		@Override
		public void configure(JobConf job) {
			getOptions(job);
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
	
			int slotId = Integer.parseInt(value.toString().trim());
			long[] range = HtmlCore.getPageRange(slotId, pages, slotpages);
			
			for (long i=range[0]; i<range[1]; i++) {
				key.set(i);
				Text v = new Text(Long.toString(i));
				output.collect(key, v);
				reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED, 8+v.getLength());
			}
		}
	}

	private void createPageRankNodesDirectly() throws IOException {

		log.info("Creating PageRank nodes...", null);

		Path fout = new Path(options.getResultPath(), VERTICALS_DIR_NAME);
		
		JobConf job = new JobConf(PagerankData.class);
		String jobname = "Create pagerank nodes";

		job.setJobName(jobname);
		setPageRankNodesOptions(job);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);
		
		if (balance) {
			/***
			 * Balance the output order of nodes, to prevent the running
			 * of pagerank bench from potential data skew
			 */
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setMapperClass(BalancedLinkNodesMapper.class);
			job.setReducerClass(BalancedLinkNodesReducer.class);
//			job.setPartitionerClass(ModulusPartitioner.class);

			if (options.getNumReds() > 0) {
				job.setNumReduceTasks(options.getNumReds());
			} else {
				job.setNumReduceTasks(Utils.getMaxNumReds());
			}
		} else {
			job.setMapOutputKeyClass(Text.class);
			job.setMapperClass(DummyToNodesMapper.class);
			job.setNumReduceTasks(0);
		}

		if (options.isSequenceOut()) {
			job.setOutputFormat(SequenceFileOutputFormat.class);
		} else {
			job.setOutputFormat(TextOutputFormat.class);
		}
		
		if (null != options.getCodecClass()) {
			job.set("mapred.output.compression.type","BLOCK");
			job.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, options.getCodecClass());
		}
		
		FileOutputFormat.setOutputPath(job, fout);

		log.info("Running Job: " +jobname);
		log.info("Dummy file " + dummy.getPath() + " as input");
		log.info("Vertices file " + fout + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}

	public static class DummyToPageRankLinksMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {

		private static final Log log = LogFactory.getLog(DummyToPageRankLinksMapper.class.getName());
		private HtmlCore html;
		private long pages, slotpages;
		private String delim;

		private void getOptions(JobConf job) {
			pages = job.getLong("pages", 0);
			slotpages = job.getLong("slotpages", 0);
			delim = job.get("delimiter");
		}

		public void configure(JobConf job) {

			try {
				html = new HtmlCore(job);
				
				getOptions(job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output,
				Reporter reporter) throws IOException {

			int slotId = Integer.parseInt(value.toString().trim());
			html.fireRandom(slotId);

			long[] range = HtmlCore.getPageRange(slotId, pages, slotpages);

			/**
			 * For output collect
			 */
			for (long i=range[0]; i<range[1]; i++) {
				key.set(i);
				
				long[] linkids = html.genPureLinkIds();
				for (int j=0; j<linkids.length; j++) {
					String to = Long.toString(linkids[j]);
					Text v = new Text(to);
					output.collect(key, v);
					reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED, 8+v.getLength());
				}
				
				if (0==(i % 10000)) {
					log.info("still running: " + (i - range[0]) + " of " + slotpages);
				}
			}
		}
	}

	private void createPageRankLinksDirectly() throws IOException, URISyntaxException {

		log.info("Creating PageRank links", null);

		JobConf job = new JobConf(PagerankData.class);
		String jobname = "Create pagerank links";

		Path fout = new Path(options.getResultPath(), EDGES_DIR_NAME);

		job.setJobName(jobname);
		setPageRankLinksOptions(job);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
//		job.setMapOutputKeyClass(LongWritable.class);
//		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);

		job.setMapperClass(DummyToPageRankLinksMapper.class);

		if (options.isSequenceOut()) {
			job.setOutputFormat(SequenceFileOutputFormat.class);
		} else {
			job.setOutputFormat(TextOutputFormat.class);
		}
		
		if (null != options.getCodecClass()) {
			job.set("mapred.output.compression.type","BLOCK");
			job.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
			FileOutputFormat.setCompressOutput(job, true);
			FileOutputFormat.setOutputCompressorClass(job, options.getCodecClass());
		}
		
		FileOutputFormat.setOutputPath(job, fout);
		
		log.info("Running Job: " +jobname);
		log.info("Dummy file " + dummy.getPath() + " as input");
		log.info("Edges file " + fout + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}

	public void generate() throws IOException, URISyntaxException {
		
		log.info("Generating pageRank data files...");
		init();
		createPageRankNodesDirectly();
		createPageRankLinksDirectly();
		closeGenerator();
	}

	private void closeGenerator() throws IOException {

		log.info("Closing pagerank data generator...");
		Utils.checkHdfsPath(options.getWorkPath(), true);
	}
}
