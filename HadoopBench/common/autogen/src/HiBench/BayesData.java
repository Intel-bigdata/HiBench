package HiBench;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class BayesData {

	private static final Log log = LogFactory.getLog(BayesData.class.getName());
	
	private DataOptions options;
	private Dummy dummy;
	private int cgroups;
	
	BayesData(DataOptions options) {
		this.options = options;
		parseArgs(options.getRemainArgs());
	}
	
	private void parseArgs(String[] args) {
		
		for (int i=0; i<args.length; i++) {
			if ("-class".equals(args[i])) {
				cgroups = Integer.parseInt(args[++i]);
			} else {
				DataOptions.printUsage("Unknown bayes data arguments -- " + args[i] + "!!!");
				System.exit(-1);
			}
		}
	}

	private static class CreateBayesPages extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {

		private static final Log log = LogFactory.getLog(CreateBayesPages.class.getName());
		
		private long pages, slotpages;
		private int groups;
		private HtmlCore generator;
		private Random rand;

		public void configure(JobConf job) {
			try {
				pages = job.getLong("pages", 0);
				slotpages = job.getLong("slotpages", 0);
				groups = job.getInt("groups", 0);
				
				generator = new HtmlCore(job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			int slotId = Integer.parseInt(value.toString().trim());
			long[] range = HtmlCore.getPageRange(slotId, pages, slotpages);
			generator.fireRandom(slotId);
			rand = new Random(slotId * 1000 + 101);
			
			Text k = new Text();
			for (long i=range[0]; i<range[1]; i++) {
				String classname = "/class" + rand.nextInt(groups);
				k.set(classname);
				value.set(generator.genBayesWords());
				output.collect(k, value);
				reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED,
					k.getLength()+value.getLength());
				if (0==(i % 10000)) {
					log.info("still running: " + (i - range[0]) + " of " + slotpages);
				}
			}
		}
	}
	
	private void setBayesOptions(JobConf job) throws URISyntaxException {
		job.setLong("pages", options.getNumPages());
		job.setLong("slotpages", options.getNumSlotPages());
		job.setInt("groups", cgroups);
		
		Utils.shareWordZipfCore(options, job);
	}
	
	private void createBayesData() throws IOException, URISyntaxException {
		
		log.info("creating bayes text data ... ");

		JobConf job = new JobConf();

		Path fout = options.getResultPath();
		Utils.checkHdfsPath(fout);
		
		String jobname = "Create bayes data";
		job.setJobName(jobname);

		Utils.shareDict(options, job);
		
		setBayesOptions(job);
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);

		job.setJarByClass(CreateBayesPages.class);
		job.setMapperClass(CreateBayesPages.class);
		job.setNumReduceTasks(0);
		
		FileOutputFormat.setOutputPath(job, fout);
		job.setOutputFormat(SequenceFileOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		log.info("Running Job: " +jobname);
		log.info("Pages file " + dummy.getPath() + " as input");
		log.info("Rankings file " + fout + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}
	
	private void init() throws IOException {

		Utils.checkHdfsPath(options.getResultPath(), true);
		Utils.checkHdfsPath(options.getWorkPath(), true);

		dummy = new Dummy(options.getWorkPath(), options.getNumMaps());

		int words = RawData.putDictToHdfs(new Path(options.getWorkPath(), HtmlCore.getDictName()), options.getNumWords());
		options.setNumWords(words);
		
		Utils.serialWordZipf(options);
	}

	public void generate() throws Exception {
		
		init();
		
		createBayesData();
		
		close();
	}

	private void close() throws IOException {
		log.info("Closing bayes data generator...");
		Utils.checkHdfsPath(options.getWorkPath());
	}
}
