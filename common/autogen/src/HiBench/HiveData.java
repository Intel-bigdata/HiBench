package HiBench;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class HiveData {

	private static final Log log = LogFactory.getLog(HiveData.class.getName());
	
	private static final String RANKINGS = "rankings";
	private static final String USERVISITS = "uservisits";
	public static final String uagentf = "user_agents";
	public static final String countryf = "country_codes";
	public static final String searchkeyf = "search_keys";
	
	private DataOptions options;
	private long visits;
	
	// client side delim
	private String cdelim = ",";
	private int chashsize = 150 * 1024 * 1024;
	
	private Dummy dummy;

	HiveData(DataOptions options) {
		this.options = options;
		parseArgs(options.getRemainArgs());
	}
	
	private void parseArgs(String[] args) {
		
		for (int i=0; i<args.length; i++) {
			if ("-v".equals(args[i])) {
				visits = Long.parseLong(args[++i]);
			} else if ("-d".equals(args[i])) {
				cdelim = args[++i];
			} else {
				DataOptions.printUsage("Unknown hive data arguments -- " + args[i] + "!!!");
			}
		}
		
		if (chashsize > options.getNumPages()) {
			chashsize = (int) options.getNumPages();
		}

	}
	
	private void setRankingsOptions(JobConf job) throws URISyntaxException {
		job.setLong("pages", options.getNumPages());
		job.setLong("slotpages", options.getNumSlotPages());
		job.set("delimiter", cdelim);
		job.setInt("hashsize", chashsize);
		Utils.shareLinkZipfCore(options, job);
	}
	
	private void setVisitsOptions(JobConf job) {
		job.setInt("slots", options.getNumMaps());
		job.setLong("pages", options.getNumPages());
		job.setLong("visits", visits);
		job.set("delimiter", cdelim);
	}

	public static class DummyToRankingsMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, JoinBytesInt> {

		private static final Log log = LogFactory.getLog(DummyToRankingsMapper.class.getName());
		
		private HtmlCore generator;
		private long pages, slotpages;
		private boolean outset;
		private OutputCollector<LongWritable, JoinBytesInt> myout;
		private JoinBytesInt uitem, ritem;
		private short[] hash;
		private HashMap<Integer, Integer> hm;
		private int hashsize;
		
		private void getOptions(JobConf job) {
			pages = job.getLong("pages", 0);
			slotpages = job.getLong("slotpages", 0);
			hashsize = job.getInt("hashsize", 0);
		}

		public void configure(JobConf job) {

			getOptions(job);
	
			try {
				generator = new HtmlCore(job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			outset = false;
			myout = null;
			uitem = new JoinBytesInt();
			uitem.url = new byte[HtmlCore.getMaxUrlLength()];
			ritem = new JoinBytesInt();
			ritem.refs = 1;
			
			hash = new short[hashsize];
			hm = new HashMap<Integer, Integer>();
		}
	
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, JoinBytesInt> output,
				Reporter reporter) throws IOException {

			if (!outset) {
				myout = output;
				outset = true;
			}

			int slotId = Integer.parseInt(value.toString().trim());
			generator.fireRandom(slotId);

			long[] range = HtmlCore.getPageRange(slotId, pages, slotpages);

			/**
			 * For output collect
			 */
			for (long i=range[0]; i<range[1]; i++) {
				key.set(i);

				generator.nextUrlJoinBytesInt(uitem);
				output.collect(key, uitem);
				
				long[] linkids = generator.genPureLinkIds();
				for (int j=0; j<linkids.length; j++) {
					long uid = linkids[j];
					if (uid < hashsize) {
						int iid = (int) uid;
						if (hash[iid]>=0) {
							if (hash[iid]==HtmlCore.MAX_SHORT) {
								hm.put(iid, (int) (hash[iid]) + 1);
								hash[iid] = -1;
							} else {
								hash[iid]++;
							}
						} else {
							hm.put(iid, hm.get(iid) + 1);
						}
					} else {
						key.set(uid); 
						output.collect(key, ritem);
					}
				}
				
				if (0==(i % 10000)) {
					log.info("still running: " + (i - range[0]) + " of " + slotpages);
				}
			}
		}
		
		@Override
		public void close ()
		{
			try {
				LongWritable k = new LongWritable();
				for (int i=0; i<hash.length; i++) {
					if (hash[i] > 0) {
						k.set(i);
						ritem.refs = hash[i];
						myout.collect(k, ritem);
					} else if (hash[i] < 0) {
						k.set(i);
						ritem.refs = hm.get(i);
						myout.collect(k, ritem);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static class JoinBytesIntCombiner extends MapReduceBase implements
	Reducer<LongWritable, JoinBytesInt, LongWritable, JoinBytesInt> {

//		Log log = null;
		JoinBytesInt item;
		
		@Override
		public void configure (JobConf job)
		{
			item = new JoinBytesInt();
//			log = LogFactory.getLog(JoinBytesIntCombiner.class.getName());
		}

		@Override
		public void reduce(LongWritable key, Iterator<JoinBytesInt> values,
				OutputCollector<LongWritable, JoinBytesInt> output, Reporter reporter) throws IOException {

			item.clear();
//			StringBuffer sb =  new StringBuffer("Combine: " + v.toString());
			while (values.hasNext()) {
				item.add(values.next());
//				sb.append("-> " + v.toString());
			}
			output.collect(key, item);
//			log.info(sb);
		}
	}
	
	public static class GenerateRankingsReducer extends MapReduceBase implements
	Reducer<LongWritable, JoinBytesInt, LongWritable, Text> {
		
		private static final Log log = LogFactory.getLog(GenerateRankingsReducer.class.getName());

		private Random rand;
		private int errors, missed;
		private JoinBytesInt v;
		private int pid;
		
		// job side delimiter
		private String delim;
//		private String missedids;
		
		public void configure (JobConf job)
		{
			delim = job.get("delimiter");
			pid = job.getInt("mapred.task.partition", 0);
			rand = new Random(pid + 1);

			v = new JoinBytesInt();

			errors = 0;
			missed = 0;
//			missedids = "";
		}

		public void close ()
		{
			log.info("pid: " + pid + ", " + errors + " erros, " + missed + " missed");
		}

		@Override
		public void reduce(LongWritable key, Iterator<JoinBytesInt> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {

			v.clear();
			while (values.hasNext()) {
				v.add(values.next());
			}
			
			if (0!=v.ulen) {
				if (v.refs > 0) {
					Text value = new Text(
							new String(v.url) +
							delim +
							v.refs +
							delim +
							(rand.nextInt(99) + 1)
							);
					output.collect(
							key, value);
							
					reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED, 8+value.getLength());
				} else {
					missed++;
				}
			} else {
				errors++;					
			}
		}
	}

	private void createRankingsTableDirectly() throws IOException, URISyntaxException {

		log.info("Creating table rankings...");

		Path fout = new Path(options.getResultPath(), RANKINGS);

		JobConf job = new JobConf(HiveData.class);
		String jobname = "Create rankings";

		/** TODO: change another more effective way as this operation may cause
		 *  about 2 min delay (originally ~15min in total)
		 */
		setRankingsOptions(job);
		job.setJobName(jobname);
		job.set("mapred.reduce.slowstart.completed.maps", "0.3");
		job.set("mapreduce.job.reduce.slowstart.completedmaps", "0.3");

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JoinBytesInt.class);

		job.setJarByClass(DummyToRankingsMapper.class);
		job.setJarByClass(JoinBytesIntCombiner.class);
		job.setJarByClass(GenerateRankingsReducer.class);
		
		job.setMapperClass(DummyToRankingsMapper.class);
		job.setCombinerClass(JoinBytesIntCombiner.class);
		job.setReducerClass(GenerateRankingsReducer.class);

		if (options.getNumReds() > 0) {
			job.setNumReduceTasks(options.getNumReds());
		} else {
			job.setNumReduceTasks(Utils.getMaxNumReds());
		}

		job.setInputFormat(NLineInputFormat.class);
		FileInputFormat.setInputPaths(job, dummy.getPath());

		job.set("mapred.map.output.compression.type", "BLOCK");
	 	job.set("mapreduce.output.fileoutputformat.compress.type","BLOCK");	
		MapFileOutputFormat.setCompressOutput(job, true);
//		MapFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.LzoCodec.class);
		MapFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.DefaultCodec.class);

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
		log.info("Pages file " + dummy.getPath() + " as input");
		log.info("Rankings file " + fout + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}

	/***
	 * Mapper to randomly create user visits. In map step, only the target
	 * urls of user visits are created, the rest content of visits will be
	 * created in reduce step
	 * @author lyi2
	 *
	 */
	public static class DummyToAccessNoMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, JoinBytesInt> {
	
		private JoinBytesInt vitem;
		private long pages;
		private long slots;
		private long visits;
		
		// job side delimiter
		private String delim;
		private Visit visit;
	
		public void configure (JobConf job)
		{
			try {
				pages = job.getLong("pages", 0);
				slots = job.getLong("slots", 0);
				visits = job.getLong("visits", 0);
				delim = job.get("delimiter");
	
				visit = new Visit(DistributedCache.getLocalCacheFiles(job),
						delim, pages);
				
				vitem = new JoinBytesInt();
				vitem.refs = 1;
	
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, JoinBytesInt> output, Reporter reporter)
						throws IOException {
	
			int slotId = Integer.parseInt(value.toString().trim());
			visit.fireRandom(slotId);

			for (long i=slotId; i<=visits;) {
				// simply setting url id is fine in map step
				key.set(visit.nextUrlId());
				output.collect(key, vitem);
				i = i + slots;
			}
		}
	}

	public static class SequenceRankingsToUrlsMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, JoinBytesInt> {
		public JoinBytesInt uitem;
		
		public void configure(JobConf job) {
			uitem = new JoinBytesInt();
//			getBasicOptions(job);
		}
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, JoinBytesInt> output, Reporter reporter) throws IOException {
			
			uitem.url= value.toString().split(",")[0].getBytes();
			uitem.ulen = (byte) uitem.url.length;
			
			output.collect(key, uitem);
		}
	}
	
	public static class TextRankingsToUrlsMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, JoinBytesInt> {
		public JoinBytesInt uitem;
		
		public void configure(JobConf job) {
			uitem = new JoinBytesInt();
//			getBasicOptions(job);
		}
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, JoinBytesInt> output, Reporter reporter) throws IOException {
			
			String[] items = value.toString().split("[,\t]");
			key.set(Long.parseLong(items[0]));
			uitem.url= items[1].getBytes();
			uitem.ulen = (byte) uitem.url.length;
			
			output.collect(key, uitem);
		}
	}
	
	public static class CreateUserVisitsReducer extends MapReduceBase implements
	Reducer<LongWritable, JoinBytesInt, LongWritable, Text> {
	
		private static final Log log = LogFactory.getLog(CreateUserVisitsReducer.class.getName());
		
		private long pages;
		private Visit visit;

		private int errors, missed;
		private JoinBytesInt vitem;
		
		// job side delimiter
		private String delim;
		private int pid;
	
		public void configure (JobConf job)
		{
			try {
				pages = job.getLong("pages", 0);
				delim = job.get("delimiter");
				pid = job.getInt("mapred.task.partition", 0);
	
				visit = new Visit(DistributedCache.getLocalCacheFiles(job),
						delim, pages);
				visit.fireRandom(pid + 1);
				
				vitem = new JoinBytesInt();
				
				errors = 0;
				missed = 0;
	
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	

		public void close ()
		{
			log.info("pid: " + pid + ", " + errors + " erros, " + missed + " missed");
		}

		/**
		 * Reduce: to sum up the record sizes (of slots) one by one so that to determine the
		 * corresponding start point to hold the records for each slot.
		 */
		@Override
		public void reduce(LongWritable key, Iterator<JoinBytesInt> values,
				OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			
			vitem.clear();
//			StringBuffer sb = new StringBuffer("Reduce: " + v.toString());
			while (values.hasNext()) {
				vitem.add(values.next());
//				sb.append("-> " + v.toString());
			}
//			log.info(sb);
			
			if (0!=vitem.ulen) {
				if (vitem.refs > 0) {
					for (int i=0; i<vitem.refs; i++) {
						Text value = new Text(visit.nextAccess(new String(vitem.url)));
						output.collect(key, value);
						reporter.incrCounter(HiBench.Counters.BYTES_DATA_GENERATED, 8+value.getLength());
					}
				} else {
					missed++;
				}
			} else {
				errors++;					
			}
		}
	}

	private void createUserVisitsTableDirectly() throws IOException, URISyntaxException {

		log.info("Creating user visits...");

		Path rankings = new Path(options.getResultPath(), RANKINGS);
		Path fout = new Path(options.getResultPath(), USERVISITS);

		JobConf job = new JobConf(HiveData.class);
		String jobname = "Create uservisits";
		job.setJobName(jobname);
		setVisitsOptions(job);
		
		/***
		 * Set distributed cache file for table generation,
		 * cache files include:
		 * 1. user agents
		 * 2. country code and language code
		 * 3. search keys
		 */

		Path uagentPath = new Path(options.getWorkPath(), uagentf);
		DistributedCache.addCacheFile(uagentPath.toUri(), job);
		
		Path countryPath = new Path(options.getWorkPath(), countryf);
		DistributedCache.addCacheFile(countryPath.toUri(), job);

		Path searchkeyPath = new Path(options.getWorkPath(), searchkeyf);
		DistributedCache.addCacheFile(searchkeyPath.toUri(), job);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(JoinBytesInt.class);

		MultipleInputs.addInputPath(job, dummy.getPath(),
				NLineInputFormat.class, DummyToAccessNoMapper.class);

		if (options.isSequenceOut()) {
			MultipleInputs.addInputPath(job, rankings,
					SequenceFileInputFormat.class, SequenceRankingsToUrlsMapper.class);
		} else {
			MultipleInputs.addInputPath(job, rankings,
					TextInputFormat.class, TextRankingsToUrlsMapper.class);
		}

		job.setCombinerClass(JoinBytesIntCombiner.class);
		job.setReducerClass(CreateUserVisitsReducer.class);
		
		if (options.getNumReds() > 0) {
			job.setNumReduceTasks(options.getNumReds());
		} else {
			job.setNumReduceTasks(Utils.getMaxNumReds());
		}

//		job.setNumReduceTasks(options.slots/2);

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
		log.info("Rankings file " + rankings + " as input");
		log.info("Ouput file " + fout);
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);
	}

	public void generate() throws Exception {
		
		log.info("Generating hive data files...");
		init();
		
		createRankingsTableDirectly();
		createUserVisitsTableDirectly();
		
		close();
	}

	public void loadFiles() throws IOException {
		RawData.createSearchKeys(new Path(options.getWorkPath(), searchkeyf));
		RawData.createUserAgents(new Path(options.getWorkPath(), uagentf));
		RawData.createCCodes(new Path(options.getWorkPath(), countryf));
	}

	private void init() throws IOException {

		log.info("Initializing hive date generator...");

		Utils.checkHdfsPath(options.getResultPath(), true);
		Utils.checkHdfsPath(options.getWorkPath(), true);

		loadFiles();
		
		Utils.serialLinkZipf(options);
		
		dummy = new Dummy(options.getWorkPath(), options.getNumMaps());
	}

	public void close() throws IOException {

		log.info("Closing hive data generator...");
		Utils.checkHdfsPath(options.getWorkPath());
	}
}
