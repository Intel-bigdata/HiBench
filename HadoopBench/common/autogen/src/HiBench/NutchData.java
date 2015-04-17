package HiBench;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.SequenceFile;
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
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlink;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseStatus;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.util.StringUtil;

public class NutchData {

	private static final Log log = LogFactory.getLog(NutchData.class.getName());

	public static final int CACHE_REF_ITEM_LENGTH = 10;
	public static final int CACHE_REF_SET_SIZE = 200 * 12500;
	public static final int CACHE_REF_THRESHOLD = 4;

	private static final String LINKDB_DIR_NAME = "linkdb";
	private static final String CRAWLDB_DIR_NAME = "crawldb";
	private static final String SEGMENTS_DIR_NAME = "segments";
	
	private DataOptions options;
	private Dummy dummy;

	private static final String NUTCH_WORK_DIR_PARAM_NAME = "nutch.working.dir";
	private static final String URLS_DIR_NAME = "urls";
	private Path segment = null;

	NutchData(DataOptions options) {
		this.options = options;
		parseArgs(options.getRemainArgs());
	}
	
	private void parseArgs(String[] args) {}
	
	private static class CreateUrlHash extends MapReduceBase implements
	Mapper<LongWritable, Text, LongWritable, Text> {

		private static final Log log = LogFactory.getLog(CreateUrlHash.class.getName());
		
		private Path workdir;
		private HtmlCore generator;
		private JobConf job;

		public void configure(JobConf job) {

			try {
				workdir = new Path(job.get(NUTCH_WORK_DIR_PARAM_NAME));
				generator = new HtmlCore(job);
				this.job = job;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<LongWritable, Text> output, Reporter reporter)
				throws IOException {
			
			int slotId = Integer.parseInt(value.toString().trim());
			generator.fireRandom(slotId);
			
			try {
				DecimalFormat df = new DecimalFormat("00000");
				String name = "part-" + df.format(slotId);
				FileSystem fs = workdir.getFileSystem(job);

				Path crawldb =
						new Path(new Path (new Path(workdir, CRAWLDB_DIR_NAME), "current"), name);
				SequenceFile.Writer crawldbOut =
						new SequenceFile.Writer(fs, job, crawldb, Text.class, CrawlDatum.class);
				
				Path crawl =
						new Path(new Path(workdir, CrawlDatum.PARSE_DIR_NAME), name);
				SequenceFile.Writer crawlOut =
						new SequenceFile.Writer(fs, job, crawl, Text.class, CrawlDatum.class);
				
				Path generate =
						new Path(new Path(workdir, CrawlDatum.FETCH_DIR_NAME), name);
				SequenceFile.Writer generateOut =
						new SequenceFile.Writer(fs, job, generate, Text.class, CrawlDatum.class);
				
				CrawlDatum datum = new CrawlDatum();
				
				long i = slotId - 1;
				while (i < generator.totalpages) {
					key.set(i);
					Text textUrl = generator.nextUrlText();
					
					if (i < generator.pages) {
						datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
						crawlOut.append(textUrl, datum);
						
						datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
						generateOut.append(textUrl, datum);

						datum.setStatus(CrawlDatum.STATUS_DB_FETCHED);
						crawldbOut.append(textUrl, datum);
					} else {
						datum.setStatus(CrawlDatum.STATUS_LINKED);
						crawldbOut.append(textUrl, datum);
					}
					output.collect(key, textUrl);

					if (0==((i / generator.slots) % 10000)) {
						log.info("still running: " + i + " of <" + generator.pages + ", " + generator.totalpages +">");
					}
					i = i + generator.slots;
				}

				crawlOut.close();
				crawldbOut.close();
				generateOut.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static class CombineReferences extends MapReduceBase implements
	Reducer<Text, References, Text, References> {

		@Override
		public void reduce(Text key, Iterator<References> values,
				OutputCollector<Text, References> output, Reporter reporter)
				throws IOException {
			
			References sum = null;
			while (values.hasNext()) {
				References links = values.next();
				if (links.len > 0) {
					if (null == sum) {
						sum = links;
					} else {
						long[] ids = new long[links.len + sum.len];
						int j = 0;
						for (int i=0; i<sum.len; i++) ids[j++] = sum.refs[i];
						for (int i=0; i<links.len; i++) ids[j++] = links.refs[i];
						sum = new References(ids.length, ids);
					}
				} else {
					output.collect(key, links);
				}
			}
		}
	}
	
	private static class CreateLinks extends MapReduceBase implements
	Reducer<Text, References, Text, NutchParse> {

//		private static final Log log = LogFactory.getLog(CreateLinks.class.getName());
		
		private HtmlCore generator;
		private IndexedMapFile indexedUrls;
		private String segName;
		private long[] cost;

		public void configure(JobConf job) {

			try {
				generator = new HtmlCore(job);
				
				indexedUrls = Utils.getSharedMapFile(URLS_DIR_NAME, job);

				int pid = job.getInt("mapred.task.partition", 0);
				generator.fireRandom(pid * 1000 + 1);
				segName = job.get(Nutch.SEGMENT_NAME_KEY);
				
				cost = new long[6];
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void reduce(Text key, Iterator<References> values,
				OutputCollector<Text, NutchParse> output, Reporter reporter)
				throws IOException {

//			log.info("0> one new value ...");
			Date d = new Date();
			long[] start = new long[7];
			
			start[0] = d.getTime();
			References olinks = null;
			References ilinks = null;
			while (values.hasNext()) {
				References links = values.next();
				if (links.len > 0) {
					if (null == ilinks) {
						ilinks = links;
					} else {
						long[] ids = new long[links.len + ilinks.len];
						int j = 0;
						for (int i=0; i<ilinks.len; i++) ids[j++] = ilinks.refs[i];
						for (int i=0; i<links.len; i++) ids[j++] = links.refs[i];
						ilinks = new References(ids.length, ids);
					}
				} else {
					olinks = links;
				}
			}
//			log.info("1> olinks & ilinks ready ...");
			d = new Date();
			start[1] = d.getTime();
			
			String[] txtContent = generator.genPageWordsAndTitls();
			ParseText text = new ParseText(txtContent[0]);
//			log.info("2> text & titles ready ...");
			d = new Date();
			start[2] = d.getTime();
			
			Outlink[] outlinks = new Outlink[-olinks.len];
			for (int i=0; i<-olinks.len; i++) {
				outlinks[i] = new Outlink(indexedUrls.get(olinks.refs[i]).toString());
			}
			d = new Date();
			start[3] = d.getTime();
			
			Metadata contentMeta = new Metadata();
			contentMeta.add(Nutch.SEGMENT_NAME_KEY, segName);
		    contentMeta.add(Nutch.SIGNATURE_KEY,
		    		StringUtil.toHexString(MD5Hash.digest(txtContent[0].getBytes()).getDigest()));

			ParseData data = new ParseData(new ParseStatus(ParseStatus.SUCCESS), txtContent[1], outlinks, contentMeta, new Metadata());
//			log.info("3> outlinks ready ...");
			d = new Date();
			start[4] = d.getTime();
			
			Inlinks inlinks = new Inlinks();
			if (null != ilinks) {
				for (int i=0; i<ilinks.len; i++) {
					inlinks.add(new Inlink(indexedUrls.get(ilinks.refs[i]).toString()));
				}
			}			
//			log.info("4> inlinks ready ...");
			d = new Date();
			start[5] = d.getTime();
			
			NutchParse parse = new NutchParse(inlinks, text, data);
			output.collect(key, parse);

//			log.info("5> output finished ...");
			d = new Date();
			start[6] = d.getTime();
			for (int i=0; i<cost.length; i++) {
				cost[i] = cost[i] + start[i+1] - start[i];
			}
		}
		
		public void close() {
//			log.info("5>>>>>>>> all finished ...");
			log.info("<<Time>> o/ilinks: " + cost[0] + ", text: " + cost[1]
					+ ", outlinks: " + cost[2] + ", md5: " + cost[3]
					+ ", inlinks: " + cost[4] + ", write: " + cost[5]);
		}
	}
	
	private static class CreateNutchPages extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, References> {

		private static final Log log = LogFactory.getLog(CreateNutchPages.class.getName());
		
		private HtmlCore generator;
		IndexedMapFile indexedUrls;

		public void configure(JobConf job) {
			try {
				generator = new HtmlCore(job);
				indexedUrls = Utils.getSharedMapFile(URLS_DIR_NAME, job);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, References> output, Reporter reporter)
				throws IOException {
			
			int slotId = Integer.parseInt(value.toString().trim());
			generator.fireRandom(slotId);
			
			HashMap<Long, References> hash = new HashMap<Long, References>();
			try {
				long i = slotId - 1;
				while (i < generator.pages) {
					
					References olinks = generator.genPageLinks();
					
					for (int j=0; j<-olinks.len; j++) {
						long to = olinks.refs[j];
						References froms = hash.get(to);
						if (null != froms) {
							if (froms.len == froms.refs.length) {
								output.collect(indexedUrls.get(to), froms);
								froms.len = 0;
							}
						} else {
							if (hash.size() > CACHE_REF_SET_SIZE) {
								for (Entry<Long, References> entry : hash.entrySet()) {
									output.collect(indexedUrls.get(entry.getKey()), entry.getValue());
								}
								hash.clear();
							}
							froms = new References(0, new long[CACHE_REF_ITEM_LENGTH]);
							hash.put(to, froms);
						}
						froms.refs[froms.len++] = i;
					}
					output.collect(indexedUrls.get(i), olinks);
					
					if (0==((i / generator.slots) % 10000)) {
						log.info("still running: " + i + " of " + generator.pages);
					}

					i = i + generator.slots;
				}
				
				for (Entry<Long, References> entry : hash.entrySet()) {
					output.collect(indexedUrls.get(entry.getKey()), entry.getValue());
				}
				hash.clear();
				indexedUrls.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private void setNutchOptions(JobConf job) throws URISyntaxException {

		job.setLong("pages", options.getNumPages() );
		job.setLong("slotpages", options.getNumSlotPages());

		Utils.shareLinkZipfCore(options, job);
		Utils.shareWordZipfCore(options, job);
		
		job.set(NUTCH_WORK_DIR_PARAM_NAME , options.getResultPath().toString());
		
//		job.set(DataOptions.URL_MAP_NAME, paths.getPath(DataPaths.URLS).toString());
	}
	
	public String generateSegmentName() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		return sdf.format(new Date(System.currentTimeMillis()));
	}

	private void createNutchUrls() throws IOException, URISyntaxException {

		log.info("Creating nutch urls ...");
		
		JobConf job = new JobConf(NutchData.class);
		Path urls = new Path(options.getWorkPath(), URLS_DIR_NAME);
		Utils.checkHdfsPath(urls);
		
		String jobname = "Create nutch urls";
		job.setJobName(jobname);

		setNutchOptions(job);
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);

		job.setMapperClass(CreateUrlHash.class);
		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputFormat(MapFileOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		MapFileOutputFormat.setOutputPath(job, urls);
		
//		SequenceFileOutputFormat.setOutputPath(job, fout);
/*		
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
*/		

		log.info("Running Job: " +jobname);
		log.info("Pages file " + dummy.getPath() + " as input");
		log.info("Rankings file " + urls + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);

		log.info("Cleaning temp files...");
		Utils.cleanTempFiles(urls);
	}
	
	private void createNutchIndexData() throws IOException, URISyntaxException {

		log.info("creating nutch index files ... ");

		JobConf job = new JobConf(NutchData.class);
		
		Utils.shareUrls(URLS_DIR_NAME, options, job);
		Utils.shareDict(options, job);
		
		setNutchOptions(job);
		
		Path fsegments = new Path(options.getResultPath(), SEGMENTS_DIR_NAME);
		Utils.checkHdfsPath(fsegments, true);
		
		segment = new Path(fsegments, generateSegmentName());
		Utils.checkHdfsPath(segment, true);
		
		String jobname = "Create nutch index data";
		job.setJobName(jobname);
		
		job.set(Nutch.SEGMENT_NAME_KEY, segment.getName());
		
		FileInputFormat.setInputPaths(job, dummy.getPath());
		job.setInputFormat(NLineInputFormat.class);

		job.setMapperClass(CreateNutchPages.class);
		job.setCombinerClass(CombineReferences.class);
		job.setReducerClass(CreateLinks.class);
		
		if (options.getNumReds() > 0) {
			job.setNumReduceTasks(options.getNumReds());
		} else {
			job.setNumReduceTasks(Utils.getMaxNumMaps());
		}
		
		FileOutputFormat.setOutputPath(job, segment);
		job.setOutputFormat(NutchOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(References.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NutchParse.class);

		log.info("Running Job: " + jobname);
		log.info("Pages file " + dummy.getPath() + " as input");
		log.info("Rankings file " + segment + " as output");
		JobClient.runJob(job);
		log.info("Finished Running Job: " + jobname);

		log.info("Cleaning temp files...");
		Utils.cleanTempFiles(segment);
	}
	
	private void init() throws IOException {

		log.info("Initializing Nutch data generator...");

		Utils.checkHdfsPath(options.getResultPath(), true);
		Utils.checkHdfsPath(options.getWorkPath(), true);
		
		int words = RawData.putDictToHdfs(new Path(options.getWorkPath(), HtmlCore.getDictName()), options.getNumWords());
		options.setNumWords(words);
		
		Utils.serialLinkZipf(options);
		Utils.serialWordZipf(options);

		dummy = new Dummy(options.getWorkPath(), options.getNumMaps());
	}
/*	
	private void test2LevelMapFile(Path furl) throws IOException {

		JobConf job = new JobConf();
		FileSystem fs = FileSystem.get(job);
		MapFile.Reader reader = new MapFile.Reader(fs, furl.toString(), job);
		Text value = new Text();
		reader.get(new LongWritable(1000), value);
		if (null != value) {
			log.info("---Find it: <1000, " + value + ">");
		}
	}
*/	
	public void generate() throws Exception {
		
		init();
		createNutchUrls();
		createNutchIndexData();
		
		Path ffetch = new Path(options.getResultPath(), CrawlDatum.FETCH_DIR_NAME);
		Path fparse = new Path(options.getResultPath(), CrawlDatum.PARSE_DIR_NAME);
		Path linkdb = new Path(segment, LINKDB_DIR_NAME);
		
		FileSystem fs = ffetch.getFileSystem(new Configuration());
		fs.rename(ffetch, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
		fs.rename(fparse, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
		fs.rename(linkdb, new Path(options.getResultPath(), LINKDB_DIR_NAME));
		fs.close();
		
		close();
	}
	
	private void close() throws IOException {
		log.info("Closing nutch data generator...");
		Utils.checkHdfsPath(options.getWorkPath());
	}
	
	public static final String getLinkDbName() {
		return LINKDB_DIR_NAME;
	}
}
