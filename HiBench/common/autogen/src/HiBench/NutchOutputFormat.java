package HiBench;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;


/* Parse content in a segment. */
public class NutchOutputFormat implements OutputFormat<Text, NutchParse> {
//	private static final Log log = LogFactory.getLog(NutchOutputFormat.class);

	public void checkOutputSpecs(FileSystem fs, JobConf job) throws IOException {
		Path out = FileOutputFormat.getOutputPath(job);
		if ((out == null) && (job.getNumReduceTasks() != 0)) {
			throw new InvalidJobConfException(
					"Output directory not set in JobConf.");
		}
		if (fs == null) {
			fs = out.getFileSystem(job);
		}

		if (fs.exists(new Path(out, CrawlDatum.PARSE_DIR_NAME)))
			throw new IOException("Segment already parsed!");
		
		
	}

	public RecordWriter<Text, NutchParse> getRecordWriter(FileSystem fs, JobConf job,
			String name, Progressable progress) throws IOException {

		final CompressionType compType = SequenceFileOutputFormat.getOutputCompressionType(job);
		Path out = FileOutputFormat.getOutputPath(job);
		
		Path text = new Path(new Path(out, ParseText.DIR_NAME), name);
		Path data = new Path(new Path(out, ParseData.DIR_NAME), name);
		Path linkdb = new Path(new Path(new Path(out, NutchData.getLinkDbName()), "current"), name);
//		Path crawl = new Path(new Path(out, CrawlDatum.PARSE_DIR_NAME), name);
//		Path crawldb = new Path(out.getParent(), "/crawldb/current");
//		Path crawlgenerate = new Path(new Path(out, CrawlDatum.GENERATE_DIR_NAME), name);

//		final String[] parseMDtoCrawlDB = job.get("db.parsemeta.to.crawldb","").split(" *, *");

		final MapFile.Writer textOut =
				new MapFile.Writer(job, fs, text.toString(), Text.class, ParseText.class,
						CompressionType.RECORD, progress);

		final MapFile.Writer dataOut =
				new MapFile.Writer(job, fs, data.toString(), Text.class, ParseData.class,
						compType, progress);

		final MapFile.Writer linkdbOut =
				new MapFile.Writer(job, fs, linkdb.toString(), Text.class, Inlinks.class,
						compType, progress);
/*
		final SequenceFile.Writer crawlOut =
				SequenceFile.createWriter(fs, job, crawl, Text.class, CrawlDatum.class,
						compType, progress);

		final SequenceFile.Writer crawlDbOut =
				SequenceFile.createWriter(fs, job, crawldb, Text.class, CrawlDatum.class,
						compType, progress);

		final SequenceFile.Writer generateOut =
				SequenceFile.createWriter(fs, job, crawlgenerate, Text.class, CrawlDatum.class,
						compType, progress);
*/		
		return new RecordWriter<Text, NutchParse>() {

			public void write(Text key, NutchParse parse)
					throws IOException {

//				CrawlDatum crawldatum = new CrawlDatum();
/*				
				crawldatum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
				crawlOut.append(parse.url, crawldatum);
				
				crawldatum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
				generateOut.append(parse.url, crawldatum);

				crawldatum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
				crawlDbOut.append(parse.url, crawldatum);
*/				
				if (null != parse.text) {
					textOut.append(key, parse.text);
				}
				
				if (null != parse.data) {
					dataOut.append(key, parse.data);
				}
				
				if (null != parse.inlinks) {
					linkdbOut.append(key, parse.inlinks);
				}
				
				// TODO: remember to append real page text

				/**
				 *  TODO: remember to append correct crawldatum
					1.	CrawlDatum d = new CrawlDatum(CrawlDatum.STATUS_SIGNATURE, 0);
						String sig = parseData.getContentMeta().get(Nutch.SIGNATURE_KEY);
					2.	parseMDCrawlDatum = new CrawlDatum(CrawlDatum.STATUS_PARSE_META, 0);
						if (parseMDCrawlDatum != null) crawlOut.append(key, parseMDCrawlDatum);
					3.	newDatum.setStatus(CrawlDatum.STATUS_LINKED);
						newDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY,	new Text(reprUrl));
					4.	CrawlDatum target = new CrawlDatum(CrawlDatum.STATUS_LINKED, interval);
						for (Entry<Text, CrawlDatum> target : targets) {
							crawlOut.append(target.getKey(), target.getValue());
						}
					5.	adjust, STATUS_LINKED
					6.	
					CrawlDatum datum = new CrawlDatum();
					datum.setStatus(CrawlDatum.STATUS_FETCH_SUCCESS);
					String timeString = parse.getData().getContentMeta().get(Nutch.FETCH_TIME_KEY);
					try {
						datum.setFetchTime(Long.parseLong(timeString));
					} catch (Exception e) {
						LOG.warn("Can't read fetch time for: " + key);
						datum.setFetchTime(System.currentTimeMillis());
					}
					crawlOut.append(key, datum);
				 */
			}

			public void close(Reporter reporter) throws IOException {
				textOut.close();
				dataOut.close();
				linkdbOut.close();
//				crawlOut.close();
//				crawlDbOut.close();
//				generateOut.close();
//				linkdbOut.close();
			}
		};
	}
}
