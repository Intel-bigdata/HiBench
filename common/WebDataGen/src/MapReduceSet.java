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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;
import org.apache.hadoop.mapred.Partitioner;

@SuppressWarnings("deprecation")
public class MapReduceSet {
	
	public static class SumUpZipfMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text>
	{

		private int agents;
		private long elems;
		private double exponent;
		private double scale;

		public void configure (JobConf job)
		{
			agents = job.getInt("agents", 0);
			elems = job.getLong("elems", 0);
			exponent = (double) job.getFloat("exponent", 0);
			scale = (double) job.getFloat("scale", 0);
			
		}

		/**
		 * Map: to calculate the record size for one agent
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException
		{
			int agentId = Integer.parseInt(value.toString().trim());
			long needSpace = 0;
			
			Text k = new Text(Integer.toString(agentId));
			Text v = new Text("");
			for (long i=agentId; i<=elems;)
			{
				long tmpspace = Math.round(scale / Math.pow(i, exponent));
				if (tmpspace > 0) {
					needSpace = needSpace + tmpspace; 
				} else {
					break;
				}
				i = i + agents;
			}

			v.set(Long.toString(needSpace));
			output.collect(k, v);
		}
	}

	public static class SumUpZipfReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

		public long startSpaceId = 0;  // To Remember the Start Point of Space

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			Text k = new Text("");
			if (values.hasNext()) {
				Text v = values.next();

				long endSpaceId = startSpaceId + Long.parseLong(v.toString());
				k.set(key.toString() +
						"-" + Long.toString(startSpaceId) +
						"-" + Long.toString(endSpaceId));
				output.collect(k, v);
				startSpaceId = endSpaceId;
			} else {
				/* content is in problem */
			}
			return;
		}
	}

	public static class ConcatTextCombiner extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

			String head = null, list = "";
			while (values.hasNext()) {
				String value = values.next().toString();
				if (value.startsWith(DataOptions.JOIN_TAG)) {
					head = value;
				} else {
					list = list + " " + value;
				}
			}

			if (head == null) {
				output.collect(key, new Text(list.trim()));
			} else {
				output.collect(key, new Text(head + list));
			}
		}
	}

	public static class CountRankingAndReplaceIdReducer  extends MapReduceBase implements
	Reducer<Text, Text, NullWritable, Text> {
		
		public Random rand;
		
		public void configure (JobConf job)
		{
			String pid = job.get("mapred.task.partition", null);
			if (null != pid) {
				rand = new Random(Integer.parseInt(pid) + 1);
			} else {
				rand = null;
//				System.exit(-1);
			}
		}

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {

			int count = 0;
			String value = null;
			String url = null;
			
			while (values.hasNext()) {
				value = values.next().toString(); 
				if (value.startsWith(DataOptions.JOIN_TAG)) {
					int endUrl = value.indexOf(" ");
					if (endUrl < 0) {
						// just a key'
						url = value.substring(1);
					} else {
						// key' together with space separated reference docids
						url = value.substring(1, endUrl);
						count = count + value.split(" ").length -1;
					}
				} else {
					count = count + value.split(" ").length;
				}
			}
			output.collect(NullWritable.get(),
					new Text(url + DataOptions.DELIMITER + count
							+ DataOptions.DELIMITER + (rand.nextInt(99) + 1)));
		}
	}

	/***
	 * Reducer to count the rankings (reference number) of urls, leave the
	 * urls still as ids
	 * @author lyi2
	 *
	 */
	public static class CountRankingReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			int count = 0;
			while (values.hasNext()) {
				count = count + values.next().toString().split(" ").length;
			}
			output.collect(key, new Text(Integer.toString(count)));
		}
	}

	/***
	 * Mapper to randomly create HTML pages, output results into three
	 * files respectively, i.e., LINK, PAGE_ZLINK_TABLE, PAGE_ZWORD_TABLE
	 * @author lyi2
	 *
	 */
	public static class CreateHtmlPagesMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {

		HtmlGenerator htmlpage;
		public long pages, agentpages, vLinkElems, vWordElems;
	
		public void configure(JobConf job) {
			vLinkElems = job.getLong("vLinkElems", 0);
			vWordElems = job.getLong("vWordElems", 0);
			pages = job.getLong("pages", 0);
			agentpages = job.getLong("agentpages", 0);
	
			htmlpage = new HtmlGenerator(pages, agentpages, vLinkElems, vWordElems);
		}
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {

			int agentId = Integer.parseInt(value.toString().trim());
			htmlpage.fireRandom(agentId);

			long[] range = htmlpage.getPageRange(agentId);

			/**
			 * For output collect
			 */
			Text k = new Text("");
			for (long i=range[0]; i<range[1]; i++) {
				String[] webPage = htmlpage.createOnePage(i);
	
				k.set("u" + i);
				output.collect(k, new Text(webPage[0]));	// url
				if (!DataOptions.SKIP_LINKS) {
					k.set("l" + i);
					output.collect(k, new Text(webPage[1]));	// page_zlink
				}
				if (!DataOptions.SKIP_WORDS) {
					k.set("t" + i);
					output.collect(k, new Text(webPage[2]));	// page_zword
				}
			}
		}
	}

	/***
	 * Mapper to randomly create user visits. In map step, only the target
	 * urls of user visits are created, the rest content of visits will be
	 * created in reduce step
	 * @author lyi2
	 *
	 */
	public static class CreateRandomAccessMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		private long pages;
		private long agents;
		private long visits;
		private VisitGenerator access;
	
		public void configure (JobConf job)
		{
			try {
				pages = job.getLong("pages", 0);
				agents = job.getLong("agents", 0);
				visits = job.getLong("visits", 0);
	
				access = new VisitGenerator(DistributedCache.getLocalCacheFiles(job),
						",", pages);
	
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
	
			int agentId = Integer.parseInt(value.toString().trim());
			access.setRandSeed(agentId);
	
			Text k = new Text("");
			Text v = new Text("1");
			for (long i=agentId; i<=visits;) {
				// simply setting url id is fine in map step
				k.set(access.nextUrlId());
				output.collect(k, v);
				i = i + agents;
			}
	
	/*
			Text k = new Text("");
			Text v =  new Text("");
			for (long i=agentId; i<=visits;)
			{
				k.set(access.nextUrlId());
				v.set(access.nextAccess());
				output.collect(k, v);
				i = i + agents;
			}
	*/
		}
	}

	public static class CreateUserVisitsCombiner extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
	
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			int count = 0;
			String uhead = null;
			while (values.hasNext()) {
				String value = values.next().toString();
				if (!value.startsWith(DataOptions.JOIN_TAG)) {
					count = count + Integer.parseInt(value.trim());
				} else {
					int loc = value.indexOf(" ");
					if (loc < 0) {
						uhead = value;
					} else {
						uhead = value.substring(0, loc);
						count = count + Integer.parseInt(value.substring(loc + 1));
					}
				}
/*				
				if ("".equals(value)) {
					// it is only a "" to represent a visit
					count++;
				} else {
					// it is a key'
					v = value;
				}
*/
			}
			
			if (null==uhead) {
				output.collect(key, new Text(Integer.toString(count)));
			} else {
				if (count > 0) {
					uhead = uhead + " " + Integer.toString(count);
				}
				output.collect(key, new Text(uhead));
			}
/*			
			if (count != 0) {
				if ("".equals(v)) {
					// key' not exist
					v = Integer.toString(count);
				} else {
					// key' exist
					v = v + " " + count;
				}
			}
			output.collect(key, new Text(v));
*/
		}
	}

	public static class CreateUserVisitsReducer extends MapReduceBase implements
	Reducer<Text, Text, NullWritable, Text> {
	
		private long pages;
		private VisitGenerator access;
	
		public void configure (JobConf job)
		{
			try {
				pages = job.getLong("pages", 0);
	
				access = new VisitGenerator(DistributedCache.getLocalCacheFiles(job),
						DataOptions.DELIMITER, pages);
				
				String pid = job.get("mapred.task.partition", null);
				if (null != pid) {
					access.setRandSeed(Integer.parseInt(pid) + 1);
				} else {
					access.setRandSeed(0);
//					System.exit(-1);
				}
	
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	
		/**
		 * Reduce: to sum up the record sizes (of agents) one by one so that to determine the
		 * corresponding start point to hold the records for each agent.
		 */
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
	
			String url = "", value;
			int count = 0;
			while (values.hasNext()) {
				value = values.next().toString();
				if (value.startsWith(DataOptions.JOIN_TAG)) {
					int endUrl = value.indexOf(" ");
					if (endUrl < 0) {
						// just a key'
						url = value.substring(1);
					} else {
						// key' together with a count
						url = value.substring(1, endUrl);
						count = count + Integer.parseInt(value.substring(endUrl+1).trim());
					}
				} else {
					count = count + Integer.parseInt(value);
				}
			}
	
			if (count>0) {
				for (int i=0; i<count; i++) {
					output.collect(NullWritable.get(),
							new Text(access.nextAccess(url)));
				}
			}
		}
	}

	/***
	 * To create the distributed file to hold all the Zipfian samples for all agents
	 * @author lyi2
	 *
	 */
	public static class CreateZipfDistrMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text>
	{
		private int agents;
		private long elems;
		private double exponent;
		private double scale;
	
		public void configure (JobConf job)
		{
			agents = job.getInt("agents", 0);
			elems = job.getLong("elems", 0);
			exponent = (double) job.getFloat("exponent", 0);
			scale = (double) job.getFloat("scale", 0);
		}
		
		/***
		 * Map: for each Web page i (also be its frequency rank), occupy a certain size of
		 * Zipfian spaces which is proportional to its Zipfian function value (i.e., 1/i^e,
		 * where e is the value of exponent). For each occupied Zipfian space, set its value
		 * to be i 
		 */
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException
		{
			String delimiter = "[-\t]";
			String[] args = value.toString().split(delimiter);
			
			int siteId = Integer.parseInt(args[0]);
			long startSpaceId = Long.parseLong(args[1]);
			long endSpaceId = Long.parseLong(args[2]);
			long needSpace = Long.parseLong(args[3]);
			
			long spaceId1=startSpaceId, spaceId2=startSpaceId;
			Text k = new Text("");
			Text v = new Text("");
			
			for (long i=siteId; i<=elems;)
			{
				spaceId2 = spaceId1 + Math.round(scale / Math.pow(i, exponent));
				if (spaceId1==spaceId2) {
					break;	// No chance for the tail elements to be referenced
				}
				
				v.set(Long.toString(i-1));
	
				for (;spaceId1<spaceId2; spaceId1++) {
					k.set(Long.toString(spaceId1));
					output.collect(k, v);
				}
				i = i + agents;
			}
	
			if ((spaceId1!=endSpaceId) || ((endSpaceId-startSpaceId)!=needSpace)) {
				/*
				 * LOG ERROR
				 */
			}
			return;
		}
	}

	/***
	 * OutputFormat to ensure that different page contents (i.e., urls,
	 * page_zlink, page_zword) will be output to different HDFS folders
	 * @author lyi2
	 *
	 */
	public static class HtmlMultipleTextOutputFormat extends MultipleTextOutputFormat<Text, Text> {
	
		/**
		 * generated the file name by recognize the first flag char in output
		 */
		protected String generateFileNameForKeyValue(Text key, Text value, String name) {
			
			String k = key.toString();
			if (k.startsWith("u")) {
				return DataPaths.LINKS + "/" + name;
			} else if (k.startsWith("l")) {
				return DataPaths.T_PAGE_ZLINK + "/" + name;
			} else if (k.startsWith("t")) {
				return DataPaths.T_PAGE_ZWORD + "/" + name;
			}
			return name;
		}

		/**
		 * generate the actual key by skipping the first flag char
		 */
		protected Text generateActualKey(Text key, Text value) {
			Text k = new Text(key.toString().substring(1));
			return k;
		}
	}

	/***
	 * JOIN content with zipf distribution to replace the vids (zipfian ids)
	 * by corresponding actual link/word ids
	 * @author lyi2
	 *
	 */
	public static class JoinContentWithZipfReducer extends MapReduceBase implements
	Reducer<Text, Text, Text, Text> {
		/**
		 * Reduce: to sum up the record sizes (of agents) one by one so that to determine the
		 * corresponding start point to hold the records for each agent.
		 */
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			String linkId = "", docIds = "";
			while (values.hasNext()) {
				String value = values.next().toString();
				if (value.startsWith(DataOptions.JOIN_TAG)) {
					int lend = value.indexOf(" ");
					if (lend < 0) {
						linkId = value.substring(1);
					} else {
						linkId = value.substring(1, lend);
						docIds = docIds + value.substring(lend);
					}
				} else {
					docIds = docIds + " " + value;
				}
			}
			if (!"".equals(docIds)) {
				output.collect(new Text(linkId), new Text(docIds.trim()));
			}
			
	/*		
			String value = "";
			while (values.hasNext()) {
				value = values.next().toString() + " " + value;
			}
			value.trim();
			
			String head="", tail="";
			
			int linkBegin = value.indexOf("!");
			if (linkBegin !=0) {
				head = value.substring(0, linkBegin);
			}
			
			int linkEnd = value.indexOf(" ", linkBegin);
			if (linkEnd > 0) {
				tail = value.substring(linkEnd);
			} else {
				linkEnd = value.length();
			}
			
			String v = head + tail;
			if (!"".equals(v)) {
				output.collect(new Text(value.substring(linkBegin, linkEnd)), new Text(v));
			}
	*/
	
	/*
			String linkId = "";
			String docIds = "";
			String value = null;
			while (values.hasNext()) {
				value = values.next().toString();
				if (value.startsWith("!")) {
					linkId = value.substring(1);
				} else {
					docIds = value + " " + docIds;
				}
			}
	
			if (!"".equals(docIds)) {
				output.collect(new Text(linkId), new Text(docIds));
			}
	*/
			
	/*
			String linkId = "";
			String docIds = "";
	
			while (values.hasNext())
			{
				String value = values.next().toString();
				if (!value.contains(":")) {
					// value from reversed xxx_zipf_distr
					linkId = value;
				} else {
					// value from reversed page_zipflink_table
					docIds = docIds + value + " ";
				}
			}
			
			if (!"".equals(docIds)) {
				output.collect(new Text(linkId), new Text(docIds));
			}
	*/
			
	/*
			if (!"".equals(docIds)) {  // link reference >= 1
				if (!"".equals(elemId)) {  // elemId <= corner
					output.collect(new Text(elemId), new Text(docIds));
				} else {	// elemId is in tail part
					output.collect(new Text(), new Text(docIds));
				}
			}
	*/
		}
	}

	/***
	 * Mapper to simply output the input
	 * @author lyi2
	 *
	 */
	public static class MyIdentityMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String delimiter = "[ \t]";
			String[] args = value.toString().split(delimiter);
			output.collect(new Text(args[0]), new Text(args[1]));
	/*		
			Text k = new Text("");
			Text v = new Text("");
			String docId = args[0];
			for (int i=1; i<args.length; i++) {
				String[] elems = args[i].split(":");
				k.set(elems[0]);			// set key to be zipfId of link
				v.set(docId+":"+elems[1]);	// set value to be docId:offset
				output.collect(k, v);
			}
	*/
		}
	}

	public static class OutputLinkEdgesMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String delimiter = "[ \t]";
			String[] args = value.toString().split(delimiter);
	
			//String from, to=key.toString();
			String to = args[0];
			String from;
			for (int i=1; i<args.length; i++) {
				if (DataOptions.KEEP_ORDER) {
					from = args[i].split(":")[0];
				} else {
					from = args[i];
				}
				output.collect(new Text(from),new Text(to));
			}
		}
	}

	public static class OutputLinkNodesMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, NullWritable, Text> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<NullWritable, Text> output, Reporter reporter) throws IOException {
	
			String delimiter = "[ \t]";
			output.collect(NullWritable.get(),
					new Text(value.toString().split(delimiter)[0]));
		}
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

	/***
	 * Mapper to reverse the original (X, Y) into (Y, X)
	 * @author lyi2
	 *
	 */
	public static class ReverseContentMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String delimiter = "[ \t]";
			String[] args = value.toString().split(delimiter);
			
			if (!DataOptions.KEEP_ORDER) {
				Text no_v = new Text(args[0]);
				for (int i=1; i<args.length; i++) {
					output.collect(new Text(args[i]), no_v);
				}
			} else {
				Text o_k = new Text("");
				Text o_v = new Text("");
				String docId = args[0];
				for (int i=1; i<args.length; i++) {
					String[] elems = args[i].split(":");
					o_k.set(elems[0]);			// set key to be zipfId of link
					o_v.set(docId+":"+elems[1]);	// set value to be docId:offset
					output.collect(o_k, o_v);
				}
			}
		}
	}

	/***
	 * Reducer to sort the elements so that to maintain the relative 
	 * order of links/words within a HTML page 
	 * @author lyi2
	 *
	 */
	public static class SortElementsReducer  extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		/***
		 * Define comparator for sorting
		 * @author lyi2
		 *
		 */
		@SuppressWarnings("rawtypes")
		class MyItemComparator implements Comparator {
			@Override
			public final int compare(Object item1, Object item2) {
				int key1 = Integer.parseInt(((String) item1).split(":")[1]);
				int key2 = Integer.parseInt(((String) item2).split(":")[1]);
	
				if (key1>key2) {
					return 1;
				}
				if (key1<key2) {
					return -1;
				}
				return 0;
			}
		}
	
		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String[] items = value.toString().split("[ \t]");
			
			Arrays.sort(items, 1, items.length, new MyItemComparator());
			Text k = new Text(items[0]);
			String v = "";
			for (int i=1; i<items.length; i++) {
				v = v.concat(items[i].split(":")[1] + " ");
			}
			output.collect(k, new Text(v));
		}
	}

	/***
	 * Mapper to simply tag the key part of a JOIN
	 * @author lyi2
	 *
	 */
	public static class TagRecordsMapper extends MapReduceBase implements
	Mapper<LongWritable, Text, Text, Text> {
	
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	
			String delimiter = "[\t]";
			String[] args = value.toString().split(delimiter);
			output.collect(new Text(args[0]), new Text(DataOptions.JOIN_TAG + args[1]));
		}
	}
	
	@SuppressWarnings("hiding")
	public static class ModulusPartitioner<LongWritable, NullWritable> implements
	Partitioner<LongWritable, NullWritable> {

		@Override
		public int getPartition(LongWritable key, NullWritable value, int numReduceTasks) {

			Long k = Long.parseLong(key.toString());
			return (int) (k % numReduceTasks);
		}

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}
	}
}
