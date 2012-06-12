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

import org.apache.hadoop.mapred.JobConf;

public class HtmlConf {
	
	public int agents;
	public long pages;
	public long agentpages;

	public ZipfRandom linkZipf;
	public ZipfRandom wordZipf;

	HtmlConf (DataOptions options) {
		
		agents = options.maps;
		pages = options.pages;
		linkZipf = null;
		wordZipf = null;

		if (options.pages > 0) {
			linkZipf = new ZipfRandom (agents, options.reds, options.pages, DataOptions.LINK_ZIPF_EXPONENT);
			linkZipf.calcScale(pages * (long) HtmlGenerator.getMeanLinksPerPage(), DataOptions.lratio);
		}
		if (options.words > 0) {
			wordZipf = new ZipfRandom (agents, options.reds, options.words, DataOptions.WORD_ZIPF_EXPONENT);
			wordZipf.calcScale(pages * (long) HtmlGenerator.getMeanWordsPerPage(), DataOptions.wratio);
		}

		agentpages = (long) Math.ceil(pages * 1.0 / agents);
	}
/*
	HtmlConf (int numAgents, long numPages, long numLinks, long numWords) {
		
		agents = numAgents;
		pages = numPages;
		linkZipf = null;
		wordZipf = null;

		if (numLinks > 0) {
			linkZipf = new ZipfRandom (agents, numLinks, DataOptions.LINK_ZIPF_EXPONENT);
		}
		if (numWords > 0) {
			wordZipf = new ZipfRandom (agents, numWords, DataOptions.WORD_ZIPF_EXPONENT);
		}

		agentpages = (long) Math.ceil(pages * 1.0 / agents);
	}
*/
	public void setJobConf(JobConf job) {
		
		job.setInt("agents", agents);
		job.setLong("pages", pages);
		job.setLong("agentpages", agentpages);
		
		long vLinkElems = (null==linkZipf)? -1 : linkZipf.velems;
		long vWordElems = (null==wordZipf)? -1 : wordZipf.velems;
		
		job.setLong("vLinkElems", vLinkElems);
		job.setLong("vWordElems", vWordElems);
	}

	public String debugInfo() {
		
		String msg = "[agents: " + agents
				+ "] [pages: " + pages
				+ "] [agentpages: " + agentpages + "]";
		if (null != linkZipf) {
			msg = msg + "\n -->linkZipf " + linkZipf.debugInfo();
		}
		if (null != wordZipf) {
			msg = msg + "\n -->wordZipf " + wordZipf.debugInfo();
		}
		return msg;
	}
}
