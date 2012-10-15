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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.apache.hadoop.fs.Path;

/***
 * Util used to generate random user visit records
 * @author lyi2
 *
 */
public class VisitGenerator {
	public Random rand;
	public String delim = ",";
	public String[] uagents, ccodes, skeys;
	public long urls, dateRange;
	private Date date;
	private SimpleDateFormat dateForm;
	
	@SuppressWarnings("deprecation")
	VisitGenerator(Path[] cacheFiles, String dl, long numUrls) throws IOException {
		rand = new Random();
		date = new Date();
		dateRange = Date.parse("Tue 1 May 2012 00:00:00");  // random date range
		dateForm = new SimpleDateFormat("yyyy-MM-dd");
		
		if (null != dl) {
			delim = dl;
		}
		urls = numUrls;

		String line, all;

		/***
		 * read in those distributed cache files as random range list
		 */
		if ((null != cacheFiles) && (cacheFiles.length > 0)) {
			for (Path cachePath : cacheFiles) {
				
				if (cachePath.getName().contains(DataPaths.uagentf)) {
					// examples of user agents
					all = "";
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));
					while ((line = br.readLine()) != null) {
						all = all.concat(line+"\n");
					}
					br.close();

					uagents = all.split("\n");
				} else if (cachePath.getName().contains(DataPaths.countryf)) {
					// examples of country codes
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));

					all = "";
					while ((line = br.readLine()) != null) {
						all = all.concat(line+"\n");
					}
					br.close();

					ccodes = all.split("\n");
				} else if (cachePath.getName().contains(DataPaths.searchkeyf)) {
					// examples of search keys
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));

					all = "";
					while ((line = br.readLine()) != null) {
						all = all.concat(line+"\n");
					}
					br.close();

					skeys = all.split("\n");
				}
			}
		}
	}
	
	private String nextCountryCode() {
		return ccodes[rand.nextInt(ccodes.length)];
	}
	
	private String nextUserAgent() {
		return uagents[rand.nextInt(uagents.length)];
	}
	
	private String nextSearchKey () {
		return skeys[rand.nextInt(skeys.length)];
	}

	private String nextTimeDuration() {
		return Integer.toString(rand.nextInt(10)+1);
	}

	private String nextIp() {
		return Integer.toString(rand.nextInt(254)+1)
				+ "." +  Integer.toString(rand.nextInt(255))
				+ "." +  Integer.toString(rand.nextInt(255))
				+  "." +  Integer.toString(rand.nextInt(254)+1);
	}
	
	private String nextDate() {
		date.setTime((long) Math.floor(rand.nextDouble() * dateRange));
		return dateForm.format(date);
	}
	
	private String nextProfit() {
		return Float.toString(rand.nextFloat());
	}

	public String nextUrlId() {
		return Long.toString((long) Math.floor(rand.nextDouble()*urls));
	}

	/***
	 * set the randseed of random generator
	 * @param randSeed
	 */
	public void setRandSeed(int randSeed) {
		rand.setSeed(randSeed);
	}

	public String nextAccess(String url) {
		return(nextIp() + delim +
			url + delim +
			nextDate() + delim +
			nextProfit() + delim +
			nextUserAgent().trim() + delim +
			nextCountryCode().trim().replace(",", delim) + delim +
			nextSearchKey().trim() + delim +
			nextTimeDuration());
	}
}
