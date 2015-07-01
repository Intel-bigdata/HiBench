package HiBench;

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
public class Visit {
	private Random rand;
	private String delim = ",";
	private String[] uagents, ccodes, skeys;
	private long urls, dateRange;
	private Date date;
	private SimpleDateFormat dateForm;
	
	@SuppressWarnings("deprecation")
	Visit(Path[] cacheFiles, String delim, long numUrls) throws IOException {
		rand = new Random();
		date = new Date();
		dateRange = Date.parse("Tue 1 May 2012 00:00:00");  // random date range
		dateForm = new SimpleDateFormat("yyyy-MM-dd");
		
		if (null != delim) {
			this.delim = delim;
		}
		urls = numUrls;

		/***
		 * read in those distributed cache files as random range list
		 */
		if ((null != cacheFiles) && (cacheFiles.length > 0)) {
			for (Path cachePath : cacheFiles) {
				
				if (cachePath.getName().contains(HiveData.uagentf)) {
					// examples of user agents
					
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));

					StringBuffer all = new StringBuffer();
					String line = null;
					while ((line = br.readLine()) != null) {
						all.append(line.trim() + "\n");
					}
					br.close();

					uagents = new String(all).split("\n");
				} else if (cachePath.getName().contains(HiveData.countryf)) {
					// examples of country codes
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));

					StringBuffer all = new StringBuffer();
					String line = null;
					while ((line = br.readLine()) != null) {
						all.append(line.trim().replace(",", delim) + "\n");
					}
					br.close();

					ccodes = new String(all).split("\n");
				} else if (cachePath.getName().contains(HiveData.searchkeyf)) {
					// examples of search keys
					BufferedReader br =
							new BufferedReader(new FileReader(cachePath.toString()));

					StringBuffer all = new StringBuffer();
					String line = null;
					while ((line = br.readLine()) != null) {
						all.append(line.trim() + "\n");
					}
					br.close();

					skeys = new String(all).split("\n");
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
				+ "." +  Integer.toString(rand.nextInt(254)+1);
	}
	
	private String nextDate() {
		date.setTime((long) Math.floor(rand.nextDouble() * dateRange));
		return dateForm.format(date);
	}
	
	private String nextProfit() {
		return Float.toString(rand.nextFloat());
	}

	public long nextUrlId() {
		return (long) Math.floor(rand.nextDouble()*urls);
	}

	/***
	 * set the randseed of random generator
	 * @param randSeed
	 */
	public void fireRandom(int randSeed) {
		rand.setSeed(randSeed);
	}

	public String nextAccess(String url) {
		return(nextIp() + delim +
			url + delim +
			nextDate() + delim +
			nextProfit() + delim +
			nextUserAgent() + delim +
			nextCountryCode() + delim +
			nextSearchKey() + delim +
			nextTimeDuration());
	}
	
	public String debug() {
		return
		"[delim: " + delim + "] " +
		"[urls: " + urls + "]";
	}
}
