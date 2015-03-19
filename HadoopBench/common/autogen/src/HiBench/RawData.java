package HiBench;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class RawData {

	private static Random rand = new Random(11);

	private static String dict = "/usr/share/dict/words";
	private static int numSourceWords = 1000;
	private static int numSourceUAgents = 2000;

	private static String[] rawUAgents = {
			"0.0800 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)",
			"0.0300 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)",
			"0.1300 	Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
			"0.0900 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)",
			"0.0700 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
			"0.0700 	Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
			"0.0400 	Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)",
			"0.0150 	Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0)",
			"0.0080 	Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
			"0.0600 	Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/xxx",
			"0.0300 	Mozilla/5.0 (Windows; U; Windows NT 5.2)AppleWebKit/525.13 (KHTML like Gecko) Version/3.1Safari/525.13",
			"0.0400 	Mozilla/5.0 (iPhone; U; CPU like Mac OS X)AppleWebKit/420.1 (KHTML like Gecko) Version/3.0 Mobile/4A93Safari/419.3",
			"0.0050 	iPhone 3.0: Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML like Gecko) Version/4.0 Mobile/7A341 Safari/528.16",
			"0.0050 	Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/125.8",
			"0.0030		Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML like Gecko) Safari/85.8",
			"0.0080 	Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12)Gecko/20080219 Firefox/2.0.0.12	Navigator/9.0.0.6",
			"0.0070 	Mozilla/5.0 (Windows; U; Windows NT 5.2)Gecko/2008070208 Firefox/3.0.1",
			"0.0080 	Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070309 Firefox/2.0.0.3",
			"0.0060 	Mozilla/5.0 (Windows; U; Windows NT 5.1)Gecko/20070803 Firefox/1.5.0.12",
			"0.0112 	Mozilla/5.0 (Windows; U; Windows NT 5.2) AppleWebKit/525.13 (KHTML like Gecko) Chrome/0.2.149.27 Safari/525.13",
			"0.0075 	Netscape 4.8 (Windows Vista): Mozilla/4.8 (Windows NT 6.0; U)",
			"0.0033 	Opera 9.2 (Windows Vista): Opera/9.20 (Windows NT 6.0; U; en)",
			"0.0028 	Opera 8.0 (Win 2000): Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; en) Opera 8.0",
			"0.0035 	Opera 7.51 (Win XP): Opera/7.51 (Windows NT 5.1; U)",
			"0.0030 	Opera 7.5 (Win XP): Opera/7.50 (Windows XP; U)",
			"0.0020 	Opera 7.5 (Win ME): Opera/7.50 (Windows ME; U)",
			"0.0012 	Opera/9.27 (Windows NT 5.2; U; zh-cn)",
			"0.0048 	Opera/8.0 (Macintosh; PPC Mac OS X; U; en)",
			"0.0026 	Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en)Opera 8.0",
			"0.0025 	Netscape 7.1 (Win 98): Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
			"0.0035 	Netscape 4.8 ( Win XP): Mozilla/4.8 (Windows NT 5.1; U)",
			"0.0010 	Netscape 3.01 gold (Win 95): Mozilla/3.01Gold (Win95; I)",
			"0.0020 	Netscape 2.02 (Win 95): Mozilla/2.02E (Win95; U)",
			"0.2441		***"
	};
	
	private static String nextSeedAgent() {

		int len = rand.nextInt(20) + 5;
		char[] sagent = new char[len + 4];
		
		sagent[0] = (char) (rand.nextInt(26) + 'A');
		for (int i=1; i<len; i++) {
			sagent[i] = (char) (rand.nextInt(26) + 'a');
		}
		sagent[len++] = '/';
		sagent[len++] = (char) (rand.nextInt(9) + (int) ('0'));
		sagent[len++] = '.';
		sagent[len++] = (char) (rand.nextInt(9) + (int) ('0'));
		
		return new String(sagent);
	}
	
	private static String nextSeedWord() {
		
		int len = rand.nextInt(15) + 3;
		char[] sword = new char[len];
		for (int i=0; i<len; i++) {
			sword[i] = (char) (rand.nextInt(26) + 'a');
		}
		
		return new String(sword);
	}

	public static void createUserAgents(Path hdfs_uagent) throws IOException {
		Utils.checkHdfsPath(hdfs_uagent);
		
		FileSystem fs = hdfs_uagent.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_uagent);
		
		for (int i=0; i<rawUAgents.length; i++) {
			String[] pair = rawUAgents[i].split("\t");
			int num = (int) Math.round(Double.parseDouble(pair[0]) * numSourceUAgents);

			String content = "";
			for (int j=0; j<num; j++) {
				if ("***".equals(pair[pair.length-1])) {
					content = content + nextSeedAgent() + "\n";
				} else {
					content = content + pair[pair.length-1] + "\n";
				}
			}

			fout.write(content.getBytes("UTF-8"));
		}
		fout.close();
	}
	
	public static void createSearchKeys(Path hdfs_searchkeys) throws IOException {

		Utils.checkHdfsPath(hdfs_searchkeys);
		
		FileSystem fs = hdfs_searchkeys.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_searchkeys);

		File fdict = new File(dict);
		int len = 0;
		if (fdict.exists()) {
			
			FileReader fr = new FileReader(fdict);
			BufferedReader br = new BufferedReader(fr);
			while (null != br.readLine()) {
				len++;
			}
			br.close();
			fr.close();
			
			int[] wids = new int[numSourceWords];
			for (int i=0; i<numSourceWords; i++) {
				wids[i] = rand.nextInt(len);
			}
			Arrays.sort(wids);

			int i=0, j=0;
			File newfdict = new File(dict);
			FileReader newfr = new FileReader(newfdict);
			BufferedReader newbr = new BufferedReader(newfr);
			while ((i<wids.length) && (j<len)) {
				String wd = newbr.readLine();
				if (j==wids[i]) {
					wd = wd + "\n";
					while ((i<wids.length) && (j==wids[i])) {
						fout.write(wd.getBytes("UTF-8"));
						i++;
					}
				}
				j++;
			}
			newbr.close();
			newfr.close();
		} else {
			for (int i=0; i<numSourceWords; i++) {
				String wd = nextSeedWord() + "\n";
				System.out.print(wd);
				fout.write(wd.getBytes("UTF-8"));
			}
		}
		fout.close();
	}

	public static void createCCodes(Path hdfs_ccode) throws IOException {
		Utils.checkHdfsPath(hdfs_ccode);
		
		FileSystem fs = hdfs_ccode.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_ccode);
		
		Locale[] locales = Locale.getAvailableLocales();  
		for( Locale locale : locales ){
			String country = null, language = null;
			try {
				country = locale.getISO3Country();
				language = locale.getLanguage().toUpperCase();
			} catch (Exception e) {
				continue;
			}
			
			if (!"".equals(country) && !"".equals(language)) {
				String ccode = country + "," + country + "-" + language + "\n";
				fout.write(ccode.getBytes("UTF-8"));
			}
		}
		fout.close();
	}

	public static int putDictToHdfs(Path hdfs_dict, int size) throws IOException {

		Utils.checkHdfsPath(hdfs_dict);
		
		FileSystem fs = hdfs_dict.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_dict);

		File fdict = new File(dict);
		int len = 0;
		if (fdict.exists()) {
			
			FileReader fr = new FileReader(fdict);
			BufferedReader br = new BufferedReader(fr);
			
			while (len < size) {
				String word = br.readLine() + "\n";
//				if (null == word) break;
				
				fout.write(word.getBytes("UTF-8"));
				len++;
			}
			br.close();
			fr.close();
		}
		fout.close();
		return len;
	}
}