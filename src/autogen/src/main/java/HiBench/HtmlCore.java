package HiBench;

import java.io.IOException;
import java.nio.Buffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

/***
 * Settings for creating HTML pages, used by CreateHtmlPagesMapper
 * @author lyi2
 *
 */
public class HtmlCore {
	
	private static final Log log = LogFactory.getLog(HtmlCore.class.getName());

	private static final int maxUrlLength =100;
	private static final int minUrlLength =10;

	private static final double linkratio = 0.05;
	private static final double linkoutratio = 0.005;	// 1% of link(ratio=5%)
	private static final double wordsymbolratio = 0.19;	// 20% of word(ratio=95%)
	private static final double wordtitleratio = 0.01;	// ~1% of word(ratio=95%)

	private static final int meanContentLen = 800;		// Guassian mean of content length
	private static final double varContentLen = 80;		// Guassian variance of content length
	private static final double gaussLowerLimit = - (meanContentLen / varContentLen);
	private static final double gaussUpperLimit = (Short.MAX_VALUE - meanContentLen) / varContentLen;
	
	private static final int meanBayesLen = 500;
	
	private static final double epercent = 0.5;
	
	public static final int INT_BYTE = Integer.SIZE / Byte.SIZE;
	public static final int LONG_BYTE = Long.SIZE / Byte.SIZE;
	public static final int CHAR_BYTE = Character.SIZE / Byte.SIZE;
	public static final int MAX_SHORT = Short.MAX_VALUE;

	public static final String LINK_ZIPF_FILE_NAME = "linkzipf";
	public static final String WORD_ZIPF_FILE_NAME = "wordzipf";
	public static final String WORD_DICT_DIR_NAME = "worddict";
	
	private static Random randRandSeed;		// special rand to create random seeds
	private static Random randUrl, randPageGo, randElinks;
	
	public int slots;
	public long pages, slotpages, totalpages, outpages;
	private ZipfCore lzipf, wzipf;
	
	private String[] dict;
	
	private int llen;
	private long[] wordids, linkids;

	private void printDict() {
		if (null != dict) {
			log.info("[dict] slots: " + slots + " length: " + dict.length);
			for (int i=0; i<dict.length; i = i + slots) {
				log.info(i + ": " + dict[i]);
			}
		} else {
			log.info("WARNING: dict empty!!!");
		}
	}
	
	HtmlCore(JobConf job) throws IOException {
		pages = job.getLong("pages", 0);
		slotpages = job.getLong("slotpages", 0);
		slots = (int) Math.ceil((pages * 1.0 / slotpages));
		outpages = (long) Math.floor(pages * epercent);
		totalpages = pages + outpages;

		dict = Utils.getDict(job);
		if (DataGen.DEBUG_MODE) {
			printDict();
		}
		
		wordids = new long[getMeanContentLength() * 2];
		linkids = new long[wordids.length];

		try {
			this.wzipf = Utils.getSharedWordZipfCore(job);
			this.lzipf = Utils.getSharedLinkZipfCore(job);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static final int getMaxUrlLength () {
		return maxUrlLength;
	}
	
	public static final int getMeanContentLength() {
		return meanContentLen;
	}
	
	public static final double getMeanLinksPerPage () {
		return meanContentLen * linkratio;
	}

	public static final double getMeanWordsPerPage () {
		return meanContentLen * (1 - linkratio);
	}


	public static long[] getPageRange(int slotId, long limit, long slotlimit) {
		long[] range = new long[2];
		range[0] = slotlimit * (slotId - 1);
		range[1] = range[0] + slotlimit;
		if (range[1] > limit) {
			range[1] = limit;
		}
		return range;
	}

	public void fireRandom(int rseed) {
		randRandSeed = new Random(rseed);

		randUrl = new Random(randRandSeed.nextLong());
		randElinks = new Random(randRandSeed.nextLong());
		if (null != lzipf) {
			lzipf.setRandSeed(randRandSeed.nextLong());
		}
		if (null != wzipf) {
			wzipf.setRandSeed(randRandSeed.nextLong());
		}
		randPageGo = new Random(randRandSeed.nextLong());
	}
	
	public int nextUrlLength()
	{
		return (int) Math.round(
				randPageGo.nextInt(maxUrlLength-minUrlLength+1)+minUrlLength);
	}

	public int nextUrlBytes(byte[] url) {
		int ulen = nextUrlLength();
		for (int i=0; i<ulen; i++) {
			url[i] = (byte) (randUrl.nextInt(26) + 'a');
		}
		return ulen;
	}

	public void nextUrlJoinBytesInt(JoinBytesInt item) {
		item.ulen = (byte) nextUrlLength();
		for (int i=0; i<item.ulen; i++) {
			item.url[i] = (byte) (randUrl.nextInt(26) + 'a');
		}
	}
	
	public byte[] nextUrlBytes() {
		int ulen = nextUrlLength();
		byte[] url = new byte[ulen];
		for (int i=0; i<ulen; i++) {
			url[i] = (byte) (randUrl.nextInt(26) + 'a');
		}
		return url;
	}
	
	public Text nextUrlText() {
		Text result = new Text("http://");
		
		int ulen = nextUrlLength();
		byte[] url = new byte[ulen + 7];
		for (int i=0; i<ulen; i++) {
			url[i] = (byte) (randUrl.nextInt(26) + 'a');
		}
		result.append(url, 0, ulen);
		return result;
	}

	public int nextContentLength() {
		double gauss = 0;
		do {
			gauss = randPageGo.nextGaussian();
		} while ((gauss < gaussLowerLimit) || (gauss > gaussUpperLimit));
		
		return (int) Math.round(meanContentLen
				+ varContentLen * gauss);
	}
	
	private long nextElink() {
		return pages + (long) Math.floor(randElinks.nextDouble() * outpages);
	}

	public String printBuffer(Buffer buf) {
		String info =
				buf.toString() +
				"< " + buf.position() +
				", " + buf.mark() +
				", " + buf.limit() + 
				", " + buf.capacity() + " >";
		return info;
	}
/*
	private void generateLinks(long pageId, HtmlLinks links) {

		links.len = (int) Math.floor(linkratio * nextContentLength());
		if (links.space.length < links.len) {
			links.space = new long[links.len];
		}
		for (int i=0; i<links.len; i++) {
			links.space[i] = lzipf.next();
		}
	}
*/
	public long[] genPureLinkIds () {
		long[] links = new long[(int) Math.floor(linkratio * nextContentLength())];
		for (int i=0; i<links.length; i++) {
			links[i] = lzipf.next();
		}
		return links;
	}
	
	public long[] genPureWordIds() {
		long[] words = new long[(int) Math.floor((1 - linkratio) * nextContentLength())];
		for (int i=0; i<words.length; i++) {
			words[i] = wzipf.next();
		}
		return words;
	}

	public References genPageLinks() {

		int pageLength = this.nextContentLength();
		if (linkids.length < (2 * pageLength)) {
			linkids = new long[2 * pageLength];
		}
		llen = 0;

		for (int i=0; i<pageLength; i++) {
			double fact = randPageGo.nextDouble();
			if (fact < linkratio) {
				// gen link
				linkids[llen++] = lzipf.next();
				if (fact < linkoutratio) {
					linkids[llen++] = this.nextElink();
				}
			}
		}
		
		return new References(-llen, linkids);
	}
	
	public int nextBayesLength() {
		
		double gauss = 0;
		do {
			gauss = randPageGo.nextGaussian();
		} while ((gauss <= gaussLowerLimit) || (gauss >= gaussUpperLimit));
		
		return (int) Math.round(meanContentLen
				+ varContentLen * gauss);
		
	}
	
	public String genBayesWords() {
		
		int len = (int) Math.ceil(this.nextContentLength() * 1.0 * meanBayesLen / meanContentLen);
		StringBuffer words = new StringBuffer("");
		for (int i=0; i<len; i++) {
			words.append(dict[(int) wzipf.next()]).append(" ");
		}
		
		return words.toString().trim();
	}
	
	public String genPageWords() {

		int pageLength = this.nextContentLength();
		StringBuffer words = new StringBuffer("");
		for (int i=0; i<pageLength; i++) {
			double fact = randPageGo.nextDouble();
			if (fact >= linkratio) {
				words.append(dict[(int) wzipf.next()]).append(" ");
				if (fact - linkratio < wordsymbolratio) {
					words.append("- ");
				}
			}
		}
		
		return words.toString().trim();
	}
	
	public String[] genPageWordsAndTitls() {

		int pageLength = this.nextContentLength();
		StringBuffer words = new StringBuffer(pageLength * 30);
		StringBuffer title = new StringBuffer(200);
		for (int i=0; i<pageLength; i++) {
			double fact = randPageGo.nextDouble();
			if (fact >= linkratio) {

				if (null != dict) {
					if (null != wzipf) {
						String w = dict[(int) wzipf.next()];
						words.append(w).append(" ");
						fact = fact - linkratio;
						if (fact < wordsymbolratio) {
							words.append("- ");
						}
						if (fact < wordtitleratio) {
							title.append(w).append(" ");
						}
					} else {
						log.info("wzipf NULL!!!");
					}
				} else {
					log.info("wordmap NULL!!!");
				}
			}
		}
		
		String[] result = new String[2];
		result[0] = words.toString().trim();
		result[1] = title.toString().trim();
		
		return result;
	}

	public String genPageText () {
		StringBuffer text = new StringBuffer();
		
		int tlen = (int) Math.floor((1 - linkratio) * nextContentLength());
		for (int i=0; i<tlen; i++) { 
			text.append(dict[(int) wzipf.next()]).append(" ");
		}
		return text.toString().trim();
	}

	public static final String getDictName() {
		return WORD_DICT_DIR_NAME;
	}
	
	public void setDict(String[] dict) {
		this.dict = dict;
	}
}
