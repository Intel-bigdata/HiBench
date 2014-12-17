package HiBench;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

public class DataOptions {

//	private static final Log log = LogFactory.getLog(DataOptions.class.getName());
	
	public static final double LINK_SIMULATE_SPACE_RATIO = 0.1;
	public static final double WORD_SIMULATE_SPACE_RATIO = 0.1;

	public static final double LINK_ZIPF_EXPONENT = 0.5;
	public static final double WORD_ZIPF_EXPONENT = 0.9;

	private static final String TEMP_DIR = "temp";
	private static final int NUM_LINUX_DICT_WORD = 479623;

	public static enum DataType {
		HIVE, PAGERANK, BAYES, NUTCH, NONE
	}
	private DataType type;

	private String base, dname;
	private Path workPath, resultPath;
	
	private int maps, reds;
	private long pages, slotpages;
	private int words;
	
	private boolean sequenceOut;
	private Class<? extends CompressionCodec> codecClass;
	
	private StringBuffer remainArgs;

	DataOptions(String[] args) throws ClassNotFoundException {
		
		type = DataType.NONE;
		base = "/HiBench";
		maps = -1;
		reds = -1;
		pages = -1;
		words = -1;
		sequenceOut = false;
		codecClass = null;
		remainArgs = new StringBuffer("");

		if (args.length < 2) {
			System.exit(printUsage("Error: number of arguments should be no less than 2!!!"));
		}

		if ("-t".equals(args[0])) {
			if ("hive".equalsIgnoreCase(args[1])) {
				type = DataType.HIVE;
				dname = "hive";
			} else if ("pagerank".equalsIgnoreCase(args[1])) {
				type = DataType.PAGERANK;
				dname = "pagerank";
			} else if ("bayes".equalsIgnoreCase(args[1])) {
				type = DataType.BAYES;
				words = NUM_LINUX_DICT_WORD;
				dname = "bayes";
			} else if ("nutch".equalsIgnoreCase(args[1])) {
				type = DataType.NUTCH;
				words = NUM_LINUX_DICT_WORD;
				dname = "nutch";
			} else {
				System.exit(printUsage("Error: arguments syntax error!!!"));
			}
		}

		for (int i=2; i<args.length; i++) {
			if ("-m".equals(args[i])) {
				maps = Integer.parseInt(args[++i]);
			} else if ("-r".equals(args[i])) {
				reds = Integer.parseInt(args[++i]);
			} else if ("-p".equals(args[i])) {
				pages = Long.parseLong(args[++i]);
			} else if ("-b".equals(args[i])) {
				base = args[++i];
			} else if ("-n".equals(args[i])) {
				dname = args[++i];
			} else if ("-o".equals(args[i])) {
				if ("sequence".equalsIgnoreCase(args[++i])) {
					sequenceOut = true;
				}
			} else if ("-c".equals(args[i])) {
				codecClass =
						Class.forName(args[++i]).asSubclass(CompressionCodec.class);
			} else {
				remainArgs.append(args[i]).append(" ");
				remainArgs.append(args[++i]).append(" ");
			}
		}
		
		checkOptions();
		
		slotpages = (long) Math.ceil(pages * 1.0 / maps);

		resultPath = new Path(base, dname);
//		workPath = new Path(resultPath, TEMP_DIR);
		workPath = new Path(base, TEMP_DIR);
	}
	
	private void checkOptions() {
		
		switch (type) {
		case HIVE:
			if (pages<=0) {
				System.exit(printUsage("Error: pages of hive data should be larger than 0!!!"));
			}
			break;
		case PAGERANK:
			if (pages<=0) {
				System.exit(printUsage("Error: pages of pagerank data should be larger than 0!!!"));
			}
			break;
		case BAYES:
			if (pages<=0 || words<=0) {
				System.exit(printUsage("Error: pages/words of bayes data should be larger than 0!!!"));
			}
			break;
		case NUTCH:
			if (pages<=0 || words<=0) {
				System.exit(printUsage("Error: pages/words of nutch data should be larger than 0!!!"));
			}
			break;
		default:
			System.exit(printUsage("Error: type of data not defined!!!"));
		}
	}
	
	public Path getWorkPath() {
		return workPath;
	}
	
	public Path getResultPath() {
		return resultPath;
	}
	
	public static final int printUsage(String msg) {
		
		if (null != msg) {
			System.out.println(msg);
			System.out.println();
		}
		
		System.out.println("generate -t hive -p <pages> -v <visits> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>] [-d <delimiter>]");
		
		System.out.println("generate -t pagerank -p <pages> "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>]");
		
		System.out.println("generate -t nutch -p <pages> [-w <words>] "
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>]");
		
		System.out.println("generate -t bayes -p <pages> [-w <words>] -g <num classes>"
				+ "[-b <base path>] [-n <data name>] "
				+ "[-m <num maps>] [-r <num reduces>] "
				+ "[-o sequence] [-c <codec>]");
		
		return -1;
	}
	
	public String[] getRemainArgs() {
		return remainArgs.toString().trim().split(" ");
	}
	
	public DataType getType() {
		return type;
	}
	
	public int getNumMaps() {
		return maps;
	}
	
	public int getNumReds() {
		return reds;
	}
	
	public long getNumPages() {
		return pages;
	}
	
	public long getNumSlotPages() {
		return slotpages;
	}
	
	public int getNumWords() {
		return words;
	}
	
	public void setNumWords(int words) {
		this.words = words;
	}
	
	public boolean isSequenceOut() {
		return sequenceOut;
	}
	
	public Class<? extends CompressionCodec> getCodecClass() {
		return codecClass;
	}
}
