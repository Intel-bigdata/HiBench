package HiBench;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

public class Utils {

	private static final Log log = LogFactory.getLog(Utils.class.getName());
	
	public static final void checkHdfsPath(Path path) throws IOException {
		
		FileSystem fs = path.getFileSystem(new Configuration());
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		fs.close();
	}
	
	public static final void createHdfsDir(Path path) throws IOException {

		FileSystem fs = path.getFileSystem(new Configuration());
		if (!fs.exists(path)) {
			fs.mkdirs(path);
		}
		fs.close();
	}
	
	public static final void checkHdfsPath(Path path, boolean mkdir)
			throws IOException {

		FileSystem fs = path.getFileSystem(new Configuration());
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
		
		if (mkdir) {
			fs.mkdirs(path);
		}
		fs.close();
	}

	public static final void cleanTempFiles (Path file) throws IOException {
		Path ftemp = new Path(file, "_logs");
		FileSystem fs = ftemp.getFileSystem(new Configuration());
		fs.delete(ftemp, true);
		fs.close();
	}

	public static final int getMaxNumReds () throws IOException {
		JobConf job = new JobConf(Utils.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		return cluster.getMaxReduceTasks();
	}
	
	public static final int getNumAvailableReds () throws IOException {
		JobConf job = new JobConf(Utils.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		int maxReduces = cluster.getMaxReduceTasks();
		int runnings = cluster.getReduceTasks();
		return maxReduces - runnings;
	}
	
	public static final int getMaxNumMaps () throws IOException {
		JobConf job = new JobConf(Utils.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		return cluster.getMaxMapTasks();
	}
	
	public static final int getNumAvailableMaps () throws IOException {
		JobConf job = new JobConf(Utils.class);
		JobClient client = new JobClient(job);
		ClusterStatus cluster = client.getClusterStatus();
		int maxMaps = cluster.getMaxMapTasks();
		int runnings = cluster.getMapTasks();
		return maxMaps - runnings;
	}
	
	private static final void shareArray(String symbol, Path fdict, int words, JobConf job) throws URISyntaxException {
		DistributedCache.createSymlink(job);
		String uridict = fdict.toUri().toString() + "#" + symbol;
		DistributedCache.addCacheFile(new URI(uridict), job);
		job.setInt(symbol, words);
	}
	
	private static final String[] getSharedArray(String symbol, JobConf job) throws IOException {
		
		String[] dict = null;
		int words = job.getInt(symbol, 0);
		if (words > 0) {
			dict = new String[words];
			
			FileReader reader = new FileReader(symbol);
			BufferedReader br = new BufferedReader(reader);
			String line = null;
			int len = 0;
			while ((line = br.readLine()) != null) {
				dict[len++] = line;
			}
			br.close();
			reader.close();
		}
		return dict;
	}
	
	public static final void shareDict(DataOptions options, JobConf job) throws URISyntaxException {
		shareArray(HtmlCore.WORD_DICT_DIR_NAME, new Path(options.getWorkPath(), HtmlCore.WORD_DICT_DIR_NAME), options.getNumWords(), job);
	}
	
	public static final String[] getDict(JobConf job) throws IOException {
		return getSharedArray(HtmlCore.WORD_DICT_DIR_NAME, job);
	}
	
    private static PathFilter getPassDirectoriesFilter(final FileSystem fs) {
    	
        return new PathFilter() {
            public boolean accept(final Path path) {
                try {
                    return fs.getFileStatus(path).isDir() &&
                            !path.getName().startsWith("_");
                    
                } catch (IOException ioe) {
                    return false;
                }
            }
        };
    }
    
	private static final void shareMapFile(String symbol, int slots, Path mfile, JobConf job) throws IOException, URISyntaxException {
		
		FileSystem fs = FileSystem.get(job);
		if (fs.exists(mfile) && fs.getFileStatus(mfile).isDir()) {

			DistributedCache.createSymlink(job);
			
			FileStatus[] fstats = fs.listStatus(mfile, getPassDirectoriesFilter(fs));
			
			LongWritable key = new LongWritable();
			Text value = new Text();
			for (int i=0; i<fstats.length; i++) {
				Path curMap = fstats[i].getPath();
				MapFile.Reader mreader = new MapFile.Reader(fs, curMap.toString(), job);
				if (mreader.next(key, value)) {
					int rid = (int) (key.get() % slots);
					String uriWithLink =
							curMap.toUri().toString() + "#" + symbol + "-" + Integer.toString(rid);
					DistributedCache.addCacheFile(new URI(uriWithLink), job);
				} else {
					System.exit(-1);
				}
				mreader.close();
			}
		}
		
		job.setInt(symbol, slots);
	}
	
	public static final void shareUrls (String symbol, DataOptions options, JobConf job) throws IOException, URISyntaxException {
		shareMapFile(symbol, options.getNumMaps(), new Path(options.getWorkPath(), symbol), job);
	}
	
	public static final IndexedMapFile getSharedMapFile(String symbol, JobConf job) throws IOException {
		
		int slots = job.getInt(symbol, 0);
		
		if (slots <=0) {
			log.error("slots number should be no less than 1 !!!");
			System.exit(-1);
		}
		
		FileSystem fs = FileSystem.getLocal(job);
		MapFile.Reader[] readers = new MapFile.Reader[slots];
		for (int i=0; i<slots; i++) {
			String symbfile = fs.getWorkingDirectory().toString() + "/" + symbol + "-" + Integer.toString(i);
			readers[i] = new MapFile.Reader(fs, symbfile, job);
		}
		
		return new IndexedMapFile(slots, readers);
	}
	
	/***
	 * Steps to make a ZipfCore available for each job
	 * Client side
	 * 		1. Zipfian creates its corresponding ZipfCore object
	 * 		2. serialize the ZipfCore into a hdfs file
	 * 		3. share the hdfs file by putting it into distributed cache file
	 * Job side
	 * 		1. read object from distributed cache file to re-create the ZipfCore
	 * @throws IOException 
	 */
	private static final void serialZipfCore(Zipfian zipfian, Path hdfs_zipf) throws IOException {
		
		Utils.checkHdfsPath(hdfs_zipf);
		
		FileSystem fs = hdfs_zipf.getFileSystem(new Configuration());
		FSDataOutputStream fout = fs.create(hdfs_zipf);

		ObjectOutputStream so = new ObjectOutputStream(fout);
		
		ZipfCore core = zipfian.createZipfCore();
		so.writeObject(core);
		
		so.close();
		fout.close();
		fs.close();
	}
	
	private static final void shareZipfCore(String fname, DataOptions options, JobConf job) throws URISyntaxException {

		DistributedCache.createSymlink(job);
		
		Path zipfPath = new Path(options.getWorkPath(), fname);
		String uriWithLink = zipfPath.toString() + "#" + fname;
		DistributedCache.addCacheFile(new URI(uriWithLink), job);
	}
	
	private static final ZipfCore getSharedZipfCore(String fname, JobConf job) throws IOException, ClassNotFoundException {
		
		ZipfCore zipfcore = null;
		
		FileSystem fs = FileSystem.getLocal(job);
		Path symbLink = new Path(fname);
		if (fs.exists(symbLink)) {
			FileInputStream fi = new FileInputStream(symbLink.toString());
			ObjectInputStream si = new ObjectInputStream(fi);
			
			zipfcore = (ZipfCore) si.readObject();
			si.close();
		}
		return zipfcore;
	}
	
	public static final ZipfCore getSharedLinkZipfCore(JobConf job) throws ClassNotFoundException, IOException {
		return getSharedZipfCore(HtmlCore.LINK_ZIPF_FILE_NAME, job);
	}
	
	public static final ZipfCore getSharedWordZipfCore(JobConf job) throws ClassNotFoundException, IOException {
		return getSharedZipfCore(HtmlCore.WORD_ZIPF_FILE_NAME, job);
	}
	
	public static final void shareLinkZipfCore(DataOptions options, JobConf job) throws URISyntaxException {
		shareZipfCore(HtmlCore.LINK_ZIPF_FILE_NAME, options, job);
	}
	
	public static final void shareWordZipfCore(DataOptions options, JobConf job) throws URISyntaxException {
		shareZipfCore(HtmlCore.WORD_ZIPF_FILE_NAME, options, job);
	}
	
	public static final void serialLinkZipf(DataOptions options) throws IOException {
		if (options.getNumPages() > 0) {
			Zipfian lzipfian = new Zipfian(options.getNumPages(), DataOptions.LINK_ZIPF_EXPONENT);
			lzipfian.setupZipf((long) (options.getNumPages() * HtmlCore.getMeanLinksPerPage()), DataOptions.LINK_SIMULATE_SPACE_RATIO);
			Path lzipfCorePath = new Path(options.getWorkPath(), HtmlCore.LINK_ZIPF_FILE_NAME);
			serialZipfCore(lzipfian, lzipfCorePath);
		} else {
			System.out.println("ERROR: number of pages should be greater than 0");
			System.exit(-1);
		}
	}
	
	public static final void serialWordZipf(DataOptions options) throws IOException {
		if (options.getNumWords() > 0) {
			Zipfian wzipfian = new Zipfian(options.getNumWords(), DataOptions.WORD_ZIPF_EXPONENT);
			wzipfian.setupZipf((long) (options.getNumPages() * HtmlCore.getMeanWordsPerPage()), DataOptions.WORD_SIMULATE_SPACE_RATIO);
			Path wzipfCorePath = new Path(options.getWorkPath(), HtmlCore.WORD_ZIPF_FILE_NAME);
			serialZipfCore(wzipfian, wzipfCorePath);
		} else {
			System.out.println("ERROR: number of words should be greater than 0");
			System.exit(-1);
		}
	}
}
