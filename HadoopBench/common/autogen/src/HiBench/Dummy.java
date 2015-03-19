package HiBench;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Dummy {
	
	private static final Log log = LogFactory.getLog(Dummy.class.getName());
	private static final String NAME = "dummy";
	
	private Path path;
	private int slots;
	
	Dummy(Path path, int slots) throws IOException {
		this.path = new Path(path, NAME);
		this.slots = slots;
		this.create();
	}
	
	public void create() throws IOException {
		
		log.info("Creating dummy file " + path + " with " + slots + " slots...");

		Utils.checkHdfsPath(path);
		
		FileSystem fs = path.getFileSystem(new Configuration());
		FSDataOutputStream out = fs.create(path);

		String contents = "";
		for (int i=1; i<=slots; i++) {
			contents = contents.concat(Integer.toString(i) + "\n");
		}

		out.write(contents.getBytes("UTF-8"));
		out.close();
	}

	public Path getPath() {
		return path;
	}
	
	public String getName() {
		return path.getName();
	}
}
