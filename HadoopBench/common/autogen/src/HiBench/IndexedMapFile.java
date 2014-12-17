package HiBench;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class IndexedMapFile {

	private static final long URL_CACHE_LIMIT = 1600000;
	private int slots;
	private HashMap<Long, Text> hash;
	private MapFile.Reader[] readers;
	
	IndexedMapFile(int slots, MapFile.Reader[] readers) {
		this.slots = slots;
		hash = new HashMap<Long, Text>();
		this.readers = readers;
	}
	
	public Text get(long id) throws IOException {
		Text value = new Text();
		
		if (id < URL_CACHE_LIMIT) {
			Text v = hash.get(id);
			if (null != v) {
				return v;
			} else {
				int vid = (int) (id % slots);
				readers[vid].get(new LongWritable(id), value);
				hash.put(id, value);
				return value;
			}
		} else {
			int vid = (int) (id % slots);
			readers[vid].get(new LongWritable(id), value);
			return value;
		}
	}
	
	public void close() throws IOException {
		if (null != readers) {
			for (int i=0; i<readers.length; i++) {
				if (null != readers[i]) {
					readers[i].close();
				}
			}
		}
	}
}
