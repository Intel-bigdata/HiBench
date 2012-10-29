package HiBench;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;


public class NutchParse implements Writable {

	public Inlinks inlinks;
	public ParseText text;
	public ParseData data;
	
	NutchParse(Inlinks inlinks, ParseText text, ParseData data) {
		this.inlinks = inlinks;
		this.text = text;
		this.data = data;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
