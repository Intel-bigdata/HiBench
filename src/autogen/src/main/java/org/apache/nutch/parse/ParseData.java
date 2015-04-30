package org.apache.nutch.parse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionMismatchException;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.nutch.metadata.Metadata;

public class ParseData extends VersionedWritable {

	public static final String DIR_NAME = "parse_data";
	
	private final static byte VERSION = 5;
	
	private String title;
	private Outlink[] outlinks;
	private Metadata contentMeta;
	private Metadata parseMeta;
	private ParseStatus status;
	private byte version = VERSION;
	  
	public ParseData(ParseStatus status, String title, Outlink[] outlinks,
			Metadata contentMeta, Metadata parseMeta) {
		this.status = status;
		this.title = title;
		this.outlinks = outlinks;
		this.contentMeta = contentMeta;
		this.parseMeta = parseMeta;
	}
	
	@Override
	public byte getVersion() {
		return version;
	}
	
	public final void readFields(DataInput in) throws IOException {

		version = in.readByte();
		// incompatible change from UTF8 (version < 5) to Text
		if (version != VERSION)
			throw new VersionMismatchException(VERSION, version);

		status = ParseStatus.read(in);
		title = Text.readString(in);                   // read title

		int numOutlinks = in.readInt();    
		outlinks = new Outlink[numOutlinks];
		for (int i = 0; i < numOutlinks; i++) {
			outlinks[i] = Outlink.read(in);
		}
		    
		if (version < 3) {
			int propertyCount = in.readInt();             // read metadata
			contentMeta.clear();
			for (int i = 0; i < propertyCount; i++) {
				contentMeta.add(Text.readString(in), Text.readString(in));
			}
		} else {
			contentMeta.clear();
			contentMeta.readFields(in);
		}
		if (version > 3) {
			parseMeta.clear();
			parseMeta.readFields(in);
		}
	}

	public final void write(DataOutput out) throws IOException {
		out.writeByte(VERSION);                       // write version
		status.write(out);                            // write status
		Text.writeString(out, title);                 // write title

		out.writeInt(outlinks.length);                // write outlinks
		for (int i = 0; i < outlinks.length; i++) {
			outlinks[i].write(out);
		}
		contentMeta.write(out);                      // write content metadata
		parseMeta.write(out);
	}

	public String toString() {

		StringBuffer buffer = new StringBuffer();

		buffer.append("Version: " + version + "\n" );
		buffer.append("Status: " + status + "\n" );
		buffer.append("Title: " + title + "\n" );

		if (outlinks != null) {
			buffer.append("Outlinks: " + outlinks.length + "\n" );
			for (int i = 0; i < outlinks.length; i++) {
				buffer.append("  outlink: " + outlinks[i] + "\n");
			}
		}

		buffer.append("Content Metadata: " + contentMeta + "\n" );
		buffer.append("Parse Metadata: " + parseMeta + "\n" );

		return buffer.toString();
	}
}
