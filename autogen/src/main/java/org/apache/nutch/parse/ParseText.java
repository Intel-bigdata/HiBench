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

package org.apache.nutch.parse;

import java.io.*;
import org.apache.hadoop.io.*;

public class ParseText implements Writable {

	public static final String DIR_NAME = "parse_text";
	private static final byte VERSION = 2;

	private String text;

	public ParseText() {}
	public ParseText(String text) { this.text = text; }

	public void readFields(DataInput in) throws IOException {
		byte version = in.readByte();
		switch (version) {
			case 1:
				text = WritableUtils.readCompressedString(in);
				break;
			case VERSION:
				text = Text.readString(in);
				break;
			default:
				throw new VersionMismatchException(VERSION, version);
		}
	}

	public final void write(DataOutput out) throws IOException {
		out.write(VERSION);
		Text.writeString(out, text);
	}

	public final static ParseText read(DataInput in) throws IOException {
		ParseText parseText = new ParseText();
		parseText.readFields(in);
		return parseText;
	}

	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}

	public boolean equals(Object o) {
		if (!(o instanceof ParseText))
			return false;
		ParseText other = (ParseText)o;
		return this.text.equals(other.text);
	}

	public String toString() {
		return text;
	}
}
