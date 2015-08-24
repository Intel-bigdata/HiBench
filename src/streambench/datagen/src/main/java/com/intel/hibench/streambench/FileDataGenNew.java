package com.intel.hibench.streambench;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class FileDataGenNew {

	
	public static ArrayList<byte[]> loadDataFromFile(String filepath){
        Path pt = new Path(filepath);
        FileSystem fs = FileSystem.get(new Configuration());

		ArrayList<byte[]> contents=new ArrayList<byte[]>();
		File dataFile=new File(filepath);
		System.out.println(dataFile.getAbsolutePath());
		try {
			BufferedReader reader=new BufferedReader(new FileReader(dataFile));
			String line=null;
			while((line=reader.readLine())!=null){
				contents.add(line.getBytes());
			}
			reader.close();
			return contents;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
}
