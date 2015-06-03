package com.intel.PRCcloud;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

public class FileDataGenNew {

	protected static final String RESOURCE_LOCATION="src/main/resources/";
	
	public static ArrayList<byte[]> loadDataFromFile(String filename){
		ArrayList<byte[]> contents=new ArrayList<byte[]>();
		URL url=FileDataGenNew.class.getResource("/"+RESOURCE_LOCATION+filename);
		File dataFile=new File(url.getFile());
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
