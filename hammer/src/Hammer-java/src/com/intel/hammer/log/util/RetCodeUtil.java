package com.intel.hammer.log.util;

public class RetCodeUtil {
	private static int SERVER_RETCODE_NUMBER = -1;
	private static String[] SERVER_RETCODE = { "100", "101", "200", "201",
		"202", "203", "204", "205", "206", "300", "301", "302", "303",
		"304", "305", "307", "400", "401", "402", "403", "404", "405",
		"406", "407", "408", "409", "410", "411", "412", "413", "414",
		"415", "416", "417", "500", "501", "502", "503", "504", "505" };
	
	public static String getRetCode(int index){
		if(SERVER_RETCODE_NUMBER < 0){
			SERVER_RETCODE_NUMBER = SERVER_RETCODE.length;
		}
		if(index < 0){
			index = 2;
		}
		if(index >= SERVER_RETCODE_NUMBER){
			index = index % SERVER_RETCODE_NUMBER;
		}
		return SERVER_RETCODE[index];
	}
}
