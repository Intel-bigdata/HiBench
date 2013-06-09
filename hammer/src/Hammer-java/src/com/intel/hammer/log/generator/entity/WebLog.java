package com.intel.hammer.log.generator.entity;

import java.util.Random;

import com.intel.hammer.log.util.RetCodeUtil;


public class WebLog {

	public String datetime = "-";
	public String cookieId = "-";
	public String info = "-";
	public String agent = "-";
	public String ip = "-";
	public String url = "-";
	public String retCode = "200";
	public int logStyle = 0;

	public String toString() {
		StringBuilder log = new StringBuilder("");
		if (logStyle == 0) {
			Random random = new Random();
			log.append(ip).append(" - - ").append(cookieId).append(" [")
					.append(datetime).append("] \"").append(info).append("\" ")
					.append(RetCodeUtil.getRetCode(Math.abs(random.nextInt()))).append(" ")
					.append(random.nextInt(2000)).append(" ").append(url)
					.append(" \"").append(agent).append("\"");
		}
		return log.toString();
	}
}
