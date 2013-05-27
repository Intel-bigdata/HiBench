package com.intel.hammer.log.generator.entity;

import com.intel.hammer.log.util.DateTimeUtil;
import com.intel.hammer.log.util.IPUtil;
import com.intel.hammer.log.util.UserAgentUtil;
import com.intel.hammer.log.util.MD5Util;


public class SalesRow {
	public String customer = "";
	public String item = "";
	public String date = "";
	public String time = "";
	public String url = "";
	
	public WebLog createLog(){
		WebLog log = new WebLog();
		
		log.agent = UserAgentUtil.getAgent(Integer.parseInt(customer+date.substring(5, 7)));
		log.cookieId = MD5Util.MD5(customer+date.substring(5, 7));
		log.datetime = DateTimeUtil.getDateTime(date, time);
		log.ip = IPUtil.intToip(Integer.parseInt(customer+date.substring(5, 7)));
		log.url = url + "?item=" + item;
		log.info = "GET " + "?item=" + item + " HTTP/1.1";
		
		return log;
	}
}
