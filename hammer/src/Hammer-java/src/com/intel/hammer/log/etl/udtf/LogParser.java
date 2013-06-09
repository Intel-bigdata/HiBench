package com.intel.hammer.log.etl.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import ua.main.java.eu.bitwalker.useragentutils.UserAgent;

import com.intel.hammer.log.util.DateTimeUtil;
import com.intel.hammer.log.util.IPUtil;

public class LogParser extends GenericUDTF {

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentLengthException(
					"LogParser takes only one argument");
		}
		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(
					"LogParser takes string as a parameter");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("w_date_sk");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_time_sk");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_page_sk");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_ip_sk");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_agent_sk");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_agent_raw");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_cookie");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_retcode");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_request");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("w_item_id");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	@Override
	public void process(Object[] args) throws HiveException {
		try {
			String input = args[0].toString();
			
			String ip = "";
			String cookie = "";
			String date = "";
			String time = "";
			String request = "";
			String retcode = "";
			String page = "";
			String agent_sk = "";
			String agent_raw = "";
			String item = "";
			
			String[] step1 = input.split("[\"\\[\\]]");

			String[] step2 = step1[0].split(" ");
			ip = Integer.toString(IPUtil.ipToInt(step2[0])); // IP
			cookie = step2[3]; // cookie

			step2 = step1[1].split(" ");
			date = Long.toString(DateTimeUtil.dateToInt(step2[0])); // date
			time = Long.toString(DateTimeUtil.timeToSec(step2[1])); // time

			request = step1[3]; // request

			step2 = step1[4].split(" ");
			retcode = step2[1]; // retcode
			String[] urls = step2[3].split("\\?item="); // url
//			String[] parts = urls[0].split("http://[a-z.]*/");
			
//			if(parts.length > 1) page = parts[1];
			page = "1";
			if(urls.length > 1) item = urls[1];

			agent_raw = step1[5]; // agent
			
			UserAgent ua = UserAgent.parseUserAgentString(agent_raw);
			agent_sk = Integer.toString(ua.getId());
			
			String[] result = {date,time,page,ip,agent_sk,agent_raw,cookie,retcode,request,item};
			forward(result);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
	}
}