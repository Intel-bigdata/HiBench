package com.intel.hammer.log.util;

import java.net.InetAddress;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;

public class MD5Util {
	private static HashMap<String, String> MD5_MAP = null;

	public static String MD5(String origin) {
		if (MD5_MAP == null) {
			MD5_MAP = new HashMap<String, String>();
		} else {
			if (MD5_MAP.containsKey(origin)) {
				return MD5_MAP.get(origin);
			}
		}

		String md5 = _MD5(origin);
		MD5_MAP.put(origin, md5);
		if(MD5_MAP.size() > 500){
			MD5_MAP.remove(MD5_MAP.keySet().iterator().next());
		}
		return md5;
	}

	private static String _MD5(String origin) {
		StringBuilder sb = new StringBuilder(32);

		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			byte[] array = md.digest(origin.getBytes("utf-8"));

			for (int i = 0; i < array.length; i++) {
				sb.append(Integer.toHexString((array[i] & 0xFF) | 0x100)
						.toUpperCase().substring(1, 3));
			}
		} catch (Exception e) {
			return null;
		}

		return sb.toString();
	}
}
