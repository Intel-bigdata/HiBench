package com.intel.hammer.log.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateTimeUtil {
	public static String getDateTime(String date, String time) {
		java.sql.Date sqlDate = java.sql.Date.valueOf(date);
		java.util.Calendar Cal = java.util.Calendar.getInstance();
		Cal.setTime(sqlDate);
		Cal.add(java.util.Calendar.SECOND, (int) Double.parseDouble(time));
		DateFormat df = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss -S",
				Locale.US);
		return df.format(Cal.getTime());
	}

	public static long timeToSec(String time) {
		String[] my = time.split(":");
		int hour = Integer.parseInt(my[0]);
		int min = Integer.parseInt(my[1]);
		int sec = Integer.parseInt(my[2]);

		long zong = hour * 3600 + min * 60 + sec;
		return zong;
	}

	public static long dateToInt(String date) {
		try {
			SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy");
			Date d1 = sdf.parse(date);
			Date d2 = sdf.parse("02/Jan/1900");
			long daysBetween = (d1.getTime() - d2.getTime() + 1000000)
					/ (3600 * 24 * 1000);
			return daysBetween + 2415022;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return -1;

	}
}
