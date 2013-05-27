package com.intel.hammer.log.util;

import java.net.InetAddress;

public class IPUtil {
	public static String intToip(int ipInt) {
		return new StringBuilder().append(((ipInt >> 24) & 0xff)).append('.')
				.append((ipInt >> 16) & 0xff).append('.')
				.append((ipInt >> 8) & 0xff).append('.').append((ipInt & 0xff))
				.toString();
	}

	public static int ipToInt(String ipAddr) {
        try {
            byte[] bytes = InetAddress.getByName(ipAddr).getAddress();
            
            int addr = bytes[3] & 0xFF;
            addr |= ((bytes[2] << 8) & 0xFF00);
            addr |= ((bytes[1] << 16) & 0xFF0000);
            addr |= ((bytes[0] << 24) & 0xFF000000);
            
            return addr;
        } catch (Exception e) {
            throw new IllegalArgumentException(ipAddr + " is invalid IP");
        }
    }
}
