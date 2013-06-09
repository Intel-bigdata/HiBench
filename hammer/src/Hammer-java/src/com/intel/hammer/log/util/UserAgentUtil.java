package com.intel.hammer.log.util;

public class UserAgentUtil {
	private static int AGENTS_NUMBER = -1;
	public static String getAgent(int index){
		if(AGENTS_NUMBER < 0){
			AGENTS_NUMBER = AGENTS_MAP.length;
		}
		if(index < 0){
			index = 0;
		}
		if(index >= AGENTS_NUMBER){
			index = index % AGENTS_NUMBER;
		}
		
		return AGENTS_MAP[index];
	}
	
	public static String[] AGENTS_MAP = {
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
		"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
		"Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1)",
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"msnbot/1.1 (+http://search.msn.com/msnbot.htm)",
		"Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)",
		"Mozilla/5.0 (iPhone; U; CPU iPhone OS 3_0 like Mac OS X; en-us) AppleWebKit/528.18 (KHTML, like Gecko) Version/4.0 Mobile/7A341 Safari/528.16",
		"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
		"Mozilla/4.8 [en] (Windows NT 6.0; U)",
		"Opera/9.20 (Windows NT 6.0; U; en)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
		"Mozilla/4.0 (compatible; MSIE 5.5; Windows NT 5.0 )",
		"Mozilla/4.0 (compatible; MSIE 5.5; Windows 98; Win 9x 4.90)",
		"Avant Browser/1.2.789rel1 (http://www.avantbrowser.com)",
		"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0; en) Opera 8.0",
		"Opera/7.51 (Windows NT 5.1; U) [en]",
		"Opera/7.50 (Windows XP; U)",
		"Opera/7.50 (Windows ME; U) [en]",
		"Mozilla/5.0 (Windows; U; Windows XP) Gecko MultiZilla/1.6.1.0a",
		"Mozilla/5.0 (Windows; U; Win98; en-US; rv:1.4) Gecko Netscape/7.1 (ax)",
		"Mozilla/4.8 [en] (Windows NT 5.1; U)",
		"Mozilla/3.01Gold (Win95; I)",
		"Mozilla/2.02E (Win95; U)",
		"Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
		"Googlebot/2.1 (+http://www.googlebot.com/bot.html)",
		"msnbot/1.0 (+http://search.msn.com/msnbot.htm)",
		"msnbot/0.11 (+http://search.msn.com/msnbot.htm)",
		"Mozilla/5.0 (compatible; Yahoo! Slurp; http://help.yahoo.com/help/us/ysearch/slurp)",
		"Mozilla/2.0 (compatible; Ask Jeeves/Teoma)",
		"Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML, like Gecko) Safari/125.8",
		"Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en) AppleWebKit/125.2 (KHTML, like Gecko) Safari/85.8",
		"Mozilla/4.0 (compatible; MSIE 5.15; Mac_PowerPC)",
		"Mozilla/5.0 (Macintosh; U; PPC Mac OS X Mach-O; en-US; rv:1.7a) Gecko/20040614 Firefox/0.9.0+",
		"Mozilla/5.0 (Macintosh; U; PPC Mac OS X; en-US) AppleWebKit/125.4 (KHTML, like Gecko, Safari) OmniWeb/v563.15",
		"ozilla/5.0 (X11; U; Linux; i686; en-US; rv:1.6) Gecko Epiphany/1.2.5",
		"Mozilla/5.0 (X11; U; Linux i586; en-US; rv:1.7.3) Gecko/20040924 FireFox/3.6.3 (Ubuntu)",
		"Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.6) Gecko/20040614 Firefox/0.8",
		"Mozilla/5.0 (X11; U; Linux; i686; en-US; rv:1.6) Gecko Galeon/1.3.14",
		"Konqueror/3.0-rc4; (Konqueror/3.0-rc4; i686 Linux;;datecode)",
		"Mozilla/5.0 (compatible; Konqueror/3.3; Linux 2.6.8-gentoo-r3; X11;",
		"Mozilla/5.0 (X11; U; Linux; i686; en-US; rv:1.6) Gecko Debian/1.6-7",
		"MSIE (MSIE 6.0; X11; Linux; i686) Opera 7.23",
		"ELinks/0.9.3 (textmode; Linux 2.6.9-kanotix-8 i686; 127x41)",
		"ELinks (0.4pre5; Linux 2.6.10-ac7 i686; 80x33)",
		"Links (2.1pre15; Linux 2.4.26 i686; 158x61)",
		"Links/0.9.1 (Linux 2.4.24; i386;)",
		"Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/0.8.12",
		"Links (2.1pre15; FreeBSD 5.3-RELEASE i386; 196x84)",
		"Mozilla/5.0 (X11; U; FreeBSD; i386; en-US; rv:1.7) Gecko",
		"Mozilla/4.77 [en] (X11; I; IRIX;64 6.5 IP30)",
		"Mozilla/4.8 [en] (X11; U; SunOS; 5.7 sun4u)",
		"Mozilla/3.0 (compatible; NetPositive/2.1.1; BeOS)" };
}
