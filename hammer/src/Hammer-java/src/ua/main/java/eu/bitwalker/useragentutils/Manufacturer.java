/*
* Copyright (c) 2008-2013, Harald Walker (bitwalker.eu)  
* All rights reserved.
* 
* Redistribution and use in source and binary forms, with or
* without modification, are permitted provided that the
* following conditions are met:
* 
* * Redistributions of source code must retain the above
* copyright notice, this list of conditions and the following
* disclaimer.
* 
* * Redistributions in binary form must reproduce the above
* copyright notice, this list of conditions and the following
* disclaimer in the documentation and/or other materials
* provided with the distribution.
* 
* * Neither the name of bitwalker nor the names of its
* contributors may be used to endorse or promote products
* derived from this software without specific prior written
* permission.
* 
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
* CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
* INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
* MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
* CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
* NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
* LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
* HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
* CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
* OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
* SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package ua.main.java.eu.bitwalker.useragentutils;

/**
 * Enum constants representing manufacturers of operating systems and client software. 
 * Manufacturer could be used for specific handling of browser requests.
 * @author harald
 */

public enum Manufacturer {
	
	/**
	 * Unknow or rare manufacturer
	 */
	OTHER(1, "Other"),
	/**
	 * Microsoft Corporation
	 */
	MICROSOFT(2, "Microsoft Corporation"),
	/**
	 * Apple Inc.
	 */
	APPLE(3, "Apple Inc."),
	/**
	 * Sun Microsystems, Inc.
	 */
	SUN(4, "Sun Microsystems, Inc."),
	/**
	 * Symbian Ltd.
	 */
	SYMBIAN(5, "Symbian Ltd."),
	/**
	 * Nokia Corporation
	 */
	NOKIA(6, "Nokia Corporation"),
	/**
	 * Research In Motion Limited
	 */
	BLACKBERRY(7, "Research In Motion Limited"),	
	/**
	 * Hewlett-Packard Company, previously Palm
	 */
	HP(8, "Hewlet Packard"),
	/**
	 * Sony Ericsson Mobile Communications AB
	 */
	SONY_ERICSSON(9, "Sony Ericsson Mobile Communications AB"),
	/**
	 * Samsung Electronics
	 */
	SAMSUNG(20, "Samsung Electronics"),
	/**
	 * Sony Computer Entertainment, Inc.
	 */
	SONY(10, "Sony Computer Entertainment, Inc."),
	/**
	 * Nintendo
	 */
	NINTENDO(11, "Nintendo"),
	/**
	 * Opera Software ASA
	 */
	OPERA(12, "Opera Software ASA"),
	/**
	 * Mozilla Foundation
	 */
	MOZILLA(13, "Mozilla Foundation"),
	/**
	 * Google Inc.
	 */
	GOOGLE(15, "Google Inc."),
	/**
	 * CompuServe Interactive Services, Inc. 
	 */
	COMPUSERVE(16, "CompuServe Interactive Services, Inc."),
	/**
	 * Yahoo Inc.
	 */
	YAHOO(17, "Yahoo Inc."),
	/**
	 * AOL LLC.
	 */
	AOL(18, "AOL LLC."),
	/**
	 * Mail.com Media Corporation
	 */
	MMC(19, "Mail.com Media Corporation"),
	/**
	 * Amazon.com, Inc.
	 */
	AMAZON(20, "Amazon.com, Inc."),
	/**
	 * Roku sells home digital media products
	 */
	ROKU(21, "Roku, Inc."),
	/**
	 * Adobe Systems Inc.
	 */
	ADOBE(23, "Adobe Systems Inc.");
	
	
	private final byte id;
	private final String name;
	
	private Manufacturer(int id, String name) {
		this.id = (byte) id;
		this.name = name;
	}

	/**
	 * @return the id
	 */
	public byte getId() {
		return id;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

}
