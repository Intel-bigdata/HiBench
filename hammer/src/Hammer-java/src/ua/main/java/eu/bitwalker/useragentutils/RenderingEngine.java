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
 * Enum constants classifying the different types of rendering engines which are being used by browsers.
 * @author harald
 *
 */
public enum RenderingEngine {
	
	/**
	 * Trident is the the Microsoft layout engine, mainly used by Internet Explorer.
	 */
	TRIDENT("Trident"),
	/**
	 * HTML parsing and rendering engine of Microsoft Office Word, used by some other products of the Office suite instead of Trident. 
	 */
	WORD("Microsoft Office Word"),
	/**
	 * Open source and cross platform layout engine, used by Firefox and many other browsers.
	 */
	GECKO("Gecko"),
	/**
	 * Layout engine based on KHTML, used by Safari, Chrome and some other browsers.
	 */
	WEBKIT("WebKit"),
	/**
	 * Proprietary layout engine by Opera Software ASA
	 */
	PRESTO("Presto"),
	/**
	 * Original layout engine of the Mozilla browser and related products. Predecessor of Gecko.
	 */
	MOZILLA("Mozilla"),
	/**
	 * Layout engine of the KDE project
	 */
	KHTML("KHTML"),
	/**
	 * Other or unknown layout engine.
	 */
	OTHER("Other");
	
	String name;
	
	private RenderingEngine(String name) {
		this.name = name;
	}

}
