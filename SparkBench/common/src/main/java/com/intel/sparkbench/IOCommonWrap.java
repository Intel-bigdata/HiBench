package com.intel.sparkbench;

/*
 * A wrapper for py4j to call
 */
public class IOCommonWrap {
    public static String getProperty(String key){
        return IOCommon$.MODULE$.getProperty(key).getOrElse(null);
    }
}
