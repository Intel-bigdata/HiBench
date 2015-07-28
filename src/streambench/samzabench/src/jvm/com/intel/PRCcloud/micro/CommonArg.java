package com.intel.PRCcloud.micro;

class CommonArg {
  static private String separator = "\\s+"; 
  static private int fieldIndex = 1;
  static private String pattern = "the";
  static private double prob = 0.1;
  static public String getSeparator() {
    return separator;
  }
  static public int getFieldIndex() {
    return fieldIndex;
  }
  static public String getPattern() {
    return pattern;
  }
  static public double getProb() {
    return prob;
  }
}
