/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.common.streaming;

public enum TestCase {

  // Do nothing with input events. It's useful to test the native schedule cost
  IDENTITY("identity"),

  // Repartition input events to ensure data shuffle happening
  REPARTITION("repartition"),

  // Wordcount is used to test the performance of state operator
  WORDCOUNT("wordcount"),

  // FixWindow is used to test the performance of window operator
  FIXWINDOW("fixwindow"),


  // ====== Following TestCase hasn't been finalized ======
  PROJECT("project"),

  SAMPLE("sample"),

  GREP("grep"),

  DISTINCTCOUNT("distinctCount"),

  STATISTICS("statistics");
  // =========================================================

  private String name;

  TestCase(String name) {
    this.name = name;
  }

  // Convert input name to uppercase and return related value of TestCase type
  public static TestCase withValue(String name) {return TestCase.valueOf(name.toUpperCase()); }
}
