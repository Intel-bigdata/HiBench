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

package com.intel.hibench.flinkbench.util;

import java.io.File;
import java.io.PrintWriter;

public class BenchLogUtil {
  private static PrintWriter out;

  public static void init() throws Exception {
    File file = new File("/tmp/benchlog-flink.txt");
    out = new PrintWriter(file);
  }

  public static void logMsg(String msg) {
    try {
      if (out == null) {
        init();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    out.println(msg);
    out.flush();
    System.out.println(msg);
  }

  public static void close() {
    if (out != null) {
      out.close();
    }
  }

  public static void handleError(String msg) {
    System.err.println(msg);
    System.exit(1);
  }
}
