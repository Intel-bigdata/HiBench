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

import java.io.Serializable;

/**
 * I only add three fields here, we can add more later if we need them.
 */
public class UserVisit implements Serializable{
  String ip;
  String sessionId;  // TODO: Not sure about the meaning of this field
  String browser;


  public UserVisit(String ip, String sessionId, String browser) {
    this.browser = browser;
    this.ip = ip;
    this.sessionId = sessionId;
  }

  public String getBrowser() {
    return browser;
  }

  public String getIp() {
    return ip;
  }

  public String getSessionId() {
    return sessionId;
  }
}
