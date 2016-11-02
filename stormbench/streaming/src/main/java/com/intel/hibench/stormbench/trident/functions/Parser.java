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
package com.intel.hibench.stormbench.trident.functions;

import com.google.common.collect.ImmutableMap;
import com.intel.hibench.common.streaming.UserVisit;
import com.intel.hibench.common.streaming.UserVisitParser;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class Parser extends BaseFunction {

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector) {
    ImmutableMap<String, String> kv = (ImmutableMap<String, String>) tuple.getValue(0);
    String key = kv.keySet().iterator().next();
    Long startTime = Long.parseLong(key);
    UserVisit uv = UserVisitParser.parse(kv.get(key));
    collector.emit(new Values(uv.getIp(), startTime));
  }
}
