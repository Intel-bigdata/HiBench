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

package com.intel.hibench.streambench.storm.trident;


import org.apache.storm.tuple.Values;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;

public class Sketch extends BaseFunction {
  private int fieldIndex;
  private String separator;

  public Sketch(int fieldIndex, String separator) {
    this.fieldIndex = fieldIndex;
    this.separator = separator;
  }

  @Override
  public void execute(TridentTuple tuple, TridentCollector collector){
    String record = tuple.getString(0);
    String[] fields = record.split(separator);
    if (fields.length > fieldIndex) 
      collector.emit(new Values(fields[fieldIndex]));
  }
}
