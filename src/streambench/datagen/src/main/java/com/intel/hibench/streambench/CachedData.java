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

package com.intel.hibench.streambench;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Cache the total records in memory.
 */
public class CachedData {
    private volatile static CachedData cachedData;

    private String sourcePath;
    private Configuration dfsConf;
    private long startOffset;
    private int index;
    private int totalRecords;
    private List<String> data;

    public static CachedData getInstance(String sourcePath,
                                         Configuration dfsConf,
                                         long startOffset,
                                         int totalRecords) {
        if(cachedData == null) {
            synchronized (CachedData.class) {
                if (cachedData == null) {
                    cachedData = new CachedData(sourcePath, dfsConf, startOffset, totalRecords);
                }
            }
        }
        return cachedData;
    }

    private CachedData(String sourcePath,
                            Configuration dfsConf,
                            long startOffset,
                            int totalRecords){
        this.totalRecords = totalRecords;
        this.sourcePath = sourcePath;
        this.dfsConf = dfsConf;
        this.startOffset = startOffset;
        this.index = 0;
        data = new ArrayList<String>(totalRecords);

        init();
    }

    private void init() {
        BufferedReader reader = SourceFileReader.getReader(dfsConf, sourcePath, startOffset);
        int sentRecords = 0;

        try {
            while (sentRecords < totalRecords) {
                String line = reader.readLine();
                if (line == null) {
                    break; // no more data from source files
                }
                data.add(line);
                sentRecords ++;
            }
        } catch (IOException e) {
            System.err.println("Failed read records from Path: " + sourcePath);
            e.printStackTrace();
        }
    }

    public String getRecord() {
        index = (index + 1) % totalRecords;
        return data.get(index);
    }
}
