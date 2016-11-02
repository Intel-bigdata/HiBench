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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ConfigLoader {
    private String ConfigFileName = null;
    private Map store;

    public ConfigLoader(String filename){
        ConfigFileName = filename;
        store = new HashMap();
        // Load and parse config
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
            String line = br.readLine();
            while(line != null){
                if ((line.length()>0) && (line.charAt(0)!='#')) {
                    String[] words = line.split("\\s+");
                    if (words.length == 2) {
                        String key = words[0];
                        String value = words[1];
                        store.put(key, value);
                    } else if (words.length == 1) {
                        String key = words[0];
                        store.put(key, "");
                    } else {
                        if (!line.startsWith("hibench"))
                            System.out.println("Warning: unknown config parsed, skip:" + line);
                    }
                }
                line = br.readLine();
            }
        } catch (FileNotFoundException e) {
            System.out.println("ERROR: Config file not found! Should not happen. Caused by:");
        } catch (IOException e) {
            System.out.println("ERROR: IO exception during read file. Should not happen. Caused by:");
            e.printStackTrace();
        }
    }

    public String getProperty(String key){
        if (store.containsKey(key))
            return (String) store.get(key);
        else {
            System.out.println("ERROR: Unknown config key:" + key);
            return null;
        }
    }
}
