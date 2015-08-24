package com.intel.hibench.streambench.spark;


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
                    } else {
                        System.out.println("Warning: unknown config parsed, skip:"+line);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            System.out.println("ERROR: Config file not found! Should not happen. Caused by:");
        } catch (IOException e) {
            System.out.println("ERROR: IO exception during read file. Should not happen. Caused by:");
            e.printStackTrace();
        }
    }

    public String getPropertiy(String key){
        if (store.containsKey(key))
            return (String) store.get(key);
        else {
            System.out.println("ERROR: Unknown config key:" + key);
            return null;
        }
    }
}
