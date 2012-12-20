/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 
  
 package org.apache.mahout.math.map;
 
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.mahout.math.function.CharCharProcedure;
import org.apache.mahout.math.function.CharProcedure;
import org.apache.mahout.math.list.CharArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Test;

public class OpenCharCharHashMapTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenCharCharHashMap(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenCharCharHashMap(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    int prime = PrimeFinder.nextPrime(907);
    
    map.ensureCapacity(prime);
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
  }
  
  @Test
  public void testClear() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertEquals(0, map.get((char) 11), 0.0000001);
  }
  
  @Test
  public void testClone() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    OpenCharCharHashMap map2 = (OpenCharCharHashMap) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    assertTrue(map.containsKey((char) 11));
    assertFalse(map.containsKey((char) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    assertTrue(map.containsValue((char) 22));
    assertFalse(map.containsValue((char) 23));
  }
  
  @Test
  public void testForEachKey() {
    final CharArrayList keys = new CharArrayList();
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    map.forEachKey(new CharProcedure() {
      
      @Override
      public boolean apply(char element) {
        keys.add(element);
        return true;
      }
    });
    
    char[] keysArray = keys.toArray(new char[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new char[] {11, 12, 14}, keysArray );
  }
  
  private static class Pair implements Comparable<Pair> {
    char k;
    char v;
    
    Pair(char k, char v) {
      this.k = k;
      this.v = v;
    }
    
    @Override
    public int compareTo(Pair o) {
      if (k < o.k) {
        return -1;
      } else if (k == o.k) {
        return 0;
      } else {
        return 1;
      }
    }
  }
  
  @Test
  public void testForEachPair() {
    final List<Pair> pairs = new ArrayList<Pair>();
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    map.forEachPair(new CharCharProcedure() {
      
      @Override
      public boolean apply(char first, char second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((char) 11, pairs.get(0).k );
    assertEquals((char) 22, pairs.get(0).v );
    assertEquals((char) 12, pairs.get(1).k );
    assertEquals((char) 23, pairs.get(1).v );
    assertEquals((char) 14, pairs.get(2).k );
    assertEquals((char) 25, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new CharCharProcedure() {
      int count = 0;
      
      @Override
      public boolean apply(char first, char second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    assertEquals(22, map.get((char)11) );
    assertEquals(0, map.get((char)0) );
  }
  
  @Test
  public void testAdjustOrPutValue() {
   OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.adjustOrPutValue((char)11, (char)1, (char)3);
    assertEquals(25, map.get((char)11) );
    map.adjustOrPutValue((char)15, (char)1, (char)3);
    assertEquals(1, map.get((char)15) );
  }
  
  @Test
  public void testKeys() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 22);
    CharArrayList keys = new CharArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) );
    assertEquals(12, keys.get(1) );
    CharArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    CharArrayList keyList = new CharArrayList();
    CharArrayList valueList = new CharArrayList();
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    map.pairsMatching(new CharCharProcedure() {

      @Override
      public boolean apply(char first, char second) {
        return (first % 2) == 0;
      }},
        keyList, valueList);
    keyList.sort();
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertEquals(12, keyList.get(0) );
    assertEquals(14, keyList.get(1) );
    assertEquals(23, valueList.get(0) );
    assertEquals(25, valueList.get(1) );
  }
  
  @Test
  public void testValues() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    CharArrayList values = new CharArrayList(100);
    map.values(values);
    assertEquals(3, values.size());
    values.sort();
    assertEquals(22, values.get(0) );
    assertEquals(23, values.get(1) );
    assertEquals(25, values.get(2) );
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    OpenCharCharHashMap map2 = (OpenCharCharHashMap) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    OpenCharCharHashMap map2 = (OpenCharCharHashMap) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((char) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 22);
    map.put((char) 12, (char) 23);
    map.put((char) 13, (char) 24);
    map.put((char) 14, (char) 25);
    map.removeKey((char) 13);
    CharArrayList keys = new CharArrayList();
    map.keysSortedByValue(keys);
    char[] keysArray = keys.toArray(new char[keys.size()]);
    assertArrayEquals(new char[] {11, 12, 14},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenCharCharHashMap map = new OpenCharCharHashMap();
    map.put((char) 11, (char) 100);
    map.put((char) 12, (char) 70);
    map.put((char) 13, (char) 30);
    map.put((char) 14, (char) 3);
    
    CharArrayList keys = new CharArrayList();
    CharArrayList values = new CharArrayList();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((char) 11, keys.get(0) );
    assertEquals((char) 100, values.get(0) );
    assertEquals((char) 12, keys.get(1) );
    assertEquals((char) 70, values.get(1) );
    assertEquals((char) 13, keys.get(2) );
    assertEquals((char) 30, values.get(2) );
    assertEquals((char) 14, keys.get(3) );
    assertEquals((char) 3, values.get(3) );
    keys.clear();
    values.clear();
    map.pairsSortedByValue(keys, values);
    assertEquals((char) 11, keys.get(3) );
    assertEquals((char) 100, values.get(3) );
    assertEquals((char) 12, keys.get(2) );
    assertEquals((char) 70, values.get(2) );
    assertEquals((char) 13, keys.get(1) );
    assertEquals((char) 30, values.get(1) );
    assertEquals((char) 14, keys.get(0) );
    assertEquals((char) 3, values.get(0) );
  }
 
 }
