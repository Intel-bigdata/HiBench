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

import org.apache.mahout.math.function.IntCharProcedure;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.list.CharArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Test;

public class OpenIntCharHashMapTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenIntCharHashMap(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenIntCharHashMap(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
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
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertEquals(0, map.get((int) 11), 0.0000001);
  }
  
  @Test
  public void testClone() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    OpenIntCharHashMap map2 = (OpenIntCharHashMap) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    assertTrue(map.containsKey((int) 11));
    assertFalse(map.containsKey((int) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    assertTrue(map.containsValue((char) 22));
    assertFalse(map.containsValue((char) 23));
  }
  
  @Test
  public void testForEachKey() {
    final IntArrayList keys = new IntArrayList();
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
    map.forEachKey(new IntProcedure() {
      
      @Override
      public boolean apply(int element) {
        keys.add(element);
        return true;
      }
    });
    
    int[] keysArray = keys.toArray(new int[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new int[] {11, 12, 14}, keysArray );
  }
  
  private static class Pair implements Comparable<Pair> {
    int k;
    char v;
    
    Pair(int k, char v) {
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
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
    map.forEachPair(new IntCharProcedure() {
      
      @Override
      public boolean apply(int first, char second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((int) 11, pairs.get(0).k );
    assertEquals((char) 22, pairs.get(0).v );
    assertEquals((int) 12, pairs.get(1).k );
    assertEquals((char) 23, pairs.get(1).v );
    assertEquals((int) 14, pairs.get(2).k );
    assertEquals((char) 25, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new IntCharProcedure() {
      int count = 0;
      
      @Override
      public boolean apply(int first, char second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    assertEquals(22, map.get((int)11) );
    assertEquals(0, map.get((int)0) );
  }
  
  @Test
  public void testAdjustOrPutValue() {
   OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.adjustOrPutValue((int)11, (char)1, (char)3);
    assertEquals(25, map.get((int)11) );
    map.adjustOrPutValue((int)15, (char)1, (char)3);
    assertEquals(1, map.get((int)15) );
  }
  
  @Test
  public void testKeys() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 22);
    IntArrayList keys = new IntArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) );
    assertEquals(12, keys.get(1) );
    IntArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    IntArrayList keyList = new IntArrayList();
    CharArrayList valueList = new CharArrayList();
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
    map.pairsMatching(new IntCharProcedure() {

      @Override
      public boolean apply(int first, char second) {
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
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
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
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    OpenIntCharHashMap map2 = (OpenIntCharHashMap) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
    OpenIntCharHashMap map2 = (OpenIntCharHashMap) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((int) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 22);
    map.put((int) 12, (char) 23);
    map.put((int) 13, (char) 24);
    map.put((int) 14, (char) 25);
    map.removeKey((int) 13);
    IntArrayList keys = new IntArrayList();
    map.keysSortedByValue(keys);
    int[] keysArray = keys.toArray(new int[keys.size()]);
    assertArrayEquals(new int[] {11, 12, 14},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenIntCharHashMap map = new OpenIntCharHashMap();
    map.put((int) 11, (char) 100);
    map.put((int) 12, (char) 70);
    map.put((int) 13, (char) 30);
    map.put((int) 14, (char) 3);
    
    IntArrayList keys = new IntArrayList();
    CharArrayList values = new CharArrayList();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((int) 11, keys.get(0) );
    assertEquals((char) 100, values.get(0) );
    assertEquals((int) 12, keys.get(1) );
    assertEquals((char) 70, values.get(1) );
    assertEquals((int) 13, keys.get(2) );
    assertEquals((char) 30, values.get(2) );
    assertEquals((int) 14, keys.get(3) );
    assertEquals((char) 3, values.get(3) );
    keys.clear();
    values.clear();
    map.pairsSortedByValue(keys, values);
    assertEquals((int) 11, keys.get(3) );
    assertEquals((char) 100, values.get(3) );
    assertEquals((int) 12, keys.get(2) );
    assertEquals((char) 70, values.get(2) );
    assertEquals((int) 13, keys.get(1) );
    assertEquals((char) 30, values.get(1) );
    assertEquals((int) 14, keys.get(0) );
    assertEquals((char) 3, values.get(0) );
  }
 
 }
