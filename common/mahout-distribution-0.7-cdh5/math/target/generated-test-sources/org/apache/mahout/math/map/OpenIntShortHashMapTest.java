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

import org.apache.mahout.math.function.IntShortProcedure;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.list.ShortArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Test;

public class OpenIntShortHashMapTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenIntShortHashMap(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenIntShortHashMap(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
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
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertEquals(0, map.get((int) 11), 0.0000001);
  }
  
  @Test
  public void testClone() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    OpenIntShortHashMap map2 = (OpenIntShortHashMap) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    assertTrue(map.containsKey((int) 11));
    assertFalse(map.containsKey((int) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    assertTrue(map.containsValue((short) 22));
    assertFalse(map.containsValue((short) 23));
  }
  
  @Test
  public void testForEachKey() {
    final IntArrayList keys = new IntArrayList();
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
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
    short v;
    
    Pair(int k, short v) {
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
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.removeKey((int) 13);
    map.forEachPair(new IntShortProcedure() {
      
      @Override
      public boolean apply(int first, short second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((int) 11, pairs.get(0).k );
    assertEquals((short) 22, pairs.get(0).v );
    assertEquals((int) 12, pairs.get(1).k );
    assertEquals((short) 23, pairs.get(1).v );
    assertEquals((int) 14, pairs.get(2).k );
    assertEquals((short) 25, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new IntShortProcedure() {
      int count = 0;
      
      @Override
      public boolean apply(int first, short second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    assertEquals(22, map.get((int)11) );
    assertEquals(0, map.get((int)0) );
  }
  
  @Test
  public void testAdjustOrPutValue() {
   OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.adjustOrPutValue((int)11, (short)1, (short)3);
    assertEquals(25, map.get((int)11) );
    map.adjustOrPutValue((int)15, (short)1, (short)3);
    assertEquals(1, map.get((int)15) );
  }
  
  @Test
  public void testKeys() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 22);
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
    ShortArrayList valueList = new ShortArrayList();
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.removeKey((int) 13);
    map.pairsMatching(new IntShortProcedure() {

      @Override
      public boolean apply(int first, short second) {
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
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.removeKey((int) 13);
    ShortArrayList values = new ShortArrayList(100);
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
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    OpenIntShortHashMap map2 = (OpenIntShortHashMap) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.removeKey((int) 13);
    OpenIntShortHashMap map2 = (OpenIntShortHashMap) map.copy();
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
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 22);
    map.put((int) 12, (short) 23);
    map.put((int) 13, (short) 24);
    map.put((int) 14, (short) 25);
    map.removeKey((int) 13);
    IntArrayList keys = new IntArrayList();
    map.keysSortedByValue(keys);
    int[] keysArray = keys.toArray(new int[keys.size()]);
    assertArrayEquals(new int[] {11, 12, 14},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenIntShortHashMap map = new OpenIntShortHashMap();
    map.put((int) 11, (short) 100);
    map.put((int) 12, (short) 70);
    map.put((int) 13, (short) 30);
    map.put((int) 14, (short) 3);
    
    IntArrayList keys = new IntArrayList();
    ShortArrayList values = new ShortArrayList();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((int) 11, keys.get(0) );
    assertEquals((short) 100, values.get(0) );
    assertEquals((int) 12, keys.get(1) );
    assertEquals((short) 70, values.get(1) );
    assertEquals((int) 13, keys.get(2) );
    assertEquals((short) 30, values.get(2) );
    assertEquals((int) 14, keys.get(3) );
    assertEquals((short) 3, values.get(3) );
    keys.clear();
    values.clear();
    map.pairsSortedByValue(keys, values);
    assertEquals((int) 11, keys.get(3) );
    assertEquals((short) 100, values.get(3) );
    assertEquals((int) 12, keys.get(2) );
    assertEquals((short) 70, values.get(2) );
    assertEquals((int) 13, keys.get(1) );
    assertEquals((short) 30, values.get(1) );
    assertEquals((int) 14, keys.get(0) );
    assertEquals((short) 3, values.get(0) );
  }
 
 }
