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

import org.apache.mahout.math.function.DoubleIntProcedure;
import org.apache.mahout.math.function.DoubleProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Test;

public class OpenDoubleIntHashMapTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenDoubleIntHashMap(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenDoubleIntHashMap(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
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
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertEquals(0, map.get((double) 11), 0.0000001);
  }
  
  @Test
  public void testClone() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    OpenDoubleIntHashMap map2 = (OpenDoubleIntHashMap) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    assertTrue(map.containsKey((double) 11));
    assertFalse(map.containsKey((double) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    assertTrue(map.containsValue((int) 22));
    assertFalse(map.containsValue((int) 23));
  }
  
  @Test
  public void testForEachKey() {
    final DoubleArrayList keys = new DoubleArrayList();
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    map.forEachKey(new DoubleProcedure() {
      
      @Override
      public boolean apply(double element) {
        keys.add(element);
        return true;
      }
    });
    
    double[] keysArray = keys.toArray(new double[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new double[] {11, 12, 14}, keysArray , (double)0.000001);
  }
  
  private static class Pair implements Comparable<Pair> {
    double k;
    int v;
    
    Pair(double k, int v) {
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
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    map.forEachPair(new DoubleIntProcedure() {
      
      @Override
      public boolean apply(double first, int second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((double) 11, pairs.get(0).k , (double)0.000001);
    assertEquals((int) 22, pairs.get(0).v );
    assertEquals((double) 12, pairs.get(1).k , (double)0.000001);
    assertEquals((int) 23, pairs.get(1).v );
    assertEquals((double) 14, pairs.get(2).k , (double)0.000001);
    assertEquals((int) 25, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new DoubleIntProcedure() {
      int count = 0;
      
      @Override
      public boolean apply(double first, int second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    assertEquals(22, map.get((double)11) );
    assertEquals(0, map.get((double)0) );
  }
  
  @Test
  public void testAdjustOrPutValue() {
   OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.adjustOrPutValue((double)11, (int)1, (int)3);
    assertEquals(25, map.get((double)11) );
    map.adjustOrPutValue((double)15, (int)1, (int)3);
    assertEquals(1, map.get((double)15) );
  }
  
  @Test
  public void testKeys() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 22);
    DoubleArrayList keys = new DoubleArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) , (double)0.000001);
    assertEquals(12, keys.get(1) , (double)0.000001);
    DoubleArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    DoubleArrayList keyList = new DoubleArrayList();
    IntArrayList valueList = new IntArrayList();
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    map.pairsMatching(new DoubleIntProcedure() {

      @Override
      public boolean apply(double first, int second) {
        return (first % 2) == 0;
      }},
        keyList, valueList);
    keyList.sort();
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertEquals(12, keyList.get(0) , (double)0.000001);
    assertEquals(14, keyList.get(1) , (double)0.000001);
    assertEquals(23, valueList.get(0) );
    assertEquals(25, valueList.get(1) );
  }
  
  @Test
  public void testValues() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    IntArrayList values = new IntArrayList(100);
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
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    OpenDoubleIntHashMap map2 = (OpenDoubleIntHashMap) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    OpenDoubleIntHashMap map2 = (OpenDoubleIntHashMap) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((double) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 22);
    map.put((double) 12, (int) 23);
    map.put((double) 13, (int) 24);
    map.put((double) 14, (int) 25);
    map.removeKey((double) 13);
    DoubleArrayList keys = new DoubleArrayList();
    map.keysSortedByValue(keys);
    double[] keysArray = keys.toArray(new double[keys.size()]);
    assertArrayEquals(new double[] {11, 12, 14},
        keysArray , (double)0.000001);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenDoubleIntHashMap map = new OpenDoubleIntHashMap();
    map.put((double) 11, (int) 100);
    map.put((double) 12, (int) 70);
    map.put((double) 13, (int) 30);
    map.put((double) 14, (int) 3);
    
    DoubleArrayList keys = new DoubleArrayList();
    IntArrayList values = new IntArrayList();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((double) 11, keys.get(0) , (double)0.000001);
    assertEquals((int) 100, values.get(0) );
    assertEquals((double) 12, keys.get(1) , (double)0.000001);
    assertEquals((int) 70, values.get(1) );
    assertEquals((double) 13, keys.get(2) , (double)0.000001);
    assertEquals((int) 30, values.get(2) );
    assertEquals((double) 14, keys.get(3) , (double)0.000001);
    assertEquals((int) 3, values.get(3) );
    keys.clear();
    values.clear();
    map.pairsSortedByValue(keys, values);
    assertEquals((double) 11, keys.get(3) , (double)0.000001);
    assertEquals((int) 100, values.get(3) );
    assertEquals((double) 12, keys.get(2) , (double)0.000001);
    assertEquals((int) 70, values.get(2) );
    assertEquals((double) 13, keys.get(1) , (double)0.000001);
    assertEquals((int) 30, values.get(1) );
    assertEquals((double) 14, keys.get(0) , (double)0.000001);
    assertEquals((int) 3, values.get(0) );
  }
 
 }
