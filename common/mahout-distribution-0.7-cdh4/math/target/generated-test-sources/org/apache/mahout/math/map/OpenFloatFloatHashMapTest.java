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

import org.apache.mahout.math.function.FloatFloatProcedure;
import org.apache.mahout.math.function.FloatProcedure;
import org.apache.mahout.math.list.FloatArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Test;

public class OpenFloatFloatHashMapTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenFloatFloatHashMap(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenFloatFloatHashMap(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
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
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertEquals(0, map.get((float) 11), 0.0000001);
  }
  
  @Test
  public void testClone() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    OpenFloatFloatHashMap map2 = (OpenFloatFloatHashMap) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    assertTrue(map.containsKey((float) 11));
    assertFalse(map.containsKey((float) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    assertTrue(map.containsValue((float) 22));
    assertFalse(map.containsValue((float) 23));
  }
  
  @Test
  public void testForEachKey() {
    final FloatArrayList keys = new FloatArrayList();
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    map.forEachKey(new FloatProcedure() {
      
      @Override
      public boolean apply(float element) {
        keys.add(element);
        return true;
      }
    });
    
    float[] keysArray = keys.toArray(new float[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new float[] {11, 12, 14}, keysArray , (float)0.000001);
  }
  
  private static class Pair implements Comparable<Pair> {
    float k;
    float v;
    
    Pair(float k, float v) {
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
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    map.forEachPair(new FloatFloatProcedure() {
      
      @Override
      public boolean apply(float first, float second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((float) 11, pairs.get(0).k , (float)0.000001);
    assertEquals((float) 22, pairs.get(0).v , (float)0.000001);
    assertEquals((float) 12, pairs.get(1).k , (float)0.000001);
    assertEquals((float) 23, pairs.get(1).v , (float)0.000001);
    assertEquals((float) 14, pairs.get(2).k , (float)0.000001);
    assertEquals((float) 25, pairs.get(2).v , (float)0.000001);
    
    pairs.clear();
    map.forEachPair(new FloatFloatProcedure() {
      int count = 0;
      
      @Override
      public boolean apply(float first, float second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    assertEquals(22, map.get((float)11) , (float)0.000001);
    assertEquals(0, map.get((float)0) , (float)0.000001);
  }
  
  @Test
  public void testAdjustOrPutValue() {
   OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.adjustOrPutValue((float)11, (float)1, (float)3);
    assertEquals(25, map.get((float)11) , (float)0.000001);
    map.adjustOrPutValue((float)15, (float)1, (float)3);
    assertEquals(1, map.get((float)15) , (float)0.000001);
  }
  
  @Test
  public void testKeys() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 22);
    FloatArrayList keys = new FloatArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) , (float)0.000001);
    assertEquals(12, keys.get(1) , (float)0.000001);
    FloatArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    FloatArrayList keyList = new FloatArrayList();
    FloatArrayList valueList = new FloatArrayList();
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    map.pairsMatching(new FloatFloatProcedure() {

      @Override
      public boolean apply(float first, float second) {
        return (first % 2) == 0;
      }},
        keyList, valueList);
    keyList.sort();
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertEquals(12, keyList.get(0) , (float)0.000001);
    assertEquals(14, keyList.get(1) , (float)0.000001);
    assertEquals(23, valueList.get(0) , (float)0.000001);
    assertEquals(25, valueList.get(1) , (float)0.000001);
  }
  
  @Test
  public void testValues() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    FloatArrayList values = new FloatArrayList(100);
    map.values(values);
    assertEquals(3, values.size());
    values.sort();
    assertEquals(22, values.get(0) , (float)0.000001);
    assertEquals(23, values.get(1) , (float)0.000001);
    assertEquals(25, values.get(2) , (float)0.000001);
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    OpenFloatFloatHashMap map2 = (OpenFloatFloatHashMap) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    OpenFloatFloatHashMap map2 = (OpenFloatFloatHashMap) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((float) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 22);
    map.put((float) 12, (float) 23);
    map.put((float) 13, (float) 24);
    map.put((float) 14, (float) 25);
    map.removeKey((float) 13);
    FloatArrayList keys = new FloatArrayList();
    map.keysSortedByValue(keys);
    float[] keysArray = keys.toArray(new float[keys.size()]);
    assertArrayEquals(new float[] {11, 12, 14},
        keysArray , (float)0.000001);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenFloatFloatHashMap map = new OpenFloatFloatHashMap();
    map.put((float) 11, (float) 100);
    map.put((float) 12, (float) 70);
    map.put((float) 13, (float) 30);
    map.put((float) 14, (float) 3);
    
    FloatArrayList keys = new FloatArrayList();
    FloatArrayList values = new FloatArrayList();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((float) 11, keys.get(0) , (float)0.000001);
    assertEquals((float) 100, values.get(0) , (float)0.000001);
    assertEquals((float) 12, keys.get(1) , (float)0.000001);
    assertEquals((float) 70, values.get(1) , (float)0.000001);
    assertEquals((float) 13, keys.get(2) , (float)0.000001);
    assertEquals((float) 30, values.get(2) , (float)0.000001);
    assertEquals((float) 14, keys.get(3) , (float)0.000001);
    assertEquals((float) 3, values.get(3) , (float)0.000001);
    keys.clear();
    values.clear();
    map.pairsSortedByValue(keys, values);
    assertEquals((float) 11, keys.get(3) , (float)0.000001);
    assertEquals((float) 100, values.get(3) , (float)0.000001);
    assertEquals((float) 12, keys.get(2) , (float)0.000001);
    assertEquals((float) 70, values.get(2) , (float)0.000001);
    assertEquals((float) 13, keys.get(1) , (float)0.000001);
    assertEquals((float) 30, values.get(1) , (float)0.000001);
    assertEquals((float) 14, keys.get(0) , (float)0.000001);
    assertEquals((float) 3, values.get(0) , (float)0.000001);
  }
 
 }
