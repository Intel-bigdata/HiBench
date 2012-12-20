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

import org.apache.mahout.math.function.FloatObjectProcedure;
import org.apache.mahout.math.function.FloatProcedure;
import org.apache.mahout.math.list.FloatArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenFloatObjectHashMapTest extends Assert {
  
  private static class TestClass implements Comparable<TestClass>{
    
    TestClass(float x) {
      this.x = x;
    }
    
    @Override
    public String toString() {
      return "[ts " + x + " ]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Float.valueOf(x).hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      TestClass other = (TestClass) obj;
      return x == other.x;
    }

    float x;

    @Override
    public int compareTo(TestClass o) {

      return Float.compare(x, o.x);
    }
  }
  
  private TestClass item;
  private TestClass anotherItem;
  private TestClass anotherItem2;
  private TestClass anotherItem3;
  private TestClass anotherItem4;
  private TestClass anotherItem5;
  
  @Before
  public void before() {
    item = new TestClass((float)101);
    anotherItem = new TestClass((float)99);
    anotherItem2 = new TestClass((float)2);
    anotherItem3 = new TestClass((float)3);
    anotherItem4 = new TestClass((float)4);
    anotherItem5 = new TestClass((float)5);
    
  }

  
  @Test
  public void testConstructors() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenFloatObjectHashMap<TestClass>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenFloatObjectHashMap<TestClass>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
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
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertSame(null, map.get((float) 11));
  }
  
  @Test
  public void testClone() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    OpenFloatObjectHashMap<TestClass> map2 = (OpenFloatObjectHashMap<TestClass>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    assertTrue(map.containsKey((float) 11));
    assertFalse(map.containsKey((float) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    assertTrue(map.containsValue(item));
    assertFalse(map.containsValue(anotherItem));
  }
  
  @Test
  public void testForEachKey() {
    final FloatArrayList keys = new FloatArrayList();
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem);
    map.put((float) 12, anotherItem2);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem4);
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
    TestClass v;
    
    Pair(float k, TestClass v) {
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
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem);
    map.put((float) 12, anotherItem2);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem4);
    map.removeKey((float) 13);
    map.forEachPair(new FloatObjectProcedure<TestClass>() {
      
      @Override
      public boolean apply(float first, TestClass second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((float) 11, pairs.get(0).k , (float)0.000001);
    assertSame(anotherItem, pairs.get(0).v );
    assertEquals((float) 12, pairs.get(1).k , (float)0.000001);
    assertSame(anotherItem2, pairs.get(1).v );
    assertEquals((float) 14, pairs.get(2).k , (float)0.000001);
    assertSame(anotherItem4, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new FloatObjectProcedure<TestClass>() {
      int count = 0;
      
      @Override
      public boolean apply(float first, TestClass second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    map.put((float) 12, anotherItem);
    assertSame(item, map.get((float)11) );
    assertSame(null, map.get((float)0) );
  }

  @Test
  public void testKeys() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    map.put((float) 12, item);
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
    List<TestClass> valueList = new ArrayList<TestClass>();
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem2);
    map.put((float) 12, anotherItem3);
    map.put((float) 13, anotherItem4);
    map.put((float) 14, anotherItem5);
    map.removeKey((float) 13);
    map.pairsMatching(new FloatObjectProcedure<TestClass>() {

      @Override
      public boolean apply(float first, TestClass second) {
        return (first % 2) == 0;
      }},
        keyList, valueList);
    keyList.sort();
    Collections.sort(valueList);
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertEquals(12, keyList.get(0) , (float)0.000001);
    assertEquals(14, keyList.get(1) , (float)0.000001);
    assertSame(anotherItem3, valueList.get(0) );
    assertSame(anotherItem5, valueList.get(1) );
  }
  
  @Test
  public void testValues() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem);
    map.put((float) 12, anotherItem2);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem4);
    map.removeKey((float) 13);
    List<TestClass> values = new ArrayList<TestClass>(100);
    map.values(values);
    assertEquals(3, values.size());
    Collections.sort(values);
    assertEquals(anotherItem2, values.get(0) );
    assertEquals(anotherItem4, values.get(1) );
    assertEquals(anotherItem, values.get(2) );
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, item);
    OpenFloatObjectHashMap<TestClass> map2 = (OpenFloatObjectHashMap<TestClass>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem);
    map.put((float) 12, anotherItem2);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem4);
    map.removeKey((float) 13);
    OpenFloatObjectHashMap<TestClass> map2 = (OpenFloatObjectHashMap<TestClass>) map.copy();
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
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem5);
    map.put((float) 12, anotherItem4);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem2);
    map.removeKey((float) 13);
    FloatArrayList keys = new FloatArrayList();
    map.keysSortedByValue(keys);
    float[] keysArray = keys.toArray(new float[keys.size()]);
    assertArrayEquals(new float[] {14, 12, 11},
        keysArray , (float)0.000001);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem5);
    map.put((float) 12, anotherItem4);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem2);
    
    FloatArrayList keys = new FloatArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((float) 11, keys.get(0) , (float)0.000001);
    assertSame(anotherItem5, values.get(0) );
    assertEquals((float) 12, keys.get(1) , (float)0.000001);
    assertSame(anotherItem4, values.get(1) );
    assertEquals((float) 13, keys.get(2) , (float)0.000001);
    assertSame(anotherItem3, values.get(2) );
    assertEquals((float) 14, keys.get(3) , (float)0.000001);
    assertSame(anotherItem2, values.get(3) );
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenFloatObjectHashMap<TestClass> map = new OpenFloatObjectHashMap<TestClass>();
    map.put((float) 11, anotherItem5);
    map.put((float) 12, anotherItem4);
    map.put((float) 13, anotherItem3);
    map.put((float) 14, anotherItem2);
    
    FloatArrayList keys = new FloatArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByValue(keys, values);
    assertEquals((float) 11, keys.get(3) , (float)0.000001);
    assertEquals(anotherItem5, values.get(3) );
    assertEquals((float) 12, keys.get(2) , (float)0.000001);
    assertEquals(anotherItem4, values.get(2) );
    assertEquals((float) 13, keys.get(1) , (float)0.000001);
    assertEquals(anotherItem3, values.get(1) );
    assertEquals((float) 14, keys.get(0) , (float)0.000001);
    assertEquals(anotherItem2, values.get(0) );
  }
 
 }
