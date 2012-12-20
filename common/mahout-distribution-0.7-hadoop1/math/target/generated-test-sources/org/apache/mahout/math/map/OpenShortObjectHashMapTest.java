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

import org.apache.mahout.math.function.ShortObjectProcedure;
import org.apache.mahout.math.function.ShortProcedure;
import org.apache.mahout.math.list.ShortArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenShortObjectHashMapTest extends Assert {
  
  private static class TestClass implements Comparable<TestClass>{
    
    TestClass(short x) {
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
      result = prime * result + Short.valueOf(x).hashCode();
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

    short x;

    @Override
    public int compareTo(TestClass o) {

      return (int)(x - o.x);
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
    item = new TestClass((short)101);
    anotherItem = new TestClass((short)99);
    anotherItem2 = new TestClass((short)2);
    anotherItem3 = new TestClass((short)3);
    anotherItem4 = new TestClass((short)4);
    anotherItem5 = new TestClass((short)5);
    
  }

  
  @Test
  public void testConstructors() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenShortObjectHashMap<TestClass>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenShortObjectHashMap<TestClass>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
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
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertSame(null, map.get((short) 11));
  }
  
  @Test
  public void testClone() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    OpenShortObjectHashMap<TestClass> map2 = (OpenShortObjectHashMap<TestClass>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    assertTrue(map.containsKey((short) 11));
    assertFalse(map.containsKey((short) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    assertTrue(map.containsValue(item));
    assertFalse(map.containsValue(anotherItem));
  }
  
  @Test
  public void testForEachKey() {
    final ShortArrayList keys = new ShortArrayList();
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem);
    map.put((short) 12, anotherItem2);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem4);
    map.removeKey((short) 13);
    map.forEachKey(new ShortProcedure() {
      
      @Override
      public boolean apply(short element) {
        keys.add(element);
        return true;
      }
    });
    
    short[] keysArray = keys.toArray(new short[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new short[] {11, 12, 14}, keysArray );
  }
  
  private static class Pair implements Comparable<Pair> {
    short k;
    TestClass v;
    
    Pair(short k, TestClass v) {
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
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem);
    map.put((short) 12, anotherItem2);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem4);
    map.removeKey((short) 13);
    map.forEachPair(new ShortObjectProcedure<TestClass>() {
      
      @Override
      public boolean apply(short first, TestClass second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((short) 11, pairs.get(0).k );
    assertSame(anotherItem, pairs.get(0).v );
    assertEquals((short) 12, pairs.get(1).k );
    assertSame(anotherItem2, pairs.get(1).v );
    assertEquals((short) 14, pairs.get(2).k );
    assertSame(anotherItem4, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new ShortObjectProcedure<TestClass>() {
      int count = 0;
      
      @Override
      public boolean apply(short first, TestClass second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    map.put((short) 12, anotherItem);
    assertSame(item, map.get((short)11) );
    assertSame(null, map.get((short)0) );
  }

  @Test
  public void testKeys() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    map.put((short) 12, item);
    ShortArrayList keys = new ShortArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) );
    assertEquals(12, keys.get(1) );
    ShortArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    ShortArrayList keyList = new ShortArrayList();
    List<TestClass> valueList = new ArrayList<TestClass>();
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem2);
    map.put((short) 12, anotherItem3);
    map.put((short) 13, anotherItem4);
    map.put((short) 14, anotherItem5);
    map.removeKey((short) 13);
    map.pairsMatching(new ShortObjectProcedure<TestClass>() {

      @Override
      public boolean apply(short first, TestClass second) {
        return (first % 2) == 0;
      }},
        keyList, valueList);
    keyList.sort();
    Collections.sort(valueList);
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertEquals(12, keyList.get(0) );
    assertEquals(14, keyList.get(1) );
    assertSame(anotherItem3, valueList.get(0) );
    assertSame(anotherItem5, valueList.get(1) );
  }
  
  @Test
  public void testValues() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem);
    map.put((short) 12, anotherItem2);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem4);
    map.removeKey((short) 13);
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
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, item);
    OpenShortObjectHashMap<TestClass> map2 = (OpenShortObjectHashMap<TestClass>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem);
    map.put((short) 12, anotherItem2);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem4);
    map.removeKey((short) 13);
    OpenShortObjectHashMap<TestClass> map2 = (OpenShortObjectHashMap<TestClass>) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((short) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem5);
    map.put((short) 12, anotherItem4);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem2);
    map.removeKey((short) 13);
    ShortArrayList keys = new ShortArrayList();
    map.keysSortedByValue(keys);
    short[] keysArray = keys.toArray(new short[keys.size()]);
    assertArrayEquals(new short[] {14, 12, 11},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem5);
    map.put((short) 12, anotherItem4);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem2);
    
    ShortArrayList keys = new ShortArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((short) 11, keys.get(0) );
    assertSame(anotherItem5, values.get(0) );
    assertEquals((short) 12, keys.get(1) );
    assertSame(anotherItem4, values.get(1) );
    assertEquals((short) 13, keys.get(2) );
    assertSame(anotherItem3, values.get(2) );
    assertEquals((short) 14, keys.get(3) );
    assertSame(anotherItem2, values.get(3) );
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenShortObjectHashMap<TestClass> map = new OpenShortObjectHashMap<TestClass>();
    map.put((short) 11, anotherItem5);
    map.put((short) 12, anotherItem4);
    map.put((short) 13, anotherItem3);
    map.put((short) 14, anotherItem2);
    
    ShortArrayList keys = new ShortArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByValue(keys, values);
    assertEquals((short) 11, keys.get(3) );
    assertEquals(anotherItem5, values.get(3) );
    assertEquals((short) 12, keys.get(2) );
    assertEquals(anotherItem4, values.get(2) );
    assertEquals((short) 13, keys.get(1) );
    assertEquals(anotherItem3, values.get(1) );
    assertEquals((short) 14, keys.get(0) );
    assertEquals(anotherItem2, values.get(0) );
  }
 
 }
