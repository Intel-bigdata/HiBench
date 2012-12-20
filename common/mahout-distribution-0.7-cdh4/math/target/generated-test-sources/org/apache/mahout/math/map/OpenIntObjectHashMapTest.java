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

import org.apache.mahout.math.function.IntObjectProcedure;
import org.apache.mahout.math.function.IntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenIntObjectHashMapTest extends Assert {
  
  private static class TestClass implements Comparable<TestClass>{
    
    TestClass(int x) {
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
      result = prime * result + Integer.valueOf(x).hashCode();
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

    int x;

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
    item = new TestClass((int)101);
    anotherItem = new TestClass((int)99);
    anotherItem2 = new TestClass((int)2);
    anotherItem3 = new TestClass((int)3);
    anotherItem4 = new TestClass((int)4);
    anotherItem5 = new TestClass((int)5);
    
  }

  
  @Test
  public void testConstructors() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenIntObjectHashMap<TestClass>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenIntObjectHashMap<TestClass>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
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
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertSame(null, map.get((int) 11));
  }
  
  @Test
  public void testClone() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    OpenIntObjectHashMap<TestClass> map2 = (OpenIntObjectHashMap<TestClass>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    assertTrue(map.containsKey((int) 11));
    assertFalse(map.containsKey((int) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    assertTrue(map.containsValue(item));
    assertFalse(map.containsValue(anotherItem));
  }
  
  @Test
  public void testForEachKey() {
    final IntArrayList keys = new IntArrayList();
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem);
    map.put((int) 12, anotherItem2);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem4);
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
    TestClass v;
    
    Pair(int k, TestClass v) {
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
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem);
    map.put((int) 12, anotherItem2);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem4);
    map.removeKey((int) 13);
    map.forEachPair(new IntObjectProcedure<TestClass>() {
      
      @Override
      public boolean apply(int first, TestClass second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((int) 11, pairs.get(0).k );
    assertSame(anotherItem, pairs.get(0).v );
    assertEquals((int) 12, pairs.get(1).k );
    assertSame(anotherItem2, pairs.get(1).v );
    assertEquals((int) 14, pairs.get(2).k );
    assertSame(anotherItem4, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new IntObjectProcedure<TestClass>() {
      int count = 0;
      
      @Override
      public boolean apply(int first, TestClass second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    map.put((int) 12, anotherItem);
    assertSame(item, map.get((int)11) );
    assertSame(null, map.get((int)0) );
  }

  @Test
  public void testKeys() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    map.put((int) 12, item);
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
    List<TestClass> valueList = new ArrayList<TestClass>();
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem2);
    map.put((int) 12, anotherItem3);
    map.put((int) 13, anotherItem4);
    map.put((int) 14, anotherItem5);
    map.removeKey((int) 13);
    map.pairsMatching(new IntObjectProcedure<TestClass>() {

      @Override
      public boolean apply(int first, TestClass second) {
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
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem);
    map.put((int) 12, anotherItem2);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem4);
    map.removeKey((int) 13);
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
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, item);
    OpenIntObjectHashMap<TestClass> map2 = (OpenIntObjectHashMap<TestClass>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem);
    map.put((int) 12, anotherItem2);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem4);
    map.removeKey((int) 13);
    OpenIntObjectHashMap<TestClass> map2 = (OpenIntObjectHashMap<TestClass>) map.copy();
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
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem5);
    map.put((int) 12, anotherItem4);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem2);
    map.removeKey((int) 13);
    IntArrayList keys = new IntArrayList();
    map.keysSortedByValue(keys);
    int[] keysArray = keys.toArray(new int[keys.size()]);
    assertArrayEquals(new int[] {14, 12, 11},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem5);
    map.put((int) 12, anotherItem4);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem2);
    
    IntArrayList keys = new IntArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((int) 11, keys.get(0) );
    assertSame(anotherItem5, values.get(0) );
    assertEquals((int) 12, keys.get(1) );
    assertSame(anotherItem4, values.get(1) );
    assertEquals((int) 13, keys.get(2) );
    assertSame(anotherItem3, values.get(2) );
    assertEquals((int) 14, keys.get(3) );
    assertSame(anotherItem2, values.get(3) );
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenIntObjectHashMap<TestClass> map = new OpenIntObjectHashMap<TestClass>();
    map.put((int) 11, anotherItem5);
    map.put((int) 12, anotherItem4);
    map.put((int) 13, anotherItem3);
    map.put((int) 14, anotherItem2);
    
    IntArrayList keys = new IntArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByValue(keys, values);
    assertEquals((int) 11, keys.get(3) );
    assertEquals(anotherItem5, values.get(3) );
    assertEquals((int) 12, keys.get(2) );
    assertEquals(anotherItem4, values.get(2) );
    assertEquals((int) 13, keys.get(1) );
    assertEquals(anotherItem3, values.get(1) );
    assertEquals((int) 14, keys.get(0) );
    assertEquals(anotherItem2, values.get(0) );
  }
 
 }
