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

import org.apache.mahout.math.function.CharObjectProcedure;
import org.apache.mahout.math.function.CharProcedure;
import org.apache.mahout.math.list.CharArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenCharObjectHashMapTest extends Assert {
  
  private static class TestClass implements Comparable<TestClass>{
    
    TestClass(char x) {
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
      result = prime * result + Character.valueOf(x).hashCode();
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

    char x;

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
    item = new TestClass((char)101);
    anotherItem = new TestClass((char)99);
    anotherItem2 = new TestClass((char)2);
    anotherItem3 = new TestClass((char)3);
    anotherItem4 = new TestClass((char)4);
    anotherItem5 = new TestClass((char)5);
    
  }

  
  @Test
  public void testConstructors() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenCharObjectHashMap<TestClass>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenCharObjectHashMap<TestClass>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
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
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertSame(null, map.get((char) 11));
  }
  
  @Test
  public void testClone() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    OpenCharObjectHashMap<TestClass> map2 = (OpenCharObjectHashMap<TestClass>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    assertTrue(map.containsKey((char) 11));
    assertFalse(map.containsKey((char) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    assertTrue(map.containsValue(item));
    assertFalse(map.containsValue(anotherItem));
  }
  
  @Test
  public void testForEachKey() {
    final CharArrayList keys = new CharArrayList();
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem);
    map.put((char) 12, anotherItem2);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem4);
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
    TestClass v;
    
    Pair(char k, TestClass v) {
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
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem);
    map.put((char) 12, anotherItem2);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem4);
    map.removeKey((char) 13);
    map.forEachPair(new CharObjectProcedure<TestClass>() {
      
      @Override
      public boolean apply(char first, TestClass second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((char) 11, pairs.get(0).k );
    assertSame(anotherItem, pairs.get(0).v );
    assertEquals((char) 12, pairs.get(1).k );
    assertSame(anotherItem2, pairs.get(1).v );
    assertEquals((char) 14, pairs.get(2).k );
    assertSame(anotherItem4, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new CharObjectProcedure<TestClass>() {
      int count = 0;
      
      @Override
      public boolean apply(char first, TestClass second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    map.put((char) 12, anotherItem);
    assertSame(item, map.get((char)11) );
    assertSame(null, map.get((char)0) );
  }

  @Test
  public void testKeys() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    map.put((char) 12, item);
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
    List<TestClass> valueList = new ArrayList<TestClass>();
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem2);
    map.put((char) 12, anotherItem3);
    map.put((char) 13, anotherItem4);
    map.put((char) 14, anotherItem5);
    map.removeKey((char) 13);
    map.pairsMatching(new CharObjectProcedure<TestClass>() {

      @Override
      public boolean apply(char first, TestClass second) {
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
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem);
    map.put((char) 12, anotherItem2);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem4);
    map.removeKey((char) 13);
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
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, item);
    OpenCharObjectHashMap<TestClass> map2 = (OpenCharObjectHashMap<TestClass>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem);
    map.put((char) 12, anotherItem2);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem4);
    map.removeKey((char) 13);
    OpenCharObjectHashMap<TestClass> map2 = (OpenCharObjectHashMap<TestClass>) map.copy();
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
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem5);
    map.put((char) 12, anotherItem4);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem2);
    map.removeKey((char) 13);
    CharArrayList keys = new CharArrayList();
    map.keysSortedByValue(keys);
    char[] keysArray = keys.toArray(new char[keys.size()]);
    assertArrayEquals(new char[] {14, 12, 11},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem5);
    map.put((char) 12, anotherItem4);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem2);
    
    CharArrayList keys = new CharArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((char) 11, keys.get(0) );
    assertSame(anotherItem5, values.get(0) );
    assertEquals((char) 12, keys.get(1) );
    assertSame(anotherItem4, values.get(1) );
    assertEquals((char) 13, keys.get(2) );
    assertSame(anotherItem3, values.get(2) );
    assertEquals((char) 14, keys.get(3) );
    assertSame(anotherItem2, values.get(3) );
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenCharObjectHashMap<TestClass> map = new OpenCharObjectHashMap<TestClass>();
    map.put((char) 11, anotherItem5);
    map.put((char) 12, anotherItem4);
    map.put((char) 13, anotherItem3);
    map.put((char) 14, anotherItem2);
    
    CharArrayList keys = new CharArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByValue(keys, values);
    assertEquals((char) 11, keys.get(3) );
    assertEquals(anotherItem5, values.get(3) );
    assertEquals((char) 12, keys.get(2) );
    assertEquals(anotherItem4, values.get(2) );
    assertEquals((char) 13, keys.get(1) );
    assertEquals(anotherItem3, values.get(1) );
    assertEquals((char) 14, keys.get(0) );
    assertEquals(anotherItem2, values.get(0) );
  }
 
 }
