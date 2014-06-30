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

import org.apache.mahout.math.function.ByteObjectProcedure;
import org.apache.mahout.math.function.ByteProcedure;
import org.apache.mahout.math.list.ByteArrayList;
import org.apache.mahout.math.set.AbstractSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OpenByteObjectHashMapTest extends Assert {
  
  private static class TestClass implements Comparable<TestClass>{
    
    TestClass(byte x) {
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
      result = prime * result + Byte.valueOf(x).hashCode();
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

    byte x;

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
    item = new TestClass((byte)101);
    anotherItem = new TestClass((byte)99);
    anotherItem2 = new TestClass((byte)2);
    anotherItem3 = new TestClass((byte)3);
    anotherItem4 = new TestClass((byte)4);
    anotherItem5 = new TestClass((byte)5);
    
  }

  
  @Test
  public void testConstructors() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenByteObjectHashMap<TestClass>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenByteObjectHashMap<TestClass>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
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
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
    assertSame(null, map.get((byte) 11));
  }
  
  @Test
  public void testClone() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    OpenByteObjectHashMap<TestClass> map2 = (OpenByteObjectHashMap<TestClass>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    assertTrue(map.containsKey((byte) 11));
    assertFalse(map.containsKey((byte) 12));
  }
  
  @Test
  public void testContainValue() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    assertTrue(map.containsValue(item));
    assertFalse(map.containsValue(anotherItem));
  }
  
  @Test
  public void testForEachKey() {
    final ByteArrayList keys = new ByteArrayList();
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem);
    map.put((byte) 12, anotherItem2);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem4);
    map.removeKey((byte) 13);
    map.forEachKey(new ByteProcedure() {
      
      @Override
      public boolean apply(byte element) {
        keys.add(element);
        return true;
      }
    });
    
    byte[] keysArray = keys.toArray(new byte[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new byte[] {11, 12, 14}, keysArray );
  }
  
  private static class Pair implements Comparable<Pair> {
    byte k;
    TestClass v;
    
    Pair(byte k, TestClass v) {
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
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem);
    map.put((byte) 12, anotherItem2);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem4);
    map.removeKey((byte) 13);
    map.forEachPair(new ByteObjectProcedure<TestClass>() {
      
      @Override
      public boolean apply(byte first, TestClass second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((byte) 11, pairs.get(0).k );
    assertSame(anotherItem, pairs.get(0).v );
    assertEquals((byte) 12, pairs.get(1).k );
    assertSame(anotherItem2, pairs.get(1).v );
    assertEquals((byte) 14, pairs.get(2).k );
    assertSame(anotherItem4, pairs.get(2).v );
    
    pairs.clear();
    map.forEachPair(new ByteObjectProcedure<TestClass>() {
      int count = 0;
      
      @Override
      public boolean apply(byte first, TestClass second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    map.put((byte) 12, anotherItem);
    assertSame(item, map.get((byte)11) );
    assertSame(null, map.get((byte)0) );
  }

  @Test
  public void testKeys() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    map.put((byte) 12, item);
    ByteArrayList keys = new ByteArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) );
    assertEquals(12, keys.get(1) );
    ByteArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  @Test
  public void testPairsMatching() {
    ByteArrayList keyList = new ByteArrayList();
    List<TestClass> valueList = new ArrayList<TestClass>();
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem2);
    map.put((byte) 12, anotherItem3);
    map.put((byte) 13, anotherItem4);
    map.put((byte) 14, anotherItem5);
    map.removeKey((byte) 13);
    map.pairsMatching(new ByteObjectProcedure<TestClass>() {

      @Override
      public boolean apply(byte first, TestClass second) {
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
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem);
    map.put((byte) 12, anotherItem2);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem4);
    map.removeKey((byte) 13);
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
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, item);
    OpenByteObjectHashMap<TestClass> map2 = (OpenByteObjectHashMap<TestClass>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem);
    map.put((byte) 12, anotherItem2);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem4);
    map.removeKey((byte) 13);
    OpenByteObjectHashMap<TestClass> map2 = (OpenByteObjectHashMap<TestClass>) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey((byte) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem5);
    map.put((byte) 12, anotherItem4);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem2);
    map.removeKey((byte) 13);
    ByteArrayList keys = new ByteArrayList();
    map.keysSortedByValue(keys);
    byte[] keysArray = keys.toArray(new byte[keys.size()]);
    assertArrayEquals(new byte[] {14, 12, 11},
        keysArray );
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem5);
    map.put((byte) 12, anotherItem4);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem2);
    
    ByteArrayList keys = new ByteArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((byte) 11, keys.get(0) );
    assertSame(anotherItem5, values.get(0) );
    assertEquals((byte) 12, keys.get(1) );
    assertSame(anotherItem4, values.get(1) );
    assertEquals((byte) 13, keys.get(2) );
    assertSame(anotherItem3, values.get(2) );
    assertEquals((byte) 14, keys.get(3) );
    assertSame(anotherItem2, values.get(3) );
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenByteObjectHashMap<TestClass> map = new OpenByteObjectHashMap<TestClass>();
    map.put((byte) 11, anotherItem5);
    map.put((byte) 12, anotherItem4);
    map.put((byte) 13, anotherItem3);
    map.put((byte) 14, anotherItem2);
    
    ByteArrayList keys = new ByteArrayList();
    List<TestClass> values = new ArrayList<TestClass>();
    map.pairsSortedByValue(keys, values);
    assertEquals((byte) 11, keys.get(3) );
    assertEquals(anotherItem5, values.get(3) );
    assertEquals((byte) 12, keys.get(2) );
    assertEquals(anotherItem4, values.get(2) );
    assertEquals((byte) 13, keys.get(1) );
    assertEquals(anotherItem3, values.get(1) );
    assertEquals((byte) 14, keys.get(0) );
    assertEquals(anotherItem2, values.get(0) );
  }
 
 }
