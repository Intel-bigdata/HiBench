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
import java.util.Collections;
import java.util.List;

import org.apache.mahout.math.function.ObjectDoubleProcedure;
import org.apache.mahout.math.function.ObjectProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.set.AbstractSet;
import org.junit.Assert;
import org.junit.Test;

public class OpenObjectDoubleHashMapTest extends Assert {

    private static class NotComparableKey {
    protected int x;
    
    public NotComparableKey(int x) {
      this.x = x;
    }
      
    @Override
    public String toString() {
      return "[k " + x + " ]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + x;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      NotComparableKey other = (NotComparableKey) obj;
      return x == other.x;
    }
  }
  
  private final NotComparableKey[] ncKeys = {
    new NotComparableKey(101),
    new NotComparableKey(99),
    new NotComparableKey(2),
    new NotComparableKey(3),
    new NotComparableKey(4),
    new NotComparableKey(5)
    };
  
  @Test
  public void testConstructors() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenObjectDoubleHashMap<String>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenObjectDoubleHashMap<String>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
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
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double)11); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testClone() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double)11);
    OpenObjectDoubleHashMap<String> map2 = (OpenObjectDoubleHashMap<String>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double)11);
    assertTrue(map.containsKey("Eleven"));
    assertTrue(map.containsKey(new String("Eleven")));
    assertFalse(map.containsKey("Twelve"));
  }
  
  @Test
  public void testContainValue() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double)11);
    assertTrue(map.containsValue((double)11));
    assertFalse(map.containsValue((double)12));
  }
  
  @Test
  public void testForEachKey() {
    final List<String> keys = new ArrayList<String>();
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    map.forEachKey(new ObjectProcedure<String>() {
      
      @Override
      public boolean apply(String element) {
        keys.add(element);
        return true;
      }
    });
    
    assertEquals(3, keys.size());
    Collections.sort(keys);
    assertSame("Fourteen", keys.get(1));
    assertSame("Twelve", keys.get(2));
    assertSame("Eleven", keys.get(0));
  }
  
  private static class Pair implements Comparable<Pair> {
    double v;
    String k;
    
    Pair(String k, double v) {
      this.k = k;
      this.v = v;
    }
    
    @Override
    public int compareTo(Pair o) {
      return k.compareTo(o.k);
    }
  }
  
  @Test
  public void testForEachPair() {
    final List<Pair> pairs = new ArrayList<Pair>();
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    map.forEachPair(new ObjectDoubleProcedure<String>() {
      
      @Override
      public boolean apply(String first, double second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((double)14, pairs.get(1).v , (double)0.000001);
    assertSame("Fourteen", pairs.get(1).k);
    assertEquals((double) 12, pairs.get(2).v , (double)0.000001);
    assertSame("Twelve", pairs.get(2).k);
    assertEquals((double) 11, pairs.get(0).v , (double)0.000001);
    assertSame("Eleven", pairs.get(0).k);
    
    pairs.clear();
    map.forEachPair(new ObjectDoubleProcedure<String>() {
      int count = 0;
      
      @Override
      public boolean apply(String first, double second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    assertEquals((double)11, map.get("Eleven") , (double)0.000001);
  }

  @Test
  public void testKeys() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    List<String> keys = new ArrayList<String>();
    map.keys(keys);
    Collections.sort(keys);
    assertSame("Twelve", keys.get(1));
    assertSame("Eleven", keys.get(0));
    List<String> k2 = map.keys();
    Collections.sort(k2);
    assertEquals(keys, k2);
  }
  
  @Test
  public void testAdjustOrPutValue() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.adjustOrPutValue("Eleven", (double)1, (double)3);
    assertEquals(14, map.get(new String("Eleven")) , (double)0.000001);
    map.adjustOrPutValue("Fifteen", (double)1, (double)3);
    assertEquals(1, map.get("Fifteen") , (double)0.000001);
  }
  
  @Test
  public void testPairsMatching() {
    List<String> keyList = new ArrayList<String>();
    DoubleArrayList valueList = new DoubleArrayList();
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    map.pairsMatching(new ObjectDoubleProcedure<String>() {

      @Override
      public boolean apply(String first, double second) {
        return (second % 2) == 0;
      }},
        keyList, valueList);
    Collections.sort(keyList);
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertSame("Fourteen", keyList.get(0));
    assertSame("Twelve", keyList.get(1));
    assertEquals((double)14, valueList.get(1) , (double)0.000001);
    assertEquals((double)12, valueList.get(0) , (double)0.000001);
  }
  
  @Test
  public void testValues() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    DoubleArrayList values = new DoubleArrayList(100);
    map.values(values);
    assertEquals(3, values.size());
    values.sort();
    assertEquals(11, values.get(0) , (double)0.000001);
    assertEquals(12, values.get(1) , (double)0.000001);
    assertEquals(14, values.get(2) , (double)0.000001);
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double)11);
    OpenObjectDoubleHashMap<String> map2 = (OpenObjectDoubleHashMap<String>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    OpenObjectDoubleHashMap<String> map2 = (OpenObjectDoubleHashMap<String>) map.copy();
    assertEquals(map, map2);
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.removeKey("Eleven");
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
  
  // keys() tested in testKeys
  
  @Test
  public void testKeysSortedByValue() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    map.removeKey("Thirteen");
    List<String> keys = new ArrayList<String>();
    map.keysSortedByValue(keys);
    String[] keysArray = keys.toArray(new String[keys.size()]);
    assertArrayEquals(new String[] {"Eleven", "Twelve", "Fourteen"},
        keysArray);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    
    DoubleArrayList values = new DoubleArrayList();
    List<String> keys = new ArrayList<String>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((double) 11, values.get(0) , (double)0.000001);
    assertSame("Eleven", keys.get(0));
    assertEquals((double) 14, values.get(1) , (double)0.000001);
    assertSame("Fourteen", keys.get(1));
    assertEquals((double) 13, values.get(2) , (double)0.000001);
    assertSame("Thirteen", keys.get(2));
    assertEquals((double) 12, values.get(3) , (double)0.000001);
    assertSame("Twelve", keys.get(3));
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testPairsSortedByKeyNotComparable() {
    OpenObjectDoubleHashMap<NotComparableKey> map = new OpenObjectDoubleHashMap<NotComparableKey>();
    map.put(ncKeys[0], (double) 11);
    map.put(ncKeys[1], (double) 12);
    map.put(ncKeys[2], (double) 13);
    map.put(ncKeys[3], (double) 14);
    DoubleArrayList values = new DoubleArrayList();
    List<NotComparableKey> keys = new ArrayList<NotComparableKey>();
    map.pairsSortedByKey(keys, values);
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenObjectDoubleHashMap<String> map = new OpenObjectDoubleHashMap<String>();
    map.put("Eleven", (double) 11);
    map.put("Twelve", (double) 12);
    map.put("Thirteen", (double) 13);
    map.put("Fourteen", (double) 14);
    
    List<String> keys = new ArrayList<String>();
    DoubleArrayList values = new DoubleArrayList();
    map.pairsSortedByValue(keys, values);
    assertEquals((double) 11, values.get(0) , (double)0.000001);
    assertEquals("Eleven", keys.get(0));
    assertEquals((double) 12, values.get(1) , (double)0.000001);
    assertEquals("Twelve", keys.get(1));
    assertEquals((double) 13, values.get(2) , (double)0.000001);
    assertEquals("Thirteen", keys.get(2));
    assertEquals((double) 14, values.get(3) , (double)0.000001);
    assertEquals("Fourteen", keys.get(3));
  }
 
 }
