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

import org.apache.mahout.math.function.ObjectFloatProcedure;
import org.apache.mahout.math.function.ObjectProcedure;
import org.apache.mahout.math.list.FloatArrayList;
import org.apache.mahout.math.set.AbstractSet;
import org.junit.Assert;
import org.junit.Test;

public class OpenObjectFloatHashMapTest extends Assert {

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
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenObjectFloatHashMap<String>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenObjectFloatHashMap<String>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
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
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float)11); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testClone() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float)11);
    OpenObjectFloatHashMap<String> map2 = (OpenObjectFloatHashMap<String>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float)11);
    assertTrue(map.containsKey("Eleven"));
    assertTrue(map.containsKey(new String("Eleven")));
    assertFalse(map.containsKey("Twelve"));
  }
  
  @Test
  public void testContainValue() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float)11);
    assertTrue(map.containsValue((float)11));
    assertFalse(map.containsValue((float)12));
  }
  
  @Test
  public void testForEachKey() {
    final List<String> keys = new ArrayList<String>();
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
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
    float v;
    String k;
    
    Pair(String k, float v) {
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
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.removeKey("Thirteen");
    map.forEachPair(new ObjectFloatProcedure<String>() {
      
      @Override
      public boolean apply(String first, float second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((float)14, pairs.get(1).v , (float)0.000001);
    assertSame("Fourteen", pairs.get(1).k);
    assertEquals((float) 12, pairs.get(2).v , (float)0.000001);
    assertSame("Twelve", pairs.get(2).k);
    assertEquals((float) 11, pairs.get(0).v , (float)0.000001);
    assertSame("Eleven", pairs.get(0).k);
    
    pairs.clear();
    map.forEachPair(new ObjectFloatProcedure<String>() {
      int count = 0;
      
      @Override
      public boolean apply(String first, float second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    assertEquals((float)11, map.get("Eleven") , (float)0.000001);
  }

  @Test
  public void testKeys() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
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
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.adjustOrPutValue("Eleven", (float)1, (float)3);
    assertEquals(14, map.get(new String("Eleven")) , (float)0.000001);
    map.adjustOrPutValue("Fifteen", (float)1, (float)3);
    assertEquals(1, map.get("Fifteen") , (float)0.000001);
  }
  
  @Test
  public void testPairsMatching() {
    List<String> keyList = new ArrayList<String>();
    FloatArrayList valueList = new FloatArrayList();
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.removeKey("Thirteen");
    map.pairsMatching(new ObjectFloatProcedure<String>() {

      @Override
      public boolean apply(String first, float second) {
        return (second % 2) == 0;
      }},
        keyList, valueList);
    Collections.sort(keyList);
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertSame("Fourteen", keyList.get(0));
    assertSame("Twelve", keyList.get(1));
    assertEquals((float)14, valueList.get(1) , (float)0.000001);
    assertEquals((float)12, valueList.get(0) , (float)0.000001);
  }
  
  @Test
  public void testValues() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.removeKey("Thirteen");
    FloatArrayList values = new FloatArrayList(100);
    map.values(values);
    assertEquals(3, values.size());
    values.sort();
    assertEquals(11, values.get(0) , (float)0.000001);
    assertEquals(12, values.get(1) , (float)0.000001);
    assertEquals(14, values.get(2) , (float)0.000001);
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float)11);
    OpenObjectFloatHashMap<String> map2 = (OpenObjectFloatHashMap<String>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.removeKey("Thirteen");
    OpenObjectFloatHashMap<String> map2 = (OpenObjectFloatHashMap<String>) map.copy();
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
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    map.removeKey("Thirteen");
    List<String> keys = new ArrayList<String>();
    map.keysSortedByValue(keys);
    String[] keysArray = keys.toArray(new String[keys.size()]);
    assertArrayEquals(new String[] {"Eleven", "Twelve", "Fourteen"},
        keysArray);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    
    FloatArrayList values = new FloatArrayList();
    List<String> keys = new ArrayList<String>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((float) 11, values.get(0) , (float)0.000001);
    assertSame("Eleven", keys.get(0));
    assertEquals((float) 14, values.get(1) , (float)0.000001);
    assertSame("Fourteen", keys.get(1));
    assertEquals((float) 13, values.get(2) , (float)0.000001);
    assertSame("Thirteen", keys.get(2));
    assertEquals((float) 12, values.get(3) , (float)0.000001);
    assertSame("Twelve", keys.get(3));
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testPairsSortedByKeyNotComparable() {
    OpenObjectFloatHashMap<NotComparableKey> map = new OpenObjectFloatHashMap<NotComparableKey>();
    map.put(ncKeys[0], (float) 11);
    map.put(ncKeys[1], (float) 12);
    map.put(ncKeys[2], (float) 13);
    map.put(ncKeys[3], (float) 14);
    FloatArrayList values = new FloatArrayList();
    List<NotComparableKey> keys = new ArrayList<NotComparableKey>();
    map.pairsSortedByKey(keys, values);
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenObjectFloatHashMap<String> map = new OpenObjectFloatHashMap<String>();
    map.put("Eleven", (float) 11);
    map.put("Twelve", (float) 12);
    map.put("Thirteen", (float) 13);
    map.put("Fourteen", (float) 14);
    
    List<String> keys = new ArrayList<String>();
    FloatArrayList values = new FloatArrayList();
    map.pairsSortedByValue(keys, values);
    assertEquals((float) 11, values.get(0) , (float)0.000001);
    assertEquals("Eleven", keys.get(0));
    assertEquals((float) 12, values.get(1) , (float)0.000001);
    assertEquals("Twelve", keys.get(1));
    assertEquals((float) 13, values.get(2) , (float)0.000001);
    assertEquals("Thirteen", keys.get(2));
    assertEquals((float) 14, values.get(3) , (float)0.000001);
    assertEquals("Fourteen", keys.get(3));
  }
 
 }
