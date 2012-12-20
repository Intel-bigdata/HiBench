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

import org.apache.mahout.math.function.ObjectCharProcedure;
import org.apache.mahout.math.function.ObjectProcedure;
import org.apache.mahout.math.list.CharArrayList;
import org.apache.mahout.math.set.AbstractSet;
import org.junit.Assert;
import org.junit.Test;

public class OpenObjectCharHashMapTest extends Assert {

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
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenObjectCharHashMap<String>(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenObjectCharHashMap<String>(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
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
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char)11); 
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testClone() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char)11);
    OpenObjectCharHashMap<String> map2 = (OpenObjectCharHashMap<String>) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContainsKey() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char)11);
    assertTrue(map.containsKey("Eleven"));
    assertTrue(map.containsKey(new String("Eleven")));
    assertFalse(map.containsKey("Twelve"));
  }
  
  @Test
  public void testContainValue() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char)11);
    assertTrue(map.containsValue((char)11));
    assertFalse(map.containsValue((char)12));
  }
  
  @Test
  public void testForEachKey() {
    final List<String> keys = new ArrayList<String>();
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
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
    char v;
    String k;
    
    Pair(String k, char v) {
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
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.removeKey("Thirteen");
    map.forEachPair(new ObjectCharProcedure<String>() {
      
      @Override
      public boolean apply(String first, char second) {
        pairs.add(new Pair(first, second));
        return true;
      }
    });
    
    Collections.sort(pairs);
    assertEquals(3, pairs.size());
    assertEquals((char)14, pairs.get(1).v );
    assertSame("Fourteen", pairs.get(1).k);
    assertEquals((char) 12, pairs.get(2).v );
    assertSame("Twelve", pairs.get(2).k);
    assertEquals((char) 11, pairs.get(0).v );
    assertSame("Eleven", pairs.get(0).k);
    
    pairs.clear();
    map.forEachPair(new ObjectCharProcedure<String>() {
      int count = 0;
      
      @Override
      public boolean apply(String first, char second) {
        pairs.add(new Pair(first, second));
        count++;
        return count < 2;
      }
    });
    
    assertEquals(2, pairs.size());
  }
  
  @Test
  public void testGet() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    assertEquals((char)11, map.get("Eleven") );
  }

  @Test
  public void testKeys() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
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
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.adjustOrPutValue("Eleven", (char)1, (char)3);
    assertEquals(14, map.get(new String("Eleven")) );
    map.adjustOrPutValue("Fifteen", (char)1, (char)3);
    assertEquals(1, map.get("Fifteen") );
  }
  
  @Test
  public void testPairsMatching() {
    List<String> keyList = new ArrayList<String>();
    CharArrayList valueList = new CharArrayList();
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.removeKey("Thirteen");
    map.pairsMatching(new ObjectCharProcedure<String>() {

      @Override
      public boolean apply(String first, char second) {
        return (second % 2) == 0;
      }},
        keyList, valueList);
    Collections.sort(keyList);
    valueList.sort();
    assertEquals(2, keyList.size());
    assertEquals(2, valueList.size());
    assertSame("Fourteen", keyList.get(0));
    assertSame("Twelve", keyList.get(1));
    assertEquals((char)14, valueList.get(1) );
    assertEquals((char)12, valueList.get(0) );
  }
  
  @Test
  public void testValues() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.removeKey("Thirteen");
    CharArrayList values = new CharArrayList(100);
    map.values(values);
    assertEquals(3, values.size());
    values.sort();
    assertEquals(11, values.get(0) );
    assertEquals(12, values.get(1) );
    assertEquals(14, values.get(2) );
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char)11);
    OpenObjectCharHashMap<String> map2 = (OpenObjectCharHashMap<String>) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.removeKey("Thirteen");
    OpenObjectCharHashMap<String> map2 = (OpenObjectCharHashMap<String>) map.copy();
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
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    map.removeKey("Thirteen");
    List<String> keys = new ArrayList<String>();
    map.keysSortedByValue(keys);
    String[] keysArray = keys.toArray(new String[keys.size()]);
    assertArrayEquals(new String[] {"Eleven", "Twelve", "Fourteen"},
        keysArray);
  }
  
  @Test
  public void testPairsSortedByKey() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    
    CharArrayList values = new CharArrayList();
    List<String> keys = new ArrayList<String>();
    map.pairsSortedByKey(keys, values);
    
    assertEquals(4, keys.size());
    assertEquals(4, values.size());
    assertEquals((char) 11, values.get(0) );
    assertSame("Eleven", keys.get(0));
    assertEquals((char) 14, values.get(1) );
    assertSame("Fourteen", keys.get(1));
    assertEquals((char) 13, values.get(2) );
    assertSame("Thirteen", keys.get(2));
    assertEquals((char) 12, values.get(3) );
    assertSame("Twelve", keys.get(3));
  }
  
  @Test(expected=UnsupportedOperationException.class)
  public void testPairsSortedByKeyNotComparable() {
    OpenObjectCharHashMap<NotComparableKey> map = new OpenObjectCharHashMap<NotComparableKey>();
    map.put(ncKeys[0], (char) 11);
    map.put(ncKeys[1], (char) 12);
    map.put(ncKeys[2], (char) 13);
    map.put(ncKeys[3], (char) 14);
    CharArrayList values = new CharArrayList();
    List<NotComparableKey> keys = new ArrayList<NotComparableKey>();
    map.pairsSortedByKey(keys, values);
  }
  
  @Test
  public void testPairsSortedByValue() {
    OpenObjectCharHashMap<String> map = new OpenObjectCharHashMap<String>();
    map.put("Eleven", (char) 11);
    map.put("Twelve", (char) 12);
    map.put("Thirteen", (char) 13);
    map.put("Fourteen", (char) 14);
    
    List<String> keys = new ArrayList<String>();
    CharArrayList values = new CharArrayList();
    map.pairsSortedByValue(keys, values);
    assertEquals((char) 11, values.get(0) );
    assertEquals("Eleven", keys.get(0));
    assertEquals((char) 12, values.get(1) );
    assertEquals("Twelve", keys.get(1));
    assertEquals((char) 13, values.get(2) );
    assertEquals("Thirteen", keys.get(2));
    assertEquals((char) 14, values.get(3) );
    assertEquals("Fourteen", keys.get(3));
  }
 
 }
