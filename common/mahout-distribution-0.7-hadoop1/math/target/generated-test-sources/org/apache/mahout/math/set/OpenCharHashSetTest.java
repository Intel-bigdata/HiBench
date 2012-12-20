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
 
  
package org.apache.mahout.math.set;
 
import java.util.Arrays;

import org.apache.mahout.math.function.CharProcedure;
import org.apache.mahout.math.list.CharArrayList;
import org.apache.mahout.math.map.PrimeFinder;

import org.junit.Assert;
import org.junit.Test;

public class OpenCharHashSetTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenCharHashSet map = new OpenCharHashSet();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenCharHashSet(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenCharHashSet(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenCharHashSet map = new OpenCharHashSet();
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
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
  }
  
  @Test
  public void testClone() {
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    OpenCharHashSet map2 = (OpenCharHashSet) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContains() {
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    assertTrue(map.contains((char) 11));
    assertFalse(map.contains((char) 12));
  }
  
  @Test
  public void testForEachKey() {
    final CharArrayList keys = new CharArrayList();
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    map.add((char) 12);
    map.add((char) 13);
    map.add((char) 14);
    map.remove((char) 13);
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
  
  @Test
  public void testKeys() {
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    map.add((char) 12);
    CharArrayList keys = new CharArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) );
    assertEquals(12, keys.get(1) );
    CharArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    OpenCharHashSet map2 = (OpenCharHashSet) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenCharHashSet map = new OpenCharHashSet();
    map.add((char) 11);
    map.add((char) 12);
    map.add((char) 13);
    map.add((char) 14);
    map.remove((char) 13);
    OpenCharHashSet map2 = (OpenCharHashSet) map.copy();
    assertTrue(map.equals(map2));
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.remove((char) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
 }
