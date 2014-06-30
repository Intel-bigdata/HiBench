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

import org.apache.mahout.math.function.DoubleProcedure;
import org.apache.mahout.math.list.DoubleArrayList;
import org.apache.mahout.math.map.PrimeFinder;

import org.junit.Assert;
import org.junit.Test;

public class OpenDoubleHashSetTest extends Assert {

  
  @Test
  public void testConstructors() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    int[] capacity = new int[1];
    double[] minLoadFactor = new double[1];
    double[] maxLoadFactor = new double[1];
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(AbstractSet.defaultCapacity, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    int prime = PrimeFinder.nextPrime(907);
    map = new OpenDoubleHashSet(prime);
    
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(AbstractSet.defaultMaxLoadFactor, maxLoadFactor[0], 0.001);
    assertEquals(AbstractSet.defaultMinLoadFactor, minLoadFactor[0], 0.001);
    
    map = new OpenDoubleHashSet(prime, 0.4, 0.8);
    map.getInternalFactors(capacity, minLoadFactor, maxLoadFactor);
    assertEquals(prime, capacity[0]);
    assertEquals(0.4, minLoadFactor[0], 0.001);
    assertEquals(0.8, maxLoadFactor[0], 0.001);
  }
  
  @Test
  public void testEnsureCapacity() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
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
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    assertEquals(1, map.size());
    map.clear();
    assertEquals(0, map.size());
  }
  
  @Test
  public void testClone() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    OpenDoubleHashSet map2 = (OpenDoubleHashSet) map.clone();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testContains() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    assertTrue(map.contains((double) 11));
    assertFalse(map.contains((double) 12));
  }
  
  @Test
  public void testForEachKey() {
    final DoubleArrayList keys = new DoubleArrayList();
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    map.add((double) 12);
    map.add((double) 13);
    map.add((double) 14);
    map.remove((double) 13);
    map.forEachKey(new DoubleProcedure() {
      
      @Override
      public boolean apply(double element) {
        keys.add(element);
        return true;
      }
    });
    
    double[] keysArray = keys.toArray(new double[keys.size()]);
    Arrays.sort(keysArray);
    
    assertArrayEquals(new double[] {11, 12, 14}, keysArray , (double)0.000001);
  }
  
  @Test
  public void testKeys() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    map.add((double) 12);
    DoubleArrayList keys = new DoubleArrayList();
    map.keys(keys);
    keys.sort();
    assertEquals(11, keys.get(0) , (double)0.000001);
    assertEquals(12, keys.get(1) , (double)0.000001);
    DoubleArrayList k2 = map.keys();
    k2.sort();
    assertEquals(keys, k2);
  }
  
  // tests of the code in the abstract class
  
  @Test
  public void testCopy() {
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    OpenDoubleHashSet map2 = (OpenDoubleHashSet) map.copy();
    map.clear();
    assertEquals(1, map2.size());
  }
  
  @Test
  public void testEquals() {
    // since there are no other subclasses of 
    // Abstractxxx available, we have to just test the
    // obvious.
    OpenDoubleHashSet map = new OpenDoubleHashSet();
    map.add((double) 11);
    map.add((double) 12);
    map.add((double) 13);
    map.add((double) 14);
    map.remove((double) 13);
    OpenDoubleHashSet map2 = (OpenDoubleHashSet) map.copy();
    assertTrue(map.equals(map2));
    assertTrue(map2.equals(map));
    assertFalse("Hello Sailor".equals(map));
    assertFalse(map.equals("hello sailor"));
    map2.remove((double) 11);
    assertFalse(map.equals(map2));
    assertFalse(map2.equals(map));
  }
 }
