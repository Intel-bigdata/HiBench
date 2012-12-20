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

package org.apache.mahout.math.list;

import org.apache.mahout.math.function.IntProcedure;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class IntArrayListTest extends Assert {
  
  private IntArrayList emptyList;
  private IntArrayList listOfFive;
  
  @Before
  public void before() {
    emptyList = new IntArrayList();
    listOfFive = new IntArrayList();
    for (int x = 0; x < 5; x ++) {
      listOfFive.add((int)x);
    }
  }
  
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetEmpty() {
    emptyList.get(0);
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void setEmpty() {
    emptyList.set(1, (int)1);
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void beforeInsertInvalidRange() {
    emptyList.beforeInsert(1, (int)0);
  }
  
  @Test
  public void testAdd() {
    emptyList.add((int)12);
    assertEquals(1, emptyList.size());
    for (int x = 0; x < 1000; x ++) {
      emptyList.add((int)(x % Integer.MAX_VALUE));
    }
    assertEquals(1001, emptyList.size());
    assertEquals(12, emptyList.get(0) );
    for (int x = 0; x < 1000; x ++) {
      assertEquals((int)(x % Integer.MAX_VALUE), emptyList.get(x+1) );
    }
  }
  
  @Test
  public void testBinarySearch() 
  {
    int x = listOfFive.binarySearchFromTo((int)0, 2, 4);
    assertEquals(-3, x);
    x = listOfFive.binarySearchFromTo((int)1, 0, 4);
    assertEquals(1, x);
  }
  
  @Test
  public void testClone() {
    IntArrayList l2 = listOfFive.copy(); // copy just calls clone.
    assertNotSame(listOfFive, l2);
    assertEquals(listOfFive, l2);
  }
  
  @Test 
  public void testElements() {
    int[] l = { 12, 24, 36, 48 };
    IntArrayList lar = new IntArrayList(l);
    assertEquals(4, lar.size());
    assertSame(l, lar.elements());
    int[] l2 = { 3, 6, 9, 12 };
    lar.elements(l2);
    assertSame(l2, lar.elements());
  }
  
  @Test
  public void testEquals() {
    int[] l = { 12, 24, 36, 48 };
    IntArrayList lar = new IntArrayList(l);
    IntArrayList lar2 = new IntArrayList();
    for (int x = 0; x < lar.size(); x++) {
      lar2.add(lar.get(x));
    }
    assertEquals(lar, lar2);
    assertFalse(lar.equals(this));
    lar2.add((int)55);
    assertFalse(lar.equals(lar2));
  }

  @Test
  public void testForEach() {
    listOfFive.forEach(new IntProcedure() {
      int count;
      @Override
      public boolean apply(int element) {
        assertFalse(count > 2);
        count ++;
        return element != 1;
      }});
  }
  
  @Test
  public void testGetQuick() {
    IntArrayList lar = new IntArrayList(10);
    lar.getQuick(1); // inside capacity, outside size.
  }
  
  @Test
  public void testIndexOfFromTo() {
    int x = listOfFive.indexOfFromTo((int)0, 2, 4);
    assertEquals(-1, x);
    x = listOfFive.indexOfFromTo((int)1, 0, 4);
    assertEquals(1, x);
  }
  
  @Test
  public void testLastIndexOfFromTo() {
    IntArrayList lar = new IntArrayList(10);
    lar.add((int)1);
    lar.add((int)2);
    lar.add((int)3);
    lar.add((int)2);
    lar.add((int)1);
    assertEquals(3, lar.lastIndexOf((int)2));
    assertEquals(3, lar.lastIndexOfFromTo((int)2, 2, 4));
    assertEquals(-1, lar.lastIndexOf((int)111));
  }
  
  @Test
  public void testPartFromTo() {
    AbstractIntList al = listOfFive.partFromTo(1, 2);
    assertEquals(2, al.size());
    assertEquals(1, al.get(0) );
    assertEquals(2, al.get(1) );
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void testPartFromToOOB() {
    listOfFive.partFromTo(10, 11);
  }
  
  @Test
  public void testRemoveAll() {
    IntArrayList lar = new IntArrayList(1000);
    for (int x = 0; x < 128; x ++) {
      lar.add((int)x);
    }
    IntArrayList larOdd = new IntArrayList(500);
    for (int x = 1; x < 128; x = x + 2) {
      larOdd.add((int)x);
    }
    lar.removeAll(larOdd);
    assertEquals(64, lar.size());
    
    for (int x = 0; x < lar.size(); x++) {
      assertEquals(x*2, lar.get(x) );
    }
  }
  
  @Test
  public void testReplaceFromToWith() {
    listOfFive.add((int)5);
    IntArrayList lar = new IntArrayList();
    lar.add((int)44);
    lar.add((int)55);
    listOfFive.replaceFromToWithFromTo(2, 3, lar, 0, 1);
    assertEquals(0, listOfFive.get(0) );
    assertEquals(1, listOfFive.get(1) );
    assertEquals(44, listOfFive.get(2) );
    assertEquals(55, listOfFive.get(3) );
    assertEquals(4, listOfFive.get(4) );
    assertEquals(5, listOfFive.get(5) );
  }
  
  @Test
  public void testRetainAllSmall() {
    IntArrayList lar = new IntArrayList();
    lar.addAllOf(listOfFive);
    lar.addAllOf(listOfFive);
    lar.addAllOf(listOfFive);
    IntArrayList lar2 = new IntArrayList();
    lar2.add((int)3);
    lar2.add((int)4);
    assertTrue(lar.retainAll(lar2));
    for(int x = 0; x < lar.size(); x ++) {
      int l = lar.get(x);
      assertTrue(l == 3 || l == 4);
    }
    assertEquals(6, lar.size());
  }
  
  @Test
  public void testRetainAllSmaller() {
    IntArrayList lar = new IntArrayList();
    lar.addAllOf(listOfFive);
    IntArrayList lar2 = new IntArrayList();
    // large 'other' arg to take the other code path.
    for (int x = 0; x < 1000; x ++) {
      lar2.add((int)3);
      lar2.add((int)4);
    }
    assertTrue(lar.retainAll(lar2));
    for(int x = 0; x < lar.size(); x ++) {
      int l = lar.get(x);
      assertTrue(l == 3 || l == 4);
    }
  }

}
