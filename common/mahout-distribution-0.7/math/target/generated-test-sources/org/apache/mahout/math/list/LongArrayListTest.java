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

import org.apache.mahout.math.function.LongProcedure;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class LongArrayListTest extends Assert {
  
  private LongArrayList emptyList;
  private LongArrayList listOfFive;
  
  @Before
  public void before() {
    emptyList = new LongArrayList();
    listOfFive = new LongArrayList();
    for (int x = 0; x < 5; x ++) {
      listOfFive.add((long)x);
    }
  }
  
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void testGetEmpty() {
    emptyList.get(0);
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void setEmpty() {
    emptyList.set(1, (long)1);
  }
  
  @Test(expected = IndexOutOfBoundsException.class)
  public void beforeInsertInvalidRange() {
    emptyList.beforeInsert(1, (long)0);
  }
  
  @Test
  public void testAdd() {
    emptyList.add((long)12);
    assertEquals(1, emptyList.size());
    for (int x = 0; x < 1000; x ++) {
      emptyList.add((long)(x % Long.MAX_VALUE));
    }
    assertEquals(1001, emptyList.size());
    assertEquals(12, emptyList.get(0) );
    for (int x = 0; x < 1000; x ++) {
      assertEquals((long)(x % Long.MAX_VALUE), emptyList.get(x+1) );
    }
  }
  
  @Test
  public void testBinarySearch() 
  {
    int x = listOfFive.binarySearchFromTo((long)0, 2, 4);
    assertEquals(-3, x);
    x = listOfFive.binarySearchFromTo((long)1, 0, 4);
    assertEquals(1, x);
  }
  
  @Test
  public void testClone() {
    LongArrayList l2 = listOfFive.copy(); // copy just calls clone.
    assertNotSame(listOfFive, l2);
    assertEquals(listOfFive, l2);
  }
  
  @Test 
  public void testElements() {
    long[] l = { 12, 24, 36, 48 };
    LongArrayList lar = new LongArrayList(l);
    assertEquals(4, lar.size());
    assertSame(l, lar.elements());
    long[] l2 = { 3, 6, 9, 12 };
    lar.elements(l2);
    assertSame(l2, lar.elements());
  }
  
  @Test
  public void testEquals() {
    long[] l = { 12, 24, 36, 48 };
    LongArrayList lar = new LongArrayList(l);
    LongArrayList lar2 = new LongArrayList();
    for (int x = 0; x < lar.size(); x++) {
      lar2.add(lar.get(x));
    }
    assertEquals(lar, lar2);
    assertFalse(lar.equals(this));
    lar2.add((long)55);
    assertFalse(lar.equals(lar2));
  }

  @Test
  public void testForEach() {
    listOfFive.forEach(new LongProcedure() {
      int count;
      @Override
      public boolean apply(long element) {
        assertFalse(count > 2);
        count ++;
        return element != 1;
      }});
  }
  
  @Test
  public void testGetQuick() {
    LongArrayList lar = new LongArrayList(10);
    lar.getQuick(1); // inside capacity, outside size.
  }
  
  @Test
  public void testIndexOfFromTo() {
    int x = listOfFive.indexOfFromTo((long)0, 2, 4);
    assertEquals(-1, x);
    x = listOfFive.indexOfFromTo((long)1, 0, 4);
    assertEquals(1, x);
  }
  
  @Test
  public void testLastIndexOfFromTo() {
    LongArrayList lar = new LongArrayList(10);
    lar.add((long)1);
    lar.add((long)2);
    lar.add((long)3);
    lar.add((long)2);
    lar.add((long)1);
    assertEquals(3, lar.lastIndexOf((long)2));
    assertEquals(3, lar.lastIndexOfFromTo((long)2, 2, 4));
    assertEquals(-1, lar.lastIndexOf((long)111));
  }
  
  @Test
  public void testPartFromTo() {
    AbstractLongList al = listOfFive.partFromTo(1, 2);
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
    LongArrayList lar = new LongArrayList(1000);
    for (int x = 0; x < 128; x ++) {
      lar.add((long)x);
    }
    LongArrayList larOdd = new LongArrayList(500);
    for (int x = 1; x < 128; x = x + 2) {
      larOdd.add((long)x);
    }
    lar.removeAll(larOdd);
    assertEquals(64, lar.size());
    
    for (int x = 0; x < lar.size(); x++) {
      assertEquals(x*2, lar.get(x) );
    }
  }
  
  @Test
  public void testReplaceFromToWith() {
    listOfFive.add((long)5);
    LongArrayList lar = new LongArrayList();
    lar.add((long)44);
    lar.add((long)55);
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
    LongArrayList lar = new LongArrayList();
    lar.addAllOf(listOfFive);
    lar.addAllOf(listOfFive);
    lar.addAllOf(listOfFive);
    LongArrayList lar2 = new LongArrayList();
    lar2.add((long)3);
    lar2.add((long)4);
    assertTrue(lar.retainAll(lar2));
    for(int x = 0; x < lar.size(); x ++) {
      long l = lar.get(x);
      assertTrue(l == 3 || l == 4);
    }
    assertEquals(6, lar.size());
  }
  
  @Test
  public void testRetainAllSmaller() {
    LongArrayList lar = new LongArrayList();
    lar.addAllOf(listOfFive);
    LongArrayList lar2 = new LongArrayList();
    // large 'other' arg to take the other code path.
    for (int x = 0; x < 1000; x ++) {
      lar2.add((long)3);
      lar2.add((long)4);
    }
    assertTrue(lar.retainAll(lar2));
    for(int x = 0; x < lar.size(); x ++) {
      long l = lar.get(x);
      assertTrue(l == 3 || l == 4);
    }
  }

}
