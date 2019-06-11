/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.table;

import org.apache.calcite.util.NlsString;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TypeConvertUtilTest {

  @Test
  public void testCastBigDecimal() {
    final BigDecimal actual =
            TypeConvertUtil.castBigDecimal(new BigDecimal(5));
    Assert.assertEquals(new BigDecimal(5), actual);
  }

  @Test
  public void testCastBoolean() {
    Assert.assertFalse(TypeConvertUtil.castBoolean(false));
  }

  @Test
  public void testCastDouble() {
    Assert.assertEquals(3.0, TypeConvertUtil.castDouble(3.0), 0.0);
  }

  @Test
  public void testCastFloat() {
    Assert.assertEquals(4.0f, TypeConvertUtil.castFloat(4.0f), 0.0);
  }

  @Test
  public void testCastInt() {
    Assert.assertEquals(new Integer(0), TypeConvertUtil.castInt(0));
  }

  @Test
  public void testCastList() {
    final List<Integer> value = new ArrayList<>();
    value.add(6);
    final List<Integer> actual = TypeConvertUtil.castList(value);

    final List<Integer> arrayList = new ArrayList<>();
    arrayList.add(6);

    Assert.assertEquals(arrayList, actual);
  }

  @Test
  public void testCastLong() {
    Assert.assertEquals(new Long(0L), TypeConvertUtil.castLong(0L));
  }

  @Test
  public void testCastObject() {
    Assert.assertEquals(0, TypeConvertUtil.castObject(0));
  }

  @Test
  public void testCastString() {
    Assert.assertEquals("Bar", TypeConvertUtil.castString("Bar"));
  }

  @Test
  public void testToBigDecimal() {
    Assert.assertNull(TypeConvertUtil.toBigDecimal(null));

    Assert.assertEquals(new BigDecimal(8), TypeConvertUtil.toBigDecimal(8));
  }

  @Test
  public void testToBoolean() {
    Assert.assertNull(TypeConvertUtil.toBoolean(null));

    Assert.assertTrue(TypeConvertUtil.toBoolean(9));
  }

  @Test
  public void testToDate() {
    Assert.assertNull(TypeConvertUtil.toDate(null, null));

    Assert.assertEquals("1970-01-25",
            TypeConvertUtil.toDate(2147483647, null).toString());
    Assert.assertEquals("2019-06-11",
            TypeConvertUtil.toDate(1560265977787L, null).toString());
    Assert.assertEquals("2019-06-11",
            TypeConvertUtil.toDate(new Date(1560265977787L), null).toString());
  }

  @Test
  public void testToDouble() {
    Assert.assertNull(TypeConvertUtil.toDouble(null));

    Assert.assertEquals(10.0, TypeConvertUtil.toDouble(10.0), 0.0);
  }

  @Test
  public void testToFloat() {
    Assert.assertNull(TypeConvertUtil.toFloat(null));

    Assert.assertEquals(11.0f, TypeConvertUtil.toFloat(11.0f), 0.0f);
  }

  @Test
  public void testToInteger() {
    Assert.assertNull(TypeConvertUtil.toInteger(null));

    Assert.assertEquals(new Integer(0), TypeConvertUtil.toInteger(false));
    Assert.assertEquals(new Integer(1), TypeConvertUtil.toInteger(true));
    Assert.assertEquals(new Integer(12), TypeConvertUtil.toInteger(12));
  }

  @Test
  public void testToLong() {
    Assert.assertNull(TypeConvertUtil.toLong(null));

    Assert.assertEquals(new Long(13L), TypeConvertUtil.toLong(13));
  }

  @Test
  public void testToShort() {
    Assert.assertNull(TypeConvertUtil.toShort(null));

    Assert.assertEquals(new Short((short)14), TypeConvertUtil.toShort(14));
  }

  @Test
  public void testToString() {
    Assert.assertNull(TypeConvertUtil.toString(null));

    final NlsString nlsString = new NlsString("abc", null, null);
    Assert.assertEquals("abc", TypeConvertUtil.toString(nlsString));
    Assert.assertEquals("15", TypeConvertUtil.toString("15"));
  }

  @Test
  public void testToTimestamp() {
    Assert.assertNull(TypeConvertUtil.toTimestamp(null, null));

    Assert.assertEquals("2019-06-11 16:12:57.787",
            TypeConvertUtil.toTimestamp(1560265977787L,
                    null).toString());
    Assert.assertEquals("1970-01-25 21:31:23.647",
            TypeConvertUtil.toTimestamp(2147483647,
                    null).toString());
    Assert.assertEquals("2019-06-11 16:12:57.787",
            TypeConvertUtil.toTimestamp(new Timestamp(1560265977787L),
                    null).toString());
    Assert.assertEquals("2019-06-11 16:12:57.787",
            TypeConvertUtil.toTimestamp(new Date(1560265977787L),
                    null).toString());
  }

}
