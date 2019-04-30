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

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

/**
 *
 */
public class TableEnvTest {

  static final TableEnv TABLE_ENV = TableEnv.getTableEnv();

  static {
    TableEnv.enableSqlPlanCacheSize(200);
  }

  /**
   *
   */
  public static class Pojo1 {
    public Integer a;
    public int b;
    public String c;
    public Date d;
    public Long e;
    public long f;
    public Double g;
    public double h;
    public BigDecimal i;
    public Date j;
    public Boolean k;
  }

  public static String hello(String p) {
    return "hello," + p;
  }

  @BeforeClass
  public static void setUp() {
    Date date = new Date(1538549662000L);
    Pojo1 p1 = new Pojo1();
    p1.i = new BigDecimal("23.0");
    p1.b = 1;
    p1.c = "a,b,c";
    p1.h = 1.0;
    p1.f = 1000;
    p1.j = date;
    p1.k = true;
    Pojo1 p2 = new Pojo1();
    p2.i = new BigDecimal("44.55");
    p2.b = 2;
    p2.c = "a,b,c,d";
    p2.h = 2.5;
    p2.f = 100024294;
    p2.j = date;
    p2.k = false;
    Pojo1 p3 = new Pojo1();
    p3.b = 3;
    DataTable t1 = TABLE_ENV.fromJavaPojoList(Lists.newArrayList(p1, p2, p3));
    TABLE_ENV.addSubSchema("test");
    TABLE_ENV.registerTable("test", "t1",
        t1);
    TABLE_ENV.registerTable("t2",
        t1);
    TABLE_ENV.addFunction("", "hello", TableEnvTest.class.getName(), "hello");
  }

  @Test
  public void testApi() {
    DataTable table1 = TABLE_ENV.sqlQuery("select * from test.t1");
    Assert.assertNotNull(table1);
    DataTable table2 = TABLE_ENV.sqlQuery("select * from t2");
    Assert.assertNotNull(table2);
    Assert.assertEquals("hello,marble",
        TABLE_ENV.sqlQuery("select hello('marble') as c1")
            .toMapList()
            .get(0)
            .get("c1"));
  }


  @Test
  public void testSqlPlanCache() {
    String query = "select * from test.t1 join t2 on t1.b=t2.b";
    TABLE_ENV.sqlQuery(query);
    Assert.assertTrue(TABLE_ENV.getExecutionCode(query).contains("T1")
        && TABLE_ENV.getExecutionCode(query).contains("T2"));
    TableEnv.clearExecutionPlan();
    TABLE_ENV.registerSqlBindableClass(query, BazJoinExample.class);
    TABLE_ENV.sqlQuery(query);
    Assert.assertTrue(
        TABLE_ENV.getExecutionPlan(query).getBindable().getClass()
            == BazJoinExample.class);
  }

}

// End TableEnvTest.java
