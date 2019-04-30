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

import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class TableEnvTest {

  static final TableEnv tableEnv = TableEnv.getTableEnv();

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
    DataTable t1 = tableEnv.fromJavaPojoList(Lists.newArrayList(p1, p2, p3));
    tableEnv.addSubSchema("test");
    tableEnv.registerTable("test", "t1",
        t1);
    tableEnv.registerTable("t2",
        t1);
    tableEnv.addFunction("", "hello", TableEnvTest.class.getName(), "hello");
  }

  @Test
  public void testCreateTableFromRowListWithSqlType() {
    List<Map<String, Object>> rowList = new ArrayList<>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("c1", 1);
    row1.put("c2", 0.1);
    row1.put("c3", new Date().getTime());
    rowList.add(row1);
    Map<String, Object> row2 = new HashMap<>();
    row1.put("c1", 2);
    row1.put("c2", 0.2);
    row1.put("c3", new Date().getTime());
    rowList.add(row2);

    Map<String, SqlTypeName> sqlTypeMap = new HashMap<>();
    sqlTypeMap.put("c1", SqlTypeName.BIGINT);
    sqlTypeMap.put("c2", SqlTypeName.DECIMAL);
    sqlTypeMap.put("c3", SqlTypeName.TIMESTAMP);
    DataTable dataTable = tableEnv.fromRowListWithSqlTypeMap(rowList,
        sqlTypeMap);
    tableEnv.registerTable("t", dataTable);
    DataTable queryResult = tableEnv.sqlQuery("select * from t");
    Assert.assertTrue(queryResult.getRowCount() == 2);
  }


  @Test
  public void testApi() {
    DataTable table1 = tableEnv.sqlQuery("select * from test.t1");
    Assert.assertNotNull(table1);
    DataTable table2 = tableEnv.sqlQuery("select * from t2");
    Assert.assertNotNull(table2);
    Assert.assertEquals("hello,marble",
        tableEnv.sqlQuery("select hello('marble') as c1")
            .toMapList()
            .get(0)
            .get("c1"));
  }


  @Test
  public void testSqlPlanCache() {
    String query = "select * from test.t1 join t2 on t1.b=t2.b";
    tableEnv.sqlQuery(query);
    Assert.assertTrue(tableEnv.getExecutionCode(query).contains("T1")
        && tableEnv.getExecutionCode(query).contains("T2"));
    TableEnv.clearExecutionPlan();
    tableEnv.registerSqlBindableClass(query, BazJoinExample.class);
    tableEnv.sqlQuery(query);
    Assert.assertTrue(
        tableEnv.getExecutionPlan(query).getBindable().getClass()
            == BazJoinExample.class);
  }

}

// End TableEnvTest.java
