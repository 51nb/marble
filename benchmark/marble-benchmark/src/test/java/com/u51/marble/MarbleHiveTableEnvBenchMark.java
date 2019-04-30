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
package com.u51.marble;

import com.google.common.base.Stopwatch;
import org.apache.calcite.adapter.hive.HiveTableEnv;
import org.apache.calcite.table.DataTable;
import org.apache.calcite.table.TableEnv;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class MarbleHiveTableEnvBenchMark {

  static {
    TableEnv.enableSqlPlanCacheSize(200);
  }

  @BeforeClass
  public static void setup() throws Throwable {
    BenchMarkUtil.initDBTables();
  }

  private static double sqlQuery(TableEnv tableEnv, String sql) {
    Stopwatch s = Stopwatch.createStarted();
    DataTable resultTable = tableEnv.sqlQuery(sql);
    System.out.println("sqlQuery result table size:" + resultTable.getRowCount());
    s.stop();
    return s.elapsed(TimeUnit.MICROSECONDS) * 0.001;
  }

  public double runSqlForSingleTable(int limit, String sql) throws Throwable {
    Stopwatch s = Stopwatch.createStarted();
    try (Connection connection = BenchMarkUtil.getDBConnection()) {
      String fetchSql = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit);
      ResultSet resultSet = connection
          .createStatement()
          .executeQuery(fetchSql);
      TableEnv tableEnv = HiveTableEnv.getTableEnv();
      DataTable input = tableEnv.fromJdbcResultSet(resultSet);
      tableEnv.registerTable("item1", input);
      s.stop();
      return s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(tableEnv, sql);
    }
  }

  public double runSqlForJoin(int limit, String sql) throws Throwable {
    Stopwatch s = Stopwatch.createStarted();
    try (Connection connection = BenchMarkUtil.getDBConnection()) {
      String fetchSql1 = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit);
      ResultSet resultSet1 = connection
          .createStatement()
          .executeQuery(fetchSql1);
      TableEnv tableEnv = HiveTableEnv.getTableEnv();
      DataTable input1 = tableEnv.fromJdbcResultSet(resultSet1);
      tableEnv.registerTable("item1", input1);

      String fetchSql2 = BenchMarkUtil.generateFetchSql("item2", "i_item_sk", limit);
      ResultSet resultSet2 = connection
          .createStatement()
          .executeQuery(fetchSql2);
      DataTable input2 = tableEnv.fromJdbcResultSet(resultSet2);
      tableEnv.registerTable("item2", input2);
      s.stop();
      return s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(tableEnv, sql);
    }
  }

  @Test
  public void testProjection() throws Throwable {
    String sql = "SELECT * FROM  item1";
    //warm up jvm class
    runSqlForSingleTable(1, sql);
    System.out.println(runSqlForSingleTable(200, sql));
    System.out.println(runSqlForSingleTable(2000, sql));
    System.out.println(runSqlForSingleTable(4000, sql));
    System.out.println(runSqlForSingleTable(10000, sql));
    System.out.println(runSqlForSingleTable(15000, sql));

  }

  @Test
  public void testEquiJoin() throws Throwable {
    String sql = "SELECT t1.*,t2.*,substring('abc',1,2)  FROM item1 t1 INNER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk";
    runSqlForJoin(1, sql);
    System.out.println(runSqlForJoin(200, sql));
    System.out.println(runSqlForJoin(2000, sql));
    System.out.println(runSqlForJoin(4000, sql));
    System.out.println(runSqlForJoin(10000, sql));
    System.out.println(runSqlForJoin(15000, sql));

  }


  @Test
  public void testThetaJoin() throws Throwable {
    String sql = "SELECT t1.i_item_sk,t2.i_item_sk  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000";
    runSqlForJoin(1, sql);
    System.out.println(runSqlForJoin(200, sql));
    System.out.println(runSqlForJoin(2000, sql));
    System.out.println(runSqlForJoin(4000, sql));
    System.out.println(runSqlForJoin(10000, sql));
    System.out.println(runSqlForJoin(15000, sql));

  }

  @Test
  public void testAggregate() throws Throwable {
    String sql = "SELECT count(*) ,sum(i_current_price),max(i_current_price)  FROM item1 group by i_item_id ";
    runSqlForSingleTable(1, sql);
    System.out.println(runSqlForSingleTable(200, sql));
    System.out.println(runSqlForSingleTable(2000, sql));
    System.out.println(runSqlForSingleTable(4000, sql));
    System.out.println(runSqlForSingleTable(10000, sql));
    System.out.println(runSqlForSingleTable(15000, sql));

  }

  @Test
  public void testComplexSql() throws Throwable {
    String sql = "SELECT t4.* FROM (SELECT t3.i_item_id,count(t3.i_item_id) as count0 ,sum(t3.i_current_price) as sum0,max(t3.i_current_price) as max0  FROM (SELECT t1.*  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000) t3 group by t3.i_item_id ) t4 where t4.count0>2";
    runSqlForJoin(1, sql);
    System.out.println(runSqlForJoin(15000, sql));
  }

}

// End MarbleHiveTableEnvBenchMark.java
