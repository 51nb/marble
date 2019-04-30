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

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Stopwatch;
import org.apache.flink.calcite.shaded.com.google.common.collect.BiMap;
import org.apache.flink.calcite.shaded.com.google.common.collect.HashBiMap;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 *
 */
public class FlinkCollectionsEnvBenchMark {

  private static final Map<Integer, TypeInformation<?>> TYPE_MAPPING;

  static {
    BiMap<TypeInformation<?>, Integer> m = HashBiMap.create();
    m.put(STRING_TYPE_INFO, Types.VARCHAR);
    m.put(BOOLEAN_TYPE_INFO, Types.BOOLEAN);
    m.put(BYTE_TYPE_INFO, Types.TINYINT);
    m.put(SHORT_TYPE_INFO, Types.SMALLINT);
    m.put(INT_TYPE_INFO, Types.INTEGER);
    m.put(LONG_TYPE_INFO, Types.BIGINT);
    m.put(FLOAT_TYPE_INFO, Types.FLOAT);
    m.put(DOUBLE_TYPE_INFO, Types.DOUBLE);
    m.put(SqlTimeTypeInfo.DATE, Types.DATE);
    m.put(SqlTimeTypeInfo.TIME, Types.TIME);
    m.put(SqlTimeTypeInfo.TIMESTAMP, Types.TIMESTAMP);
    m.put(BIG_DEC_TYPE_INFO, Types.DECIMAL);
    m.put(BYTE_PRIMITIVE_ARRAY_TYPE_INFO, Types.BINARY);
    TYPE_MAPPING = m.inverse();
  }

  @BeforeClass
  public static void setup() throws Throwable {
    BenchMarkUtil.initDBTables();
  }

  public RowTypeInfo typeOfJdbc(ResultSetMetaData metaData) throws Exception {
    int columnCount = metaData.getColumnCount();
    List<String> columnNameList = new ArrayList<>();
    List<TypeInformation> columnTypeList = new ArrayList<>();
    for (int i = 1; i <= columnCount; i++) {
      int jdbcType = metaData.getColumnType(i);
      String name = metaData.getColumnLabel(i);
      columnTypeList.add(TYPE_MAPPING.get(jdbcType));
      columnNameList.add(name.toLowerCase());

    }
    return new RowTypeInfo(columnTypeList.toArray(new TypeInformation[0]), columnNameList.toArray(new String[0]));

  }

  private static double sqlQuery(BatchTableEnvironment tEnv, String sql) throws Throwable {
    Stopwatch s = Stopwatch.createStarted();
    Table t2 = tEnv.sqlQuery(sql);
    System.out.println("sqlQuery result table size:" + tEnv.toDataSet(t2, Row.class).collect().size());
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
      RowTypeInfo rowTypeInfo = typeOfJdbc(resultSet.getMetaData());
      ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
      BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
      DataSet ds = env.createInput(
          JDBCInputFormat.buildJDBCInputFormat()
              .setDrivername(BenchMarkUtil.DB_DRIVER)
              .setDBUrl(BenchMarkUtil.DB_CONNECTION_URL)
              .setQuery(fetchSql)
              .setRowTypeInfo(rowTypeInfo)
              .finish()
      );
      ds.collect();
      tEnv.registerDataSet("item1", ds);
      s.stop();
      return s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(tEnv, sql);
    }
  }

  public double runSqlForJoin(int limit, String sql) throws Throwable {
    Stopwatch s = Stopwatch.createStarted();
    try (Connection connection = BenchMarkUtil.getDBConnection()) {
      ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
      BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
      String fetchSql1 = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit);
      ResultSet resultSet1 = connection
          .createStatement()
          .executeQuery(fetchSql1);
      RowTypeInfo rowTypeInfo1 = typeOfJdbc(resultSet1.getMetaData());
      DataSet ds1 = env.createInput(
          JDBCInputFormat.buildJDBCInputFormat()
              .setDrivername(BenchMarkUtil.DB_DRIVER)
              .setDBUrl(BenchMarkUtil.DB_CONNECTION_URL)
              .setQuery(fetchSql1)
              .setRowTypeInfo(rowTypeInfo1)
              .finish()
      );
      ds1.collect();
      tEnv.registerDataSet("item1", ds1);

      String fetchSql2 = BenchMarkUtil.generateFetchSql("item2", "i_item_sk", limit);
      ResultSet resultSet2 = connection
          .createStatement()
          .executeQuery(fetchSql2);
      RowTypeInfo rowTypeInfo2 = typeOfJdbc(resultSet2.getMetaData());
      DataSet ds2 = env.createInput(
          JDBCInputFormat.buildJDBCInputFormat()
              .setDrivername(BenchMarkUtil.DB_DRIVER)
              .setDBUrl(BenchMarkUtil.DB_CONNECTION_URL)
              .setQuery(fetchSql2)
              .setRowTypeInfo(rowTypeInfo2)
              .finish()
      );
      ds2.collect();
      tEnv.registerDataSet("item2", ds2);
      s.stop();
      return s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(tEnv, sql);
    }
  }


  @Test
  public void testProjection() throws Throwable {
    String sql = "SELECT * FROM  item1";
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
    String sql = "SELECT t1.i_item_sk,t2.i_item_sk  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <10000";
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

// End FlinkCollectionsEnvBenchMark.java
