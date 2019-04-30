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


import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class BenchMarkUtil {

  private BenchMarkUtil() {
  }

  public static final String DB_DRIVER = "org.h2.Driver";
  public static final String DB_CONNECTION_URL = "jdbc:h2:mem:test;"
      + "DB_CLOSE_DELAY=-1";
  public static final String DB_USER = "";
  public static final String DB_PASSWORD = "";

  public static Connection getDBConnection() throws Exception {
    Class.forName(DB_DRIVER);
    return DriverManager.getConnection(DB_CONNECTION_URL, DB_USER, DB_PASSWORD);
  }

  private static final String TABLE_ITEM1_DDL_SQL = "CREATE TABLE item1\n" +
      "(\n" +
      "    i_item_sk                 BIGINT,\n" +
      "    i_item_id                 VARCHAR(255),\n" +
      "    i_rec_start_date          VARCHAR(255),\n" +
      "    i_rec_end_date            VARCHAR(255),\n" +
      "    i_item_desc               VARCHAR(255),\n" +
      "    i_current_price           DOUBLE,\n" +
      "    i_wholesale_cost          DOUBLE,\n" +
      "    i_brand_id                INT,\n" +
      "    i_brand                   VARCHAR(255),\n" +
      "    i_class_id                INT,\n" +
      "    i_class                   VARCHAR(255),\n" +
      "    i_category_id             INT,\n" +
      "    i_category                VARCHAR(255),\n" +
      "    i_manufact_id             INT,\n" +
      "    i_manufact                VARCHAR(255),\n" +
      "    i_size                    VARCHAR(255),\n" +
      "    i_formulation             VARCHAR(255),\n" +
      "    i_color                   VARCHAR(255),\n" +
      "    i_units                   VARCHAR(255),\n" +
      "    i_container               VARCHAR(255),\n" +
      "    i_manager_id              INT,\n" +
      "    i_product_name            VARCHAR(255)\n" +
      ") AS SELECT * FROM CSVREAD('classpath:table_item_data.csv',NULL, "
      + "'charset=UTF-8 fieldSeparator=|')";

  private static final String TABLE_ITEM2_DDL_SQL = "CREATE TABLE item2\n" +
      "(\n" +
      "    i_item_sk                 BIGINT,\n" +
      "    i_item_id                 VARCHAR(255),\n" +
      "    i_rec_start_date          VARCHAR(255),\n" +
      "    i_rec_end_date            VARCHAR(255),\n" +
      "    i_item_desc               VARCHAR(255),\n" +
      "    i_current_price           DOUBLE,\n" +
      "    i_wholesale_cost          DOUBLE,\n" +
      "    i_brand_id                INT,\n" +
      "    i_brand                   VARCHAR(255),\n" +
      "    i_class_id                INT,\n" +
      "    i_class                   VARCHAR(255),\n" +
      "    i_category_id             INT,\n" +
      "    i_category                VARCHAR(255),\n" +
      "    i_manufact_id             INT,\n" +
      "    i_manufact                VARCHAR(255),\n" +
      "    i_size                    VARCHAR(255),\n" +
      "    i_formulation             VARCHAR(255),\n" +
      "    i_color                   VARCHAR(255),\n" +
      "    i_units                   VARCHAR(255),\n" +
      "    i_container               VARCHAR(255),\n" +
      "    i_manager_id              INT,\n" +
      "    i_product_name            VARCHAR(255)\n" +
      ") AS SELECT * FROM CSVREAD('classpath:table_item_data.csv',NULL, "
      + "'charset=UTF-8 fieldSeparator=|')";

  public static void initDBTables() throws Exception {
    try (Connection connection = getDBConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute(TABLE_ITEM1_DDL_SQL);
      stmt.execute(TABLE_ITEM2_DDL_SQL);
      stmt.close();
    }

  }

  public static String generateFetchSql(String tableName, String orderBy,
                                        int limit) {
    return "SELECT * FROM "
        + tableName
        + " ORDER BY "
        + orderBy
        + " ASC LIMIT "
        + limit;
  }

  public static List<Map<String, Object>> toMapList(ResultSet resultSet)
      throws Exception {
    List<Map<String, Object>> rt = new ArrayList<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      Map<String, Object> map = new HashMap<>();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnLabel(i);
        Object jdbcObject = resultSet.getObject(i);
        map.put(columnName, jdbcObject);
      }
      rt.add(map);
    }
    return rt;
  }


  public static void main(String[] arg) throws Exception {
    initDBTables();
    try (Connection connection = getDBConnection()) {
      Statement stmt = connection.createStatement();
      ResultSet resultSet1 = stmt.executeQuery(
          "SELECT COUNT(*) FROM item1");
      print(toMapList(resultSet1));
      ResultSet resultSet2 = stmt.executeQuery(
          "SELECT * FROM item1 ORDER BY i_item_sk ASC LIMIT 10");
      print(toMapList(resultSet2));
      ResultSet resultSet3 = stmt.executeQuery("SELECT t1.*  FROM (SELECT * FROM item1 ORDER BY i_item_sk ASC LIMIT 10 ) t1 LEFT OUTER JOIN (SELECT * FROM item2 ORDER BY i_item_sk ASC LIMIT 10) t2 ON t1.i_item_sk=t2.i_item_sk AND t1.i_item_sk<3");
      print(toMapList(resultSet3));
      stmt.close();
    }

  }

  private static void print(List<Map<String, Object>> rt) {
    System.out.println();
    rt.forEach(o -> System.out.println(o));
    System.out.println();
  }
}

// End BenchMarkUtil.java
