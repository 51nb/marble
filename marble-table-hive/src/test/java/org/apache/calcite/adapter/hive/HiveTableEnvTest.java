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
package org.apache.calcite.adapter.hive;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.table.DataTable;
import org.apache.calcite.table.TableEnv;

import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 */
public class HiveTableEnvTest {

  static final TableEnv tableEnv = HiveTableEnv.getTableEnv();

  static {
    TableEnv.enableSqlPlanCacheSize(200);
  }

  private static final String DB_DRIVER = "org.h2.Driver";
  private static final String DB_CONNECTION_URL = "jdbc:h2:mem:test;"
      + "DB_CLOSE_DELAY=-1";
  private static final String DB_USER = "";
  private static final String DB_PASSWORD = "";

  public static Connection getDBConnection() throws Exception {
    Class.forName(DB_DRIVER);
    return DriverManager.getConnection(DB_CONNECTION_URL, DB_USER, DB_PASSWORD);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    try (Connection connection = getDBConnection()) {
      Statement stmt = connection.createStatement();
      stmt.execute(
          "CREATE TABLE t1(c1 BIGINT,c2 INT,c3 DECIMAL,c4 DOUBLE ,c5 VARCHAR)");
      stmt.execute(
          "INSERT INTO t1(c1,c2,c3,c4,c5) VALUES(1,2,3.1415,0.1,'a,b,c')");
      stmt.execute(
          "INSERT INTO t1(c1,c2,c3,c4,c5) VALUES(2,4,3.1415,0.2,'a,b')");
      stmt.execute(
          "CREATE TABLE t2(c1 VARCHAR,c2 INT,c3 DECIMAL,c4 DOUBLE ,c5 "
              + "VARCHAR)");
      stmt.execute(
          "INSERT INTO t2(c1,c2,c3,c4,c5) VALUES('1',2,3.1415,0.1,'a')");
      stmt.execute(
          "INSERT INTO t2(c1,c2,c3,c4,c5) VALUES('2',4,3.1415,0.2,'b')");
      DataTable t1 = tableEnv.fromJdbcResultSet(
          stmt.executeQuery("select * from t1"));
      tableEnv.registerTable("t1", t1);
      DataTable t2 = tableEnv.fromJdbcResultSet(
          stmt.executeQuery("select * from t2"));
      tableEnv.registerTable("t2", t2);
      stmt.close();
    }
  }

  private List<Map<String, Object>> executeSql(String sql) {
    List<Map<String, Object>> rt = tableEnv.sqlQuery(sql).toMapList();
    System.out.println(rt);
    return rt;
  }

  /**
   * hive functions tests ,see the manual:
   * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF
   */

  //Arithmetic Operators
  @Test public void testDivide() {
    Assert.assertTrue(
        (Double) executeSql("select  2/5 as c1").get(0).get("c1") == 0.4);
  }

  @Test public void testMod() {
    Assert.assertTrue(
        (Integer) executeSql("select  5%3 as c1").get(0).get("c1") == 2);
  }

  //Logical Operators
  @Test public void testIn() {
    String sql = "select  count(*) as cnt from t1 where c1 in(1,2)";
    Assert.assertTrue(
        (long) executeSql(sql).get(0).get("cnt") == 2);
  }

  //Relational Operators
  @Test public void testEquals() {
    //hive implicit cast: string=>double long=>double
    String sql = "select  * from t1 where c1 ='1' ";
    Assert.assertTrue(executeSql(sql).size() > 0);
  }

  @Test public void testGreatThan() {
    String sql = "select  * from t1 where c1 >'1' ";
    Assert.assertTrue(executeSql(sql).size() > 0);
  }

  //String Functions
  @Test public void testRlike() {
    Assert.assertEquals(1, executeSql(
        "select case when 'a1' rlike '^[abcde]?[12345]$'   then 1 else 0 end "
            + "as c1")
        .get(0)
        .get("c1"));
    Assert.assertEquals(0, executeSql(
        "select case when 'a1' not rlike '^[abcde]?[12345]$'   then 1 else 0 "
            + "end "
            + "as c1")
        .get(0)
        .get("c1"));
    Assert.assertEquals(1, executeSql(
        "select case when 'a1' regexp '^[abcde]?[12345]$'   then 1 else 0 end "
            + "as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testSubString() {
    Assert.assertEquals("b", executeSql(
        "select substring('Facebook', 5, 1) as c1")
        .get(0)
        .get("c1"));
    Assert.assertEquals("b", executeSql(
        "select substr('Facebook', 5, 1) as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testTrim() {
    Assert.assertEquals("foobar", executeSql(
        "select trim(' foobar ') as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testConcat() {
    Assert.assertEquals("foobar", executeSql(
        "select concat('foo', 'bar') as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testGetJsonObject() {
    Assert.assertEquals("{\"weight\":8,\"type\":\"apple\"}", executeSql(
        "select get_json_object(" + "'{"
            + " \"store\":\n"
            + "        {\n"
            + "         \"fruit\":[{\"weight\":8,\"type\":\"apple\"}, "
            + "{\"weight\":9,\"type\":\"pear\"}],  \n"
            + "         \"bicycle\":{\"price\":19.95,\"color\":\"red\"}\n"
            + "         }, \n"
            + " \"email\":\"amy@only_for_json_udf_test.net\", \n"
            + " \"owner\":\"amy\" \n"
            + "}'" + ", '$.store.fruit[0]') as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testRegexp() {
    Assert.assertEquals("bar", executeSql(
        "select  regexp_extract('foothebar', 'foo(.*?)(bar)', 2) as c1")
        .get(0)
        .get("c1"));
    Assert.assertEquals("fb", executeSql(
        "select  regexp_replace('foobar', 'oo|ar', '') as c1")
        .get(0)
        .get("c1"));
  }

  @Test public void testSplit() {
    Assert.assertEquals("a", executeSql(
        "select  split('a,b,c',',')[0] as c1")
        .get(0)
        .get("c1"));
  }

  //Type Conversion Functions
  @Test public void testCastNull() {
    //hive will return null ,but calcite will throw exception
    String sql = "select cast('' as double) as a ,1 as b";
    Assert.assertEquals(null, executeSql(sql).get(0).get("a"));
    String sql1 = "select cast('0.1' as Double) as a,1 as b";
    Assert.assertEquals(0.1, executeSql(sql1).get(0).get("a"));
    String sql2 = "select cast('0.1' as decimal) as a";
    Assert.assertEquals(BigDecimal.valueOf(0.1),
        executeSql(sql2).get(0).get("a"));
  }

  //Date Functions
  @Test public void testUnixTime() {
    Assert.assertEquals("2012012000",
        executeSql("select from_unixtime(1326988805,'yyyyMMddHH') as c1").get(0)
            .get("c1"));
    Assert.assertEquals(1237478400L,
        executeSql(
            "select unix_timestamp('2009-03-20', 'yyyy-MM-dd')  as c1").get(0)
            .get("c1"));
  }

  @Test public void testDate() {
    Assert.assertEquals(
        "1970-01-01",
        executeSql("select to_date('1970-01-01 00:00:00') as c1").get(0)
            .get("c1"));
    Assert.assertEquals(
        2,
        executeSql("select datediff('2009-03-01', '2009-02-27') as c1").get(0)
            .get("c1"));
    Assert.assertEquals(
        "2009-01-01",
        executeSql("select date_add('2008-12-31', 1) as c1").get(0)
            .get("c1"));
    Assert.assertEquals(
        "2008-12-30",
        executeSql("select  date_sub('2008-12-31', 1) as c1").get(0)
            .get("c1"));
  }

  @Test public void testCurrentDate() {
    executeSql("select current_date");
    Assert.assertEquals(
        SqlFunctions.toInt(java.sql.Date.valueOf(
            SessionState.get()
                .getQueryCurrentTimestamp()
                .toString()
                .substring(0, 10)), TimeZone.getTimeZone("UTC")),
        executeSql("select current_date as c1").get(0)
            .get("c1"));
  }

  //Conditional Functions
  @Test public void testIf() {
    //TODO:select if(1<0,0,null)  is not allowed in Calcite
    //see https://issues.apache.org/jira/browse/CALCITE-1531
    Assert.assertEquals(null,
        executeSql("select if(1<0,0,cast(null as decimal)) as c1").get(0)
            .get("c1"));
  }

  //UDAF
  @Test public void testCount() {
    String sql = "select count(*) from t1 ";
    Assert.assertTrue(
        executeSql(sql).size() > 0);
  }

  @Test public void testSum() {
    String sql = "select sum(c4) as c1 from t1 where c4>0";
    Assert.assertTrue(
        ((Double) executeSql(sql).get(0).get("c1"))
            >= 0.3);
  }

  @Test public void testMax() {
    String sql = "select max(c4) as c1 from t1 ";
    Assert.assertTrue(
        ((Double) executeSql(sql).get(0).get("c1"))
            == 0.2);
  }

  @Test public void testPercentileApprox() {
    String sql = "select percentile_approx(c2,0.8)  as c1 from t1 ";
    Assert.assertTrue(
        (Double) executeSql(sql).get(0).get("c1") > 2);
  }

  //window agg function
  @Test public void testRowNumber() {
    String sql =
        "select *,row_number() over(partition by c3 order by c2 ) as r0"
            + "   from t1 ";
    executeSql(sql);
  }

  @Test public void testFirstValue() {
    String sql =
        "select *,first_value(c1) over(partition by c3 order by c2 ) as"
            + " f0  from t1 ";
    executeSql(sql);
  }

  @Test public void testRank() {
    String sql = "select *,rank() over(partition by c3 order by c2 ) as r0 "
        + "  from t1 ";
    executeSql(sql);
  }

  @Test public void testLead() {
    String sql = "select lead(c4,1,10.0)  over(order by c4) as r0 from t1 ";
    executeSql(sql);
  }

  //Table Function
  @Test public void testLateralTable() {
    String query = "select t1.* ,t2.n1 from t1,LATERAL TABLE(explode(split"
        + "(t1.c5,','))) as t2(n1) ";
    DataTable queryResult = tableEnv.sqlQuery(query);
    Assert.assertTrue(queryResult.toMapList().size() == 3 + 2);
  }

  /**
   * Theta outer Join with equi conditions
   */
  @Test public void testThetaOuterJoinWithEquiCondition() {
    String query =
        "select t1.*,t2.*  from t1 left outer join t2 on t1.c1=t2.c1 "
            + "and t1.c1<2 ";
    DataTable queryResult = tableEnv.sqlQuery(query);
    Assert.assertTrue(queryResult.toMapList().size() == 2);
  }
}

// End HiveTableEnvTest.java
