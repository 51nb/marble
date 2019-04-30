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
package com.u51.marble

import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.junit.{BeforeClass, Test}


/**
  *
  */

object SparkLocalBenchMark {
  @BeforeClass
  def setup(): Unit = {
    BenchMarkUtil.initDBTables()
  }
}

class SparkLocalBenchMark {

  val SPARK_WORK_DIR = "/tmp/spark"
  var spark: SparkSession = new SparkSession.Builder().appName("spark-local-benchmark")
      .config("spark.ui.enabled", false)
      .config("spark.sql.warehouse.dir", SPARK_WORK_DIR)
      .config("hive.exec.scratchdir", SPARK_WORK_DIR + "/hive")
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.default.parallelism", 1)
      .master("local[1]")
      .getOrCreate()


  def sqlQuery(sql: String): Double = {
    val s = Stopwatch.createStarted
    val resultTable = spark.sql(sql).collect()
    System.out.println("sqlQuery result table size:" + resultTable.length)
    s.stop
    s.elapsed(TimeUnit.MICROSECONDS) * 0.001
  }

  val connectOptions = Map(
    "url" -> BenchMarkUtil.DB_CONNECTION_URL,
    "user" -> BenchMarkUtil.DB_USER,
    "password" -> BenchMarkUtil.DB_PASSWORD,
    "driver" -> BenchMarkUtil.DB_DRIVER)

  def runSqlForSingleTable(limit: Int, sql: String): Double = {
    System.out.println("start runSqlForSingleTable")
    val s = Stopwatch.createStarted
    spark = spark.newSession()
    spark.sqlContext.clearCache()
    val fetchSql = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit)
    spark.read.format("jdbc")
        .options(connectOptions)
        .option("dbtable", s"($fetchSql) tmp ")
        .load().persist(StorageLevel.MEMORY_ONLY).createOrReplaceTempView("item1")
    s.stop
    s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(sql)

  }

  def runSqlForJoin(limit: Int, sql: String): Double = {
    System.out.println("start runSqlForJoin")
    val s = Stopwatch.createStarted
    spark = spark.newSession()
    spark.sqlContext.clearCache()
    val fetchSql1 = BenchMarkUtil.generateFetchSql("item1", "i_item_sk", limit)
    spark.read.format("jdbc")
        .options(connectOptions)
        .option("dbtable", s"($fetchSql1) tmp ")
        .load().persist(StorageLevel.MEMORY_ONLY).createOrReplaceTempView("item1")
    val fetchSql2 = BenchMarkUtil.generateFetchSql("item2", "i_item_sk", limit)
    spark.read.format("jdbc")
        .options(connectOptions)
        .option("dbtable", s"($fetchSql2) tmp ")
        .load().persist(StorageLevel.MEMORY_ONLY).createOrReplaceTempView("item2")
    s.stop
    s.elapsed(TimeUnit.MICROSECONDS) * 0.001 + sqlQuery(sql)
  }

  @Test
  def testProjection(): Unit = {
    val sql = "SELECT * FROM  item1"
    runSqlForSingleTable(1, sql)
    System.out.println(runSqlForSingleTable(200, sql))
    System.out.println(runSqlForSingleTable(2000, sql))
    System.out.println(runSqlForSingleTable(4000, sql))
    System.out.println(runSqlForSingleTable(10000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))

  }

  @Test
  def testEquiJoin(): Unit = {
    val sql = "SELECT t1.*,t2.*,substring('abc',1,2)  FROM item1 t1 INNER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk"
    runSqlForJoin(1, sql)
    System.out.println(runSqlForJoin(200, sql))
    System.out.println(runSqlForJoin(2000, sql))
    System.out.println(runSqlForJoin(4000, sql))
    System.out.println(runSqlForJoin(10000, sql))
    System.out.println(runSqlForJoin(15000, sql))

  }

  @Test
  def testThetaJoin(): Unit = {
    val sql = "SELECT t1.i_item_sk,t2.i_item_sk  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000"
    runSqlForJoin(1, sql)
    System.out.println(runSqlForJoin(200, sql))
    System.out.println(runSqlForJoin(2000, sql))
    System.out.println(runSqlForJoin(4000, sql))
    System.out.println(runSqlForJoin(10000, sql))
    System.out.println(runSqlForJoin(15000, sql))

  }

  @Test
  def testAggregate(): Unit = {
    val sql = "SELECT count(*) ,sum(i_current_price),max(i_current_price)  FROM item1 group by i_item_id "
    runSqlForSingleTable(1, sql)
    System.out.println(runSqlForSingleTable(2000, sql))
    System.out.println(runSqlForSingleTable(4000, sql))
    System.out.println(runSqlForSingleTable(10000, sql))
    System.out.println(runSqlForSingleTable(15000, sql))
  }

  @Test
  def testComplexSql(): Unit = {
    val sql = "SELECT t4.* FROM (SELECT t3.i_item_id,count(t3.*) as count0 ,sum(t3.i_current_price) as sum0,max(t3.i_current_price) as max0  FROM (SELECT t1.*  FROM item1 t1 LEFT OUTER JOIN item2 t2 ON t1.i_item_sk=t2.i_item_sk and t1.i_item_sk <15000) t3 group by t3.i_item_id ) t4 where t4.count0>2"
    runSqlForJoin(1, sql)
    System.out.println(runSqlForJoin(15000, sql))
  }

}

// End SparkLocalBenchMark.java
