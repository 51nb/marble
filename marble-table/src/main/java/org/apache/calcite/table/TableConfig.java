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

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;

import java.util.TimeZone;

/**
 * A config to define the runtime behavior of the Table API.
 */
public class TableConfig {

  private TimeZone timeZone = TimeZone.getTimeZone("UTC");
  private SqlParser.Config sqlParserConfig;
  private SqlOperatorTable sqlOperatorTable;
  private CalciteConnectionConfig calciteConnectionConfig;
  private RelDataTypeSystem relDataTypeSystem;
  private SqlRexConvertletTable convertletTable;
  private RexExecutor rexExecutor;


  public RexExecutor getRexExecutor() {
    return rexExecutor;
  }

  public void setRexExecutor(RexExecutor rexExecutor) {
    this.rexExecutor = rexExecutor;
  }

  public SqlRexConvertletTable getConvertletTable() {
    return convertletTable;
  }

  public void setConvertletTable(
      SqlRexConvertletTable convertletTable) {
    this.convertletTable = convertletTable;
  }

  public RelDataTypeSystem getRelDataTypeSystem() {
    return relDataTypeSystem;
  }

  public void setRelDataTypeSystem(
      RelDataTypeSystem relDataTypeSystem) {
    this.relDataTypeSystem = relDataTypeSystem;
  }

  public TimeZone getTimeZone() {
    return timeZone;
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public SqlParser.Config getSqlParserConfig() {
    return sqlParserConfig;
  }

  public void setSqlParserConfig(
      SqlParser.Config sqlParserConfig) {
    this.sqlParserConfig = sqlParserConfig;
  }

  public SqlOperatorTable getSqlOperatorTable() {
    return sqlOperatorTable;
  }

  public void setSqlOperatorTable(
      SqlOperatorTable sqlOperatorTable) {
    this.sqlOperatorTable = sqlOperatorTable;
  }

  public CalciteConnectionConfig getCalciteConnectionConfig() {
    return calciteConnectionConfig;
  }

  public void setCalciteConnectionConfig(
      CalciteConnectionConfig calciteConnectionConfig) {
    this.calciteConnectionConfig = calciteConnectionConfig;
  }
}

// End TableConfig.java
