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

import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * hold a RelDataType. Since some Hive UDFs will check if a parameter is
 * constant,a RelDataTypeHolder will hold the info of constant value.
 */
public class RelDataTypeHolder {

  private SqlTypeName sqlTypeName;

  private RelDataTypeHolder componentType;

  private boolean isConstant;

  private Object value;


  public RelDataTypeHolder(RelDataType relDataType, boolean isConstant,
      Object value) {
    this.sqlTypeName = relDataType.getSqlTypeName();
    RelDataType componentType = relDataType.getComponentType();
    if (componentType != null) {
      this.componentType = new RelDataTypeHolder(componentType);
    }
    this.isConstant = isConstant;
    this.value = value;
  }

  public RelDataTypeHolder(RelDataType relDataType) {
    this(relDataType, false, null);
  }

  public RelDataTypeHolder(SqlTypeName sqlTypeName) {
    this.sqlTypeName = sqlTypeName;
  }

  public RelDataTypeHolder(SqlTypeName sqlTypeName,
      final RelDataTypeHolder componentType) {
    this.sqlTypeName = sqlTypeName;
    this.componentType = componentType;
  }

  public RelDataTypeHolder(SqlTypeName sqlTypeName, boolean isConstant,
      Object value) {
    this.sqlTypeName = sqlTypeName;
    this.isConstant = isConstant;
    this.value = value;
  }

  public SqlTypeName getSqlTypeName() {
    return sqlTypeName;
  }

  public RelDataTypeHolder getComponentType() {
    return componentType;
  }

  public boolean isConstant() {
    return isConstant;
  }

  public Object getValue() {
    return value;
  }


  public static  Expression generateExpression(RelDataType relDataType) {
    SqlTypeName sqlTypeName = relDataType.getSqlTypeName();
    RelDataType componentType = relDataType.getComponentType();
    if (componentType == null) {
      return Expressions.new_(RelDataTypeHolder.class,
          new ConstantExpression(SqlTypeName.class, sqlTypeName));
    } else {
      Expression componentTypeExpr = generateExpression(componentType);
      return Expressions.new_(RelDataTypeHolder.class,
          new ConstantExpression(SqlTypeName.class, sqlTypeName),
          componentTypeExpr);
    }
  }


  public static Expression generateExpressionWithConstantValue(RelDataType relDataType,
      Expression valueExpression) {
    return Expressions.new_(RelDataTypeHolder.class,
        new ConstantExpression(SqlTypeName.class,
            relDataType.getSqlTypeName()),
        new ConstantExpression(boolean.class, true),
        valueExpression);
  }

}

// End RelDataTypeHolder.java
