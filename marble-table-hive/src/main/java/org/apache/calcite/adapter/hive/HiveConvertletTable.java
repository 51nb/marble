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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql2rel.SqlRexContext;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.math.BigDecimal;

/**
 *
 */
public class HiveConvertletTable extends StandardConvertletTable {

  public HiveConvertletTable() {
    super();
    registerOp(HiveSqlOperatorTable.CAST, this::convertCast);
  }

  @Override protected RexNode convertCast(SqlRexContext cx,
      SqlCall call) {
    RelDataTypeFactory typeFactory = cx.getTypeFactory();
    assert call.getKind() == SqlKind.CAST;
    final SqlNode left = call.operand(0);
    final SqlNode right = call.operand(1);
    if (right instanceof SqlIntervalQualifier) {
      final SqlIntervalQualifier intervalQualifier =
          (SqlIntervalQualifier) right;
      if (left instanceof SqlIntervalLiteral) {
        RexLiteral sourceInterval =
            (RexLiteral) cx.convertExpression(left);
        BigDecimal sourceValue =
            (BigDecimal) sourceInterval.getValue();
        RexLiteral castedInterval =
            cx.getRexBuilder().makeIntervalLiteral(sourceValue,
                intervalQualifier);
        return castToValidatedType(cx, call, castedInterval);
      } else if (left instanceof SqlNumericLiteral) {
        RexLiteral sourceInterval =
            (RexLiteral) cx.convertExpression(left);
        BigDecimal sourceValue =
            (BigDecimal) sourceInterval.getValue();
        final BigDecimal multiplier = intervalQualifier.getUnit().multiplier;
        sourceValue = sourceValue.multiply(multiplier);
        RexLiteral castedInterval =
            cx.getRexBuilder().makeIntervalLiteral(
                sourceValue,
                intervalQualifier);
        return castToValidatedType(cx, call, castedInterval);
      }
      return castToValidatedType(cx, call, cx.convertExpression(left));
    }
    SqlDataTypeSpec dataType = (SqlDataTypeSpec) right;
    //considering hive cast: select cast('' as double) will return null,
    // so we make a cast call always be nullable
    RelDataType type = dataType.deriveType(typeFactory, true);
    if (SqlUtil.isNullLiteral(left, false)) {
      final SqlValidatorImpl validator = (SqlValidatorImpl) cx.getValidator();
      validator.setValidatedNodeType(left, type);
      return cx.convertExpression(left);
    }
    RexNode arg = cx.convertExpression(left);
    if (type == null) {
      type = cx.getValidator().getValidatedNodeType(dataType.getTypeName());
    }
    if (arg.getType().isNullable()) {
      type = typeFactory.createTypeWithNullability(type, true);
    }
    if (null != dataType.getCollectionsTypeName()) {
      final RelDataType argComponentType =
          arg.getType().getComponentType();
      final RelDataType componentType = type.getComponentType();
      if (argComponentType.isStruct()
          && !componentType.isStruct()) {
        RelDataType tt =
            typeFactory.builder()
                .add(
                    argComponentType.getFieldList().get(0).getName(),
                    componentType)
                .build();
        tt = typeFactory.createTypeWithNullability(
            tt,
            componentType.isNullable());
        boolean isn = type.isNullable();
        type = typeFactory.createMultisetType(tt, -1);
        type = typeFactory.createTypeWithNullability(type, isn);
      }
    }
    return cx.getRexBuilder().makeCast(type, arg);
  }
}

// End HiveConvertletTable.java
