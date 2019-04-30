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


import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.NewArrayExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.table.TypeConvertUtil;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hive UDF Implementor
 */
public class HiveUDFImplementor implements NotNullImplementor {


  @Override public Expression implement(RexToLixTranslator translator,
      RexCall call, List<Expression> translatedOperands) {
    try {
      SqlOperator operator = call.getOperator();
      Expression opNameExpr = new ConstantExpression(String.class,
          operator.getName());
      Expression syntaxExpr = new ConstantExpression(SqlSyntax.class,
          operator.getSyntax());
      Method newGenericUDFMethod = HiveUDFImplementor.class.getMethod(
          "newGenericUDF", String.class, SqlSyntax.class);
      Expression createUdfInstanceExpr = Expressions.call(newGenericUDFMethod,
          opNameExpr,
          syntaxExpr);

      String udfInstanceName =
          "udfInstance_" + HiveUDFInstanceCollecterPerSqlQuery.get()
              .getSizeOfStashedHiveUDFInstance();
      HiveUDFInstanceCollecterPerSqlQuery.get()
          .incrementSizeOfStashedHiveUDFInstance();

      ParameterExpression udfInstanceVariableExpr = Expressions
          .parameter(Types.of(GenericUDF.class, Object.class),
              "hiveUDFInstanceHolder." + udfInstanceName);

      //stashed a field MemberDeclaration for a udf instance
      MemberDeclaration udfMemberDeclaration = Expressions.fieldDecl(
          Modifier.PUBLIC,
          Expressions
              .parameter(Types.of(GenericUDF.class, Object.class),
                  udfInstanceName),
          createUdfInstanceExpr);
      HiveUDFInstanceCollecterPerSqlQuery.get()
          .getStashedFieldsForHiveUDFInstanceHolder().add(udfMemberDeclaration);

      Expression argsExpr = new NewArrayExpression(Object.class, 1, null,
          translatedOperands);
      Method callGenericUDFMethod = HiveUDFImplementor.class.getMethod(
          "callGenericUDF", GenericUDF.class,
          Object[].class, RelDataTypeHolder[].class);

      List<Expression> argsType = IntStream.range(0, call.operands.size())
          .mapToObj(index -> {
            RexNode rexNode = call.getOperands().get(index);
            Expression argExp = translatedOperands.get(index);
            if (argExp instanceof ConstantExpression) {
              return RelDataTypeHolder.generateExpressionWithConstantValue(
                  rexNode.getType(),
                  argExp);
            } else {
              return RelDataTypeHolder.generateExpression(rexNode.getType());
            }
          })
          .collect(Collectors.toList());
      Expression argTypeArrayExpr = new NewArrayExpression(
          RelDataTypeHolder.class, 1,
          null, argsType);

      Expression callExpr = Expressions.call(callGenericUDFMethod,
          Arrays.asList(udfInstanceVariableExpr, argsExpr, argTypeArrayExpr));
      String castMethodName =
          TypeConvertUtil.CALCITE_SQL_TYPE_2_CAST_METHOD.get(
              call.type.getSqlTypeName());
      Method castMethod = TypeConvertUtil.class.getMethod(castMethodName,
          Object.class);
      Expression castExpr = Expressions.call(castMethod, callExpr);
      return castExpr;

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public static GenericUDF newGenericUDF(String opName,
      SqlSyntax syntax) {
    if (opName.equals("NOT RLIKE")) {
      //we use a RexImpTable.NotImplementor to wrapper a HiveUDFImplementor ,
      // so `NOT RLIKE` and `RLIKE` would be treated as same here
      opName = "RLIKE";
    }
    if (opName.equals("NOT REGEXP")) {
      opName = "REGEXP";
    }
    Class hiveUDFClazz = HiveSqlOperatorTable.instance()
        .getHiveUDFClass(opName, syntax);
    if (GenericUDF.class.isAssignableFrom(hiveUDFClazz)) {
      try {
        return (GenericUDF) hiveUDFClazz.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException(
            "fail to new instance for class " + hiveUDFClazz, e);
      }
    } else if (UDF.class.isAssignableFrom(hiveUDFClazz)) {
      return new GenericUDFBridge(opName, false, hiveUDFClazz.getName());
    } else {
      throw new IllegalArgumentException("unknown hive udf class for opName="
          + opName
          + ",and syntax="
          + syntax);
    }
  }


  public static Object callGenericUDF(GenericUDF udfInstance, Object[] args,
      RelDataTypeHolder[] argsType) {
    try {
      ObjectInspector[] inputObjectInspector =
          TypeInferenceUtil.getObjectInspector(
              argsType);
      ObjectInspector outputObjectInspector = udfInstance.initialize(
          inputObjectInspector);
      GenericUDF.DeferredJavaObject[] deferredJavaObjectArray =
          new GenericUDF.DeferredJavaObject[args.length];
      for (int i = 0; i < args.length; i++) {
        deferredJavaObjectArray[i] = new GenericUDF.DeferredJavaObject(
            TypeInferenceUtil.convertCalciteObject2HiveWritableObject(
                argsType[i].getSqlTypeName(), args[i]));
      }
      Object result = udfInstance.evaluate(
          deferredJavaObjectArray);
      return TypeInferenceUtil.convertHiveObject2CalciteObject(
          outputObjectInspector,
          result);
    } catch (Exception e) {
      throw new RuntimeException("call hive udf error", e);
    }
  }

}

// End HiveUDFImplementor.java
