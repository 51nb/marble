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

import org.apache.calcite.adapter.enumerable.AggAddContext;
import org.apache.calcite.adapter.enumerable.AggContext;
import org.apache.calcite.adapter.enumerable.AggResetContext;
import org.apache.calcite.adapter.enumerable.AggResultContext;
import org.apache.calcite.adapter.enumerable.StrictAggImplementor;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewArrayExpression;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.table.TypeConvertUtil;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Hive UDAF Implementor
 */
public class HiveUDAFImplementor extends StrictAggImplementor {

  public static final Method METHOD_EXECUTE_ITERATE;
  public static final Method METHOD_EXECUTE_TERMINATE;

  static {
    try {
      METHOD_EXECUTE_ITERATE = HiveUDAFImplementor.class.getMethod(
          "executeIterate",
          Class.class,
          HiveUDAFParticipator.class, Object[].class,
          RelDataTypeHolder[].class, boolean.class);
      METHOD_EXECUTE_TERMINATE = HiveUDAFImplementor.class.getMethod(
          "executeTerminate",
          HiveUDAFParticipator.class);
    } catch (NoSuchMethodException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private HiveSqlAggFunction hiveSqlAggFunction;

  public HiveUDAFImplementor(HiveSqlAggFunction hiveSqlAggFunction) {
    this.hiveSqlAggFunction = hiveSqlAggFunction;
  }

  @Override public List<Type> getNotNullState(AggContext info) {
    return Arrays.asList(HiveUDAFParticipator.class);
  }

  @Override public void implementNotNullReset(AggContext info,
      AggResetContext context) {

    Expression acc0 = context.accumulator().get(0);
    Expression participatorExpr = Expressions.new_(HiveUDAFParticipator.class);
    Expression assignExpr = Expressions.assign(acc0, participatorExpr);
    context.currentBlock().add(Expressions.statement(assignExpr));

  }

  @Override public void implementNotNullAdd(AggContext info,
      AggAddContext context) {
    Class hiveUDAFClass =
        HiveSqlOperatorTable.instance()
            .getHiveUDAFClass(hiveSqlAggFunction.getName());
    Expression hiveUDAFClassExpr = Expressions.constant(hiveUDAFClass,
        Class.class);
    Expression acc0 = context.accumulator().get(0);
    Expression argArray = new NewArrayExpression(Object.class, 1, null,
        context.arguments());
    List<Expression> list = new ArrayList<>();
    for (int i = 0; i < context.rexArguments().size(); i++) {
      Expression argExpr;
      if (TypeInferenceUtil.isOperandConstantForHiveUDAF(hiveUDAFClass, i)) {
        Expression valExpr = ((NewArrayExpression) argArray).expressions.get(i);
        argExpr = RelDataTypeHolder.generateExpressionWithConstantValue(
            context.rexArguments().get(i).getType(),
            valExpr);

      } else {
        argExpr = RelDataTypeHolder.generateExpression(
            context.rexArguments().get(i).getType());
      }
      list.add(argExpr);
    }
    Expression argTypeArray = new NewArrayExpression(RelDataTypeHolder.class, 1,
        null, list);
    boolean isDistinct = info.call().isDistinct();
    Expression iterateExpr = Expressions.call(METHOD_EXECUTE_ITERATE,
        hiveUDAFClassExpr, acc0,
        argArray, argTypeArray,
        new ConstantExpression(Boolean.class, isDistinct));
    context.currentBlock().add(Expressions.statement(iterateExpr));

  }

  @Override public Expression implementNotNullResult(AggContext info,
      AggResultContext context) {
    Expression acc0 = context.accumulator().get(0);
    Expression resultExpr = Expressions.call(METHOD_EXECUTE_TERMINATE, acc0);
    Method castMethod;
    try {
      SqlTypeName resultType = info.returnRelType().getSqlTypeName();
      String castMethodName =
          TypeConvertUtil.CALCITE_SQL_TYPE_2_CAST_METHOD
              .get(resultType);
      castMethod = TypeConvertUtil.class.getMethod(castMethodName,
          Object.class);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
    Expression castExpr = Expressions.call(castMethod, resultExpr);
    return castExpr;
  }


  public static void executeIterate(Class hiveUDAFClass,
      HiveUDAFParticipator hiveUDAFParticipator,
      Object[] params, RelDataTypeHolder[] paramTypes, boolean isDistinct) {
    try {
      if (hiveUDAFParticipator.getGenericUDAFEvaluator() == null) {
        hiveUDAFParticipator.setGenericUDAFEvaluator(
            HiveSqlUDAFReturnTypeInference.newGenericUDAFEvaluator(
                hiveUDAFClass, paramTypes, isDistinct));
        hiveUDAFParticipator.setOutputObjectInspector(
            HiveSqlUDAFReturnTypeInference.initUDAFEvaluator(
                hiveUDAFParticipator.getGenericUDAFEvaluator(),
                paramTypes));
        hiveUDAFParticipator.setAggregationBuffer(
            hiveUDAFParticipator.getGenericUDAFEvaluator()
                .getNewAggregationBuffer());
      }

      Object[] convertedParas = new Object[params.length];
      for (int i = 0; i < convertedParas.length; i++) {
        convertedParas[i] =
            TypeInferenceUtil.convertCalciteObject2HiveWritableObject(
                paramTypes[i], params[i]);
      }
      hiveUDAFParticipator.getGenericUDAFEvaluator()
          .iterate(hiveUDAFParticipator.getAggregationBuffer(), convertedParas);
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }


  public static Object executeTerminate(
      HiveUDAFParticipator hiveUDAFParticipator) {
    try {
      return TypeInferenceUtil.convertHiveObject2CalciteObject(
          hiveUDAFParticipator.getOutputObjectInspector(),
          hiveUDAFParticipator.getGenericUDAFEvaluator()
              .terminate(hiveUDAFParticipator.getAggregationBuffer()));
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

}

// End HiveUDAFImplementor.java
