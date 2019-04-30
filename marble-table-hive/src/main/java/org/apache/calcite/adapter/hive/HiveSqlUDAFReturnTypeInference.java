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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * A HiveSqlUDAFReturnTypeInference bridges a GenericUDAFEvaluator to infer
 * the result type of a hive UDAF call
 */
public class HiveSqlUDAFReturnTypeInference implements SqlReturnTypeInference {


  private HiveSqlUDAFReturnTypeInference() {
  }

  public static final HiveSqlUDAFReturnTypeInference INSTANCE =
      new HiveSqlUDAFReturnTypeInference();


  @Override public RelDataType inferReturnType(
      final SqlOperatorBinding opBinding) {
    try {
      RelDataTypeFactory factory = opBinding.getTypeFactory();
      SqlOperator sqlOperator = opBinding.getOperator();
      String opName = sqlOperator.getName();
      Class hiveUDAFClass = HiveSqlOperatorTable.instance()
          .getHiveUDAFClass(opName);
      List<RelDataTypeHolder> argsType = new ArrayList<>();

      for (int i = 0; i < opBinding.getOperandCount(); i++) {
        RelDataTypeHolder relDataTypeHolder;
        if (TypeInferenceUtil.isOperandConstantForHiveUDAF(hiveUDAFClass, i)) {
          //we use a pre-defined fake value here to getGenericUDAFReturnType
          Object constantValue = TypeInferenceUtil
              .HIVE_UDAF_CONSTANT_OBJECT_INSPECT_CONTEXT_MAP
              .get(hiveUDAFClass).get(i);
          relDataTypeHolder = new RelDataTypeHolder(opBinding.getOperandType(i),
              true, constantValue);
        } else {
          relDataTypeHolder = new RelDataTypeHolder(
              opBinding.getOperandType(i));
        }
        argsType.add(relDataTypeHolder);
      }
      RelDataType resultType = getGenericUDAFReturnType(
          hiveUDAFClass,
          argsType.toArray(new RelDataTypeHolder[0]), factory);
      return resultType;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static RelDataType getGenericUDAFReturnType(Class udafClazz,
      RelDataTypeHolder[] relDataTypeHolders, RelDataTypeFactory factory) {
    //we set isDistinct to false here,it has no effect on type inferring
    GenericUDAFEvaluator genericUDAFEvaluator = newGenericUDAFEvaluator(
        udafClazz, relDataTypeHolders, false);
    ObjectInspector objectInspector = initUDAFEvaluator(genericUDAFEvaluator,
        relDataTypeHolders);
    return TypeInferenceUtil.getRelDataType(objectInspector, factory);
  }

  public static GenericUDAFEvaluator newGenericUDAFEvaluator(Class udafClass,
      RelDataTypeHolder[] relDataTypeHolders, boolean isDistinct) {
    ObjectInspector[] objectInspectors = TypeInferenceUtil.getObjectInspector(
        relDataTypeHolders);
    try {
      GenericUDAFResolver2 genericUDAFResolver;
      if (GenericUDAFResolver2.class.isAssignableFrom(udafClass)) {
        genericUDAFResolver = (GenericUDAFResolver2) udafClass.newInstance();
      } else {
        UDAF udaf = (UDAF) udafClass.newInstance();
        genericUDAFResolver = new GenericUDAFBridge(udaf);
      }
      GenericUDAFParameterInfo info = new SimpleGenericUDAFParameterInfo(
          objectInspectors, isDistinct, false);
      return genericUDAFResolver.getEvaluator(info);
    } catch (IllegalAccessException | InstantiationException
        | SemanticException e) {
      throw new RuntimeException(e);
    }
  }


  public static ObjectInspector initUDAFEvaluator(
      GenericUDAFEvaluator genericUDAFEvaluator,
      RelDataTypeHolder[] relDataTypeHolders) {
    ObjectInspector[] objectInspectors = TypeInferenceUtil.getObjectInspector(
        relDataTypeHolders);
    try {
      ObjectInspector outObjectInspector = genericUDAFEvaluator.init(
          GenericUDAFEvaluator.Mode.COMPLETE, objectInspectors);
      return outObjectInspector;
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
  }

}

// End HiveSqlUDAFReturnTypeInference.java
