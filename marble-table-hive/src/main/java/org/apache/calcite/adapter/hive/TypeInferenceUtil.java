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
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.table.TypeConvertUtil;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLeadLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFPercentileApprox;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector
    .StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive
    .TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
    .Category.PRIMITIVE;

/**
 * Util to infer data types of calcite and hive
 * hive types See
 * {@link <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types">hive types</a>}
 */
public final class TypeInferenceUtil {

  private TypeInferenceUtil() {
  }

  private static final Map<
      SqlTypeName, PrimitiveTypeInfo> CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO =
      new HashMap<>();

  private static final Map<
      PrimitiveTypeInfo, SqlTypeName> HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO = new
      HashMap<>();

  static {
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.DECIMAL,
        TypeInfoFactory.decimalTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.DOUBLE,
        TypeInfoFactory.doubleTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.BIGINT,
        TypeInfoFactory.longTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.FLOAT,
        TypeInfoFactory.floatTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.INTEGER,
        TypeInfoFactory.intTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.CHAR,
        TypeInfoFactory.stringTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.VARCHAR,
        TypeInfoFactory.stringTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.DATE,
        TypeInfoFactory.dateTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.TIMESTAMP,
        TypeInfoFactory.timestampTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.BOOLEAN,
        TypeInfoFactory.booleanTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.SMALLINT,
        TypeInfoFactory.shortTypeInfo);
    CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.put(SqlTypeName.TINYINT,
        TypeInfoFactory.shortTypeInfo);
    //
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.decimalTypeInfo,
        SqlTypeName.DECIMAL);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.doubleTypeInfo,
        SqlTypeName.DOUBLE);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.longTypeInfo,
        SqlTypeName.BIGINT);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.floatTypeInfo,
        SqlTypeName.FLOAT);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.intTypeInfo,
        SqlTypeName.INTEGER);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.stringTypeInfo,
        SqlTypeName.VARCHAR);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.dateTypeInfo,
        SqlTypeName.DATE);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.timestampTypeInfo,
        SqlTypeName.TIMESTAMP);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.booleanTypeInfo,
        SqlTypeName.BOOLEAN);
    HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.put(TypeInfoFactory.shortTypeInfo,
        SqlTypeName.SMALLINT);

  }

  public static ObjectInspector[] getObjectInspector(
      RelDataTypeHolder[] argsType) {
    List<ObjectInspector> objectInspectors = Arrays.stream(argsType)
        .map(x -> getObjectInspector(x))
        .collect(Collectors.toList());
    return objectInspectors.toArray(new ObjectInspector[0]);
  }

  private static ObjectInspector getObjectInspector(
      RelDataTypeHolder relDataTypeHolder) {
    SqlTypeName sqlTypeName = relDataTypeHolder.getSqlTypeName();
    if (sqlTypeName.equals(SqlTypeName.ARRAY)) {
      RelDataTypeHolder componentType = relDataTypeHolder.getComponentType();
      if (componentType == null) {
        throw new IllegalStateException("element type of array is null !");
      }
      return ObjectInspectorFactory.getStandardListObjectInspector(
          getObjectInspector(relDataTypeHolder.getComponentType()));
    } else {
      return getPrimitiveObjectInspector(relDataTypeHolder);
    }
  }

  private static ObjectInspector getPrimitiveObjectInspector(
      RelDataTypeHolder relDataTypeHolder) {
    SqlTypeName sqlTypeName = relDataTypeHolder.getSqlTypeName();

    //FIXME Hive TypeInfoFactory.decimalTypeInfo use a default scale and
    // precision
    PrimitiveTypeInfo primitiveTypeInfo = CALCITE_SQL_TYPE_2_HIVE_TYPE_INFO.get(
        sqlTypeName);
    if (primitiveTypeInfo == null) {
      throw new IllegalArgumentException(
          "can't find hive primitiveTypeInfo for Calcite SqlType: "
              + sqlTypeName);
    }
    ObjectInspector result;
    if (relDataTypeHolder.isConstant()) {
      Object value = relDataTypeHolder.getValue();
      Object hiveWritableValue = convertCalciteObject2HiveWritableObject(
          sqlTypeName,
          value);
      result =
          PrimitiveObjectInspectorFactory
              .getPrimitiveWritableConstantObjectInspector(
                  primitiveTypeInfo, hiveWritableValue);
    } else {
      result =
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
              primitiveTypeInfo);
    }

    return result;
  }

  public static RelDataType getRelDataType(ObjectInspector oi,
      RelDataTypeFactory factory) {
    if (oi == null) {
      return factory.createTypeWithNullability(
          factory.createSqlType(SqlTypeName.ANY), true);
    }
    ObjectInspector.Category category = oi.getCategory();
    if (!category.equals(PRIMITIVE)) {
      if (category.equals(ObjectInspector.Category.LIST)) {
        StandardListObjectInspector standardListObjectInspector =
            (StandardListObjectInspector) oi;
        return factory.createArrayType(
            getRelDataType(
                standardListObjectInspector.getListElementObjectInspector(),
                factory), -1);
      } else {
        throw new IllegalArgumentException(
            "unsupported ObjectInspector category :" + category);
      }

    }
    PrimitiveObjectInspector primitiveObjectInspector =
        (PrimitiveObjectInspector) oi;
    PrimitiveTypeInfo primitiveTypeInfo =
        primitiveObjectInspector.getTypeInfo();

    SqlTypeName sqlTypeName = HIVE_TYPE_2_CALCITE_SQL_TYPE_INFO.get(
        primitiveTypeInfo);
    //handle DecimalTypeInfo special case
    if (primitiveTypeInfo instanceof DecimalTypeInfo) {
      sqlTypeName = SqlTypeName.DECIMAL;
//      return factory.createTypeWithNullability(
//          factory.createSqlType(
//              sqlTypeName, ((DecimalTypeInfo) primitiveTypeInfo).getPrecision(),
//              ((DecimalTypeInfo) primitiveTypeInfo).getScale()),
//          true);
    }
    if (sqlTypeName == null) {
      throw new RuntimeException(
          "can't get sqlType for hive primitiveTypeInfo:" + primitiveTypeInfo);
    }
    return factory.createTypeWithNullability(
        factory.createSqlType(
            sqlTypeName),
        true);

  }

  public static Object convertCalciteObject2HiveWritableObject(
      SqlTypeName sqlTypeName, Object value) {
    if (value == null) {
      return null;
    }
    Object result;
    switch (sqlTypeName) {
    case DECIMAL:
      result = new HiveDecimalWritable(
          HiveDecimal.create(TypeConvertUtil.toBigDecimal(value)));
      break;
    case DOUBLE:
      result = new DoubleWritable(TypeConvertUtil.toDouble(value));
      break;
    case BIGINT:
      result = new LongWritable(TypeConvertUtil.toLong(value));
      break;
    case FLOAT:
      result = new FloatWritable(TypeConvertUtil.toFloat(value));
      break;
    case INTEGER:
      result = new IntWritable(TypeConvertUtil.toInteger(value));
      break;
    case VARCHAR:
    case CHAR:
      result = new Text(TypeConvertUtil.toString(value));
      break;
    case DATE:
      result = new DateWritable(TypeConvertUtil.toDate(value, null));
      break;
    case TIMESTAMP:
      result = new TimestampWritable(TypeConvertUtil.toTimestamp(value, null));
      break;
    case BOOLEAN:
      result = new BooleanWritable(TypeConvertUtil.toBoolean(value));
      break;
    case SMALLINT:
      result = new ShortWritable(TypeConvertUtil.toShort(value));
      break;
    default:
      throw new UnsupportedOperationException(
          "can't convert an java Object to Hive Writable Object for SqlType: "
              + sqlTypeName + ",and value=" + value);
    }
    return result;
  }

  /**
   * convert the  result of a hive function to the right java object
   *
   * @param objectInspector the outObjectInspector inferred by a hive
   *                        function
   * @param value           the result hive object evaluated by a hive function
   * @return converted calcite object result
   */
  public static Object convertHiveObject2CalciteObject(
      ObjectInspector objectInspector,
      Object value) {
    if (value == null) {
      return null;
    }
    if (objectInspector instanceof HiveDecimalObjectInspector) {
      return ((HiveDecimalObjectInspector) objectInspector)
          .getPrimitiveJavaObject(
              value).bigDecimalValue();
    }
    if (objectInspector instanceof DoubleObjectInspector) {
      return ((DoubleObjectInspector) objectInspector).get(value);
    }
    if (objectInspector instanceof IntObjectInspector) {
      return ((IntObjectInspector) objectInspector).get(value);
    }
    if (objectInspector instanceof LongObjectInspector) {
      return ((LongObjectInspector) objectInspector).get(value);
    }
    if (objectInspector instanceof FloatObjectInspector) {
      return ((FloatObjectInspector) objectInspector).get(value);
    }
    if (objectInspector instanceof ShortObjectInspector) {
      return ((ShortObjectInspector) objectInspector).get(value);
    }
    if (objectInspector instanceof StringObjectInspector) {
      return ((StringObjectInspector) objectInspector).getPrimitiveJavaObject(
          value);
    }
    if (objectInspector instanceof BooleanObjectInspector) {
      return ((BooleanObjectInspector) objectInspector).getPrimitiveJavaObject(
          value);
    }
    if (objectInspector instanceof DateObjectInspector) {
      Date date = ((DateObjectInspector) objectInspector)
          .getPrimitiveJavaObject(
              value);
      return SqlFunctions.toInt(date, TimeZone.getTimeZone("UTC"));
    }
    if (objectInspector instanceof TimestampObjectInspector) {
      Timestamp timestamp = ((TimestampObjectInspector) objectInspector)
          .getPrimitiveJavaObject(
              value);
      return SqlFunctions.toLong(timestamp, TimeZone.getTimeZone("UTC"));
    }
    if (objectInspector instanceof StandardListObjectInspector) {
      StandardListObjectInspector standardListObjectInspector =
          (StandardListObjectInspector) objectInspector;
      List<?> list = standardListObjectInspector.getList(value);
      if (list == null) {
        return null;
      }
      ObjectInspector listElementObjectInspector =
          standardListObjectInspector.getListElementObjectInspector();
      List<Object> convertedList = new ArrayList<>();
      for (Object hiveObject : list) {
        convertedList.add(
            convertHiveObject2CalciteObject(listElementObjectInspector,
                hiveObject));
      }
      return convertedList;
    }
    throw new UnsupportedOperationException(
        "fail to convertHiveObject2CalciteObject ,objectInspector="
            + objectInspector
            + ",value="
            + value);
  }


  public static final Map<Class, Map<Integer, Object>>
      HIVE_UDAF_CONSTANT_OBJECT_INSPECT_CONTEXT_MAP = new HashMap<>();

  static {
    Map<Integer, Object> map1 = new HashMap<>();
    map1.put(1, 0.5);
    HIVE_UDAF_CONSTANT_OBJECT_INSPECT_CONTEXT_MAP.put(
        GenericUDAFPercentileApprox.class, map1);
    Map<Integer, Object> map2 = new HashMap<>();
    map2.put(1, 0);
    HIVE_UDAF_CONSTANT_OBJECT_INSPECT_CONTEXT_MAP.put(GenericUDAFLeadLag.class,
        map2);
  }

  /**
   * Workaround: due to the limit of calcite,once a SqlCall be converted to an
   * {@link org.apache.calcite.rel.core.Aggregate},we can't identify if it's
   * operand is constant or not, we do some hard-coding here to provide
   * essential information.
   */
  public static boolean isOperandConstantForHiveUDAF(Class hiveUDAFClass,
      int operandIndex) {
    Map<Integer, Object> parameterIndex2ValueMap =
        HIVE_UDAF_CONSTANT_OBJECT_INSPECT_CONTEXT_MAP
            .get(hiveUDAFClass);
    return parameterIndex2ValueMap != null
        && parameterIndex2ValueMap.containsKey(
        operandIndex);
  }

}

// End TypeInferenceUtil.java
