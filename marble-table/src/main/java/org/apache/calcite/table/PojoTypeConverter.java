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


import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.CaseFormat;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * convert a pojo to PojoRelDataTypeValue(java value => calcite value)
 * supported field types include:
 * primitive types and collection types that with a non-primitive generic type
 */
public class PojoTypeConverter {

  private RelDataTypeFactory typeFactory;
  private Map<String, String> fieldName2SqlColumnNameMap;
  private boolean upperUnderscore;
  private TimeZone timeZone;

  public PojoTypeConverter(RelDataTypeFactory typeFactory,
      Map<String, String> fieldName2SqlColumnNameMap, boolean upperUnderscore,
      TimeZone timeZone) {
    this.typeFactory = typeFactory;
    this.fieldName2SqlColumnNameMap = fieldName2SqlColumnNameMap;
    this.upperUnderscore = upperUnderscore;
    this.timeZone = timeZone;
  }

  private static final Map<Class<?>, SqlTypeName> BASIC_TYPES = new HashMap<>();

  static {
    BASIC_TYPES.put(String.class, SqlTypeName.VARCHAR);
    BASIC_TYPES.put(Boolean.class, SqlTypeName.BOOLEAN);
    BASIC_TYPES.put(boolean.class, SqlTypeName.BOOLEAN);
    BASIC_TYPES.put(Short.class, SqlTypeName.SMALLINT);
    BASIC_TYPES.put(short.class, SqlTypeName.SMALLINT);
    BASIC_TYPES.put(Integer.class, SqlTypeName.INTEGER);
    BASIC_TYPES.put(int.class, SqlTypeName.INTEGER);
    BASIC_TYPES.put(Long.class, SqlTypeName.BIGINT);
    BASIC_TYPES.put(long.class, SqlTypeName.BIGINT);
    BASIC_TYPES.put(Float.class, SqlTypeName.FLOAT);
    BASIC_TYPES.put(float.class, SqlTypeName.FLOAT);
    BASIC_TYPES.put(Double.class, SqlTypeName.DOUBLE);
    BASIC_TYPES.put(double.class, SqlTypeName.DOUBLE);
    BASIC_TYPES.put(Character.class, SqlTypeName.CHAR);
    BASIC_TYPES.put(char.class, SqlTypeName.CHAR);
    BASIC_TYPES.put(Date.class, SqlTypeName.TIMESTAMP);
    BASIC_TYPES.put(BigDecimal.class, SqlTypeName.DECIMAL);
  }

  public static List<Field> getAllDeclaredFields(Class<?> clazz) {
    List<Field> result = new ArrayList<Field>();
    while (clazz != null && !clazz.equals(Object.class)) {
      Field[] fields = clazz.getDeclaredFields();
      for (Field field : fields) {
        if (Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(
            field.getModifiers()) || Modifier.isFinal(field.getModifiers())) {
          continue;
        }
        result.add(field);
      }
      clazz = clazz.getSuperclass();
    }
    return result;
  }

  public PojoRelDataTypeValue getPojoRelDataTypeValue(Class clazz,
      List<Field> fieldList, Object pojo) {
    try {
      if (fieldList.size() == 0) {
        throw new IllegalArgumentException(
            "class " + clazz + " has 0 fields ！");
      }
      final RelDataTypeFactory.Builder builder = typeFactory
          .builder();
      List row = new ArrayList();
      for (Field field : fieldList) {
        PojoRelDataTypeValue typeValue = getTypeValueForPojoField(field, pojo);
        if (typeValue != null) {
          builder.add(convertFieldName(field.getName()),
              typeValue.getRelDataType())
              .nullable(true);
          row.add(typeValue.getValue());
        }
      }
      if (row.size() == 0) {
        throw new RuntimeException(
            "class:" + clazz + "must have an effective field ");
      }
      return new PojoRelDataTypeValue(row.toArray(), builder.build());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SqlTypeName getSqlType(Class clazz) {
    SqlTypeName typeName = BASIC_TYPES.get(clazz);
    if (typeName == null) {
      if (clazz.isArray() || Collection.class.isAssignableFrom(clazz)) {
        typeName = SqlTypeName.ARRAY;
      }
    }
    if (typeName == null) {
      throw new IllegalArgumentException(
          "can't resolve sql type for class:" + clazz);
    }
    return typeName;
  }

  /**
   * Convert ParameterizedType or Class to a Class.
   */
  public static Class<?> typeToClass(Type t) {
    if (t instanceof Class) {
      return (Class<?>) t;
    } else if (t instanceof ParameterizedType) {
      return (Class<?>) ((ParameterizedType) t).getActualTypeArguments()[0];
    }
    throw new IllegalArgumentException("Cannot convert type to class");
  }

  public PojoRelDataTypeValue getTypeValueForPojoField(Field field, Object pojo)
      throws IllegalAccessException {
    SqlTypeName typeName = getSqlType(field.getType());
    Object javaValue = pojo == null ? null : field.get(pojo);
    PojoRelDataTypeValue typeValue;
    if (typeName.equals(SqlTypeName.ARRAY)) {
      if (field.getGenericType() == null) {
        throw new RuntimeException(
            "array field:" + field.getName() + " must have a generic type!");
      } else {
        typeValue = javaObjectToCalciteObject(typeName,
            javaValue,
            timeZone, typeToClass(field.getGenericType()));
      }

    } else {
      typeValue = javaObjectToCalciteObject(typeName,
          javaValue,
          timeZone, null);
    }

    return typeValue;
  }

  private String convertFieldName(String name) {
    String aliasName = null;
    if (fieldName2SqlColumnNameMap != null) {
      aliasName = fieldName2SqlColumnNameMap.get(name);
    }
    if (aliasName == null) {
      aliasName = name;
    }
    return upperUnderscore ? CaseFormat.LOWER_CAMEL.to(
        CaseFormat.UPPER_UNDERSCORE, aliasName) : aliasName;
  }


  public PojoRelDataTypeValue javaObjectToCalciteObject(SqlTypeName typeName,
      Object value,
      TimeZone timeZone, Class elementClass) {
    switch (typeName) {
    case VARCHAR:
      String stringVal = TypeConvertUtil.toString(value);
      return new PojoRelDataTypeValue(stringVal,
          typeFactory.createSqlType(typeName));
    case BIGINT:
      Long longVal = TypeConvertUtil.toLong(value);
      return new PojoRelDataTypeValue(longVal,
          typeFactory.createSqlType(typeName));
    case INTEGER:
      Integer intVal = TypeConvertUtil.toInteger(value);
      return new PojoRelDataTypeValue(intVal,
          typeFactory.createSqlType(typeName));
    case DOUBLE:
      Double doubleVal = TypeConvertUtil.toDouble(value);
      return new PojoRelDataTypeValue(doubleVal,
          typeFactory.createSqlType(typeName));
    case DECIMAL:
      BigDecimal decimalVal = TypeConvertUtil.toBigDecimal(value);
      return new PojoRelDataTypeValue(decimalVal,
          typeFactory.createSqlType(typeName));
    case FLOAT:
      Float floatVal = TypeConvertUtil.toFloat(value);
      return new PojoRelDataTypeValue(floatVal,
          typeFactory.createSqlType(typeName));
    case SMALLINT:
      Short shortVal = TypeConvertUtil.toShort(value);
      return new PojoRelDataTypeValue(shortVal,
          typeFactory.createSqlType(typeName));
    case BOOLEAN:
      Boolean boolVal = TypeConvertUtil.toBoolean(value);
      return new PojoRelDataTypeValue(boolVal,
          typeFactory.createSqlType(typeName));
    case TIMESTAMP:
      Timestamp timestamp = TypeConvertUtil.toTimestamp(value, timeZone);
      if (timestamp == null) {
        return new PojoRelDataTypeValue(null,
            typeFactory.createSqlType(typeName));
      }
      Long time = timestamp.getTime();
      return new PojoRelDataTypeValue(time,
          typeFactory.createSqlType(typeName));
    case DATE:
      Date date = TypeConvertUtil.toDate(value, timeZone);
      if (date == null) {
        return new PojoRelDataTypeValue(null,
            typeFactory.createSqlType(typeName));
      }
      int calciteDateValue = (int) DateTimeUtils.floorDiv(date.getTime(),
          DateTimeUtils.MILLIS_PER_DAY);
      return new PojoRelDataTypeValue(calciteDateValue,
          typeFactory.createSqlType(typeName));

    case ARRAY:
      RelDataType elementType = getPojoRelDataTypeValue(
          elementClass, getAllDeclaredFields(elementClass),
          null).getRelDataType();
      if (value == null) {
        return new PojoRelDataTypeValue(null,
            typeFactory.createArrayType(
                elementType,
                -1));
      }
      Object[] array;
      if (value.getClass().isArray()) {
        array = (Object[]) value;
      } else if (value instanceof Collection) {
        array = ((Collection) value).toArray();
      } else {
        throw new RuntimeException(
            "can't convert value to array,value class=" + value.getClass());
      }
      List<Object> convertedList = new ArrayList<>();
      for (Object element : array) {
        PojoRelDataTypeValue elementTypeValue = getPojoRelDataTypeValue(
            elementClass, getAllDeclaredFields(elementClass),
            element);
        convertedList.add(elementTypeValue.getValue());
      }
      return new PojoRelDataTypeValue(convertedList,
          typeFactory.createArrayType(elementType,
              -1));

    default:
      return new PojoRelDataTypeValue(value,
          typeFactory.createSqlType(typeName));
    }
  }
}

// End PojoTypeConverter.java
