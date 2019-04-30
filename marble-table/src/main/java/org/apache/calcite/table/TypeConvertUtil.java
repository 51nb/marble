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

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Helper methods to convert types for  objects.
 * can be used  to   cast types explicitly in generated code by calcite
 */
public final class TypeConvertUtil {
  private TypeConvertUtil() {
  }

  public static Short toShort(Object value) {
    if (value == null) {
      return null;
    }
    return SqlFunctions.toShort(value);
  }

  public static Float toFloat(Object value) {
    if (value == null) {
      return null;
    }
    return SqlFunctions.toFloat(value);
  }

  public static Long toLong(Object value) {
    if (value == null) {
      return null;
    }
    return SqlFunctions.toLong(value);
  }

  public static BigDecimal toBigDecimal(Object value) {
    if (value == null) {
      return null;
    }
    return SqlFunctions.toBigDecimal(value);
  }

  public static Integer toInteger(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Boolean) {
      return ((Boolean) value) ? 1 : 0;
    }
    return SqlFunctions.toInt(value);
  }

  public static Double toDouble(Object value) {
    if (value == null) {
      return null;
    }
    return SqlFunctions.toDouble(value);
  }

  private static Long toTimeWithLocalTimeZone(Long time, TimeZone timeZone) {
    return timeZone == null ? time : time + timeZone.getOffset(time);
  }

  public static Timestamp toTimestamp(Object value, TimeZone timeZone) {
    if (value == null) {
      return null;
    }
    if (value instanceof Date) {
      Long time = ((Date) value).getTime();
      return new Timestamp(toTimeWithLocalTimeZone(time, timeZone));
    }
    if (value instanceof java.util.Date) {
      Long time = ((java.util.Date) value).getTime();
      return new Timestamp(toTimeWithLocalTimeZone(time, timeZone));
    } else if (value instanceof Long) {
      Long time = (Long) value;
      return new Timestamp(toTimeWithLocalTimeZone(time, timeZone));
    } else if (value instanceof Integer) {
      Long time = ((Integer) value).longValue();
      return new Timestamp(toTimeWithLocalTimeZone(time, timeZone));
    } else {
      return (Timestamp) value;
    }
  }

  public static Date toDate(Object value, TimeZone timeZone) {
    if (value == null) {
      return null;
    }
    if (value instanceof Integer) {
      Integer valInt = (Integer) value;
      return new Date(toTimeWithLocalTimeZone(valInt.longValue(), timeZone));
    } else if (value instanceof Long) {
      Long valLong = (Long) value;
      return new Date(toTimeWithLocalTimeZone(valLong, timeZone));
    } else {
      return (Date) value;
    }

  }

  public static String toString(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof NlsString) {
      return ((NlsString) value).getValue();
    } else {
      return value.toString();
    }
  }

  public static Boolean toBoolean(Object value) {
    if (value == null) {
      return null;
    } else {
      return SqlFunctions.toBoolean(value);
    }
  }


  public static final Map<SqlTypeName, String> CALCITE_SQL_TYPE_2_CAST_METHOD =
      new HashMap<>();

  static {
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.BOOLEAN, "castBoolean");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.SMALLINT, "castInt");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.INTEGER, "castInt");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.BIGINT, "castLong");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.FLOAT, "castFloat");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.DOUBLE, "castDouble");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.VARCHAR, "castString");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.TIMESTAMP, "castLong");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.DECIMAL, "castBigDecimal");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.DATE, "castInt");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.ANY, "castObject");
    CALCITE_SQL_TYPE_2_CAST_METHOD.put(SqlTypeName.ARRAY, "castList");
  }

  public static Long castLong(Object value) {
    return (Long) value;
  }

  public static String castString(Object value) {
    return (String) value;
  }

  public static Double castDouble(Object value) {
    return (Double) value;
  }

  public static Integer castInt(Object value) {
    return (Integer) value;
  }

  public static Boolean castBoolean(Object value) {
    return (Boolean) value;
  }

  public static BigDecimal castBigDecimal(Object value) {
    return (BigDecimal) value;
  }

  public static Float castFloat(Object value) {
    return (Float) value;
  }

  public static List castList(Object value) {
    return (List) value;
  }

  public static Object castObject(Object value) {
    return value;
  }


}

// End TypeConvertUtil.java
