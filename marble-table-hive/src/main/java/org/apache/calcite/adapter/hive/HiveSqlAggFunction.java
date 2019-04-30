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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;


/**
 * hive aggregate function with no argument checker
 */
public class HiveSqlAggFunction extends SqlAggFunction {


  protected HiveSqlAggFunction(String name, boolean requiresOrder,
      boolean requiresOver,
      SqlReturnTypeInference sqlReturnTypeInference) {
    this(name, new SqlIdentifier(name, SqlParserPos.ZERO),
        SqlKind.OTHER_FUNCTION,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        requiresOrder, requiresOver, sqlReturnTypeInference);
  }

  protected HiveSqlAggFunction(String name, SqlIdentifier sqlIdentifier,
      SqlKind kind,
      SqlFunctionCategory funcType,
      boolean requiresOrder,
      boolean requiresOver,
      SqlReturnTypeInference sqlReturnTypeInference) {
    super(name, sqlIdentifier,
        kind, sqlReturnTypeInference, null,
        HiveSqlFunction.ArgChecker.INSTANCE,
        funcType, requiresOrder, requiresOver);
  }


}

// End HiveSqlAggFunction.java
