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
package org.apache.calcite.adapter.hive.udtf;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.BaseQueryable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.collect.Lists;

import java.util.List;

/**
 *
 */
public class UDTFExplode {

  private UDTFExplode() {
  }

  public static QueryableTable eval(List<String> params) {
    return new AbstractQueryableTable(String.class) {
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataType dataType = typeFactory.createSqlType(
            SqlTypeName.VARCHAR);
        return typeFactory.createStructType(
            Lists.newArrayList(dataType), Lists.newArrayList("s"));
      }

      public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
          SchemaPlus schema, String tableName) {
        BaseQueryable<String> queryable =
            new BaseQueryable<String>(null, String.class, null) {
              public Enumerator<String> enumerator() {
                return new Enumerator<String>() {
                  int i = 0;
                  int curI;
                  String curS;

                  public String current() {
                    return curS;
                  }

                  public boolean moveNext() {
                    if (params == null) {
                      return false;
                    }
                    if (i < params.size()) {
                      curI = i;
                      curS = params.get(i);
                      ++i;
                      return true;
                    } else {
                      return false;
                    }
                  }

                  public void reset() {
                    i = 0;
                  }

                  public void close() {
                  }
                };
              }
            };
        //noinspection unchecked
        return (Queryable<T>) queryable;
      }
    };
  }
}

// End UDTFExplode.java
