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

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.AvaticaSite;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;

import com.google.common.collect.ImmutableMap;

import java.util.TimeZone;

/**
 * Runtime context allowing access to the tables in a database.
 * simplifying the default DataContextImpl：
 * {@link org.apache.calcite.jdbc.CalciteConnectionImpl.DataContextImpl}
 */
public class TableDataContexImpl implements DataContext {

  private final ImmutableMap<Object, Object> map;
  private final SchemaPlus rootSchema;
  private final QueryProvider queryProvider;
  private final JavaTypeFactory typeFactory;

  public TableDataContexImpl(QueryProvider queryProvider,
      SchemaPlus rootSchema,
      JavaTypeFactory typeFactory, TableConfig tableConfig) {
    this.queryProvider = queryProvider;
    this.typeFactory = typeFactory;
    this.rootSchema = rootSchema;

    final long time = System.currentTimeMillis();
    final TimeZone timeZone = tableConfig.getTimeZone();
    final long localOffset = timeZone.getOffset(time);
    final long currentOffset = localOffset;
    ImmutableMap.Builder<Object, Object> builder = ImmutableMap.builder();
    builder.put(Variable.UTC_TIMESTAMP.camelName, time)
        .put(Variable.CURRENT_TIMESTAMP.camelName, time + currentOffset)
        .put(Variable.LOCAL_TIMESTAMP.camelName, time + localOffset)
        .put(Variable.TIME_ZONE.camelName, timeZone);

    map = builder.build();
  }

  public synchronized Object get(String name) {
    Object o = map.get(name);
    if (o == AvaticaSite.DUMMY_VALUE) {
      return null;
    }
    return o;
  }


  public SchemaPlus getRootSchema() {
    return rootSchema;
  }

  public JavaTypeFactory getTypeFactory() {
    return typeFactory;
  }

  public QueryProvider getQueryProvider() {
    return queryProvider;
  }
}

// End TableDataContexImpl.java
