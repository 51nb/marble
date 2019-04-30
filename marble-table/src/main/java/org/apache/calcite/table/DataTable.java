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

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import com.google.common.collect.Maps;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * table impl for a type-known dataset
 * see {@link org.apache.calcite.sql.ddl.SqlCreateTable.MutableArrayTable}
 */
public class DataTable extends AbstractTable
    implements QueryableTable {

  private List rows = new DataTableStoreList();
  private RelProtoDataType protoRowType;
  private RelDataType rowType;

  /**
   * new ArrayList<>()
   * Creates a DataTable.
   */
  public DataTable(RelDataType rowType, List rows) {
    super();
    this.rowType = Objects.requireNonNull(rowType);
    this.protoRowType = RelDataTypeImpl.proto(rowType);
    this.rows = rows;

  }

  public int getRowCount() {
    return rows.size();
  }

  public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
      SchemaPlus schema, String tableName) {
    return new DataTableQueryable<T>(queryProvider, schema, this,
        tableName) {
      public Enumerator<T> enumerator() {
        //noinspection unchecked
        return (Enumerator<T>) Linq4j.enumerator(rows);
      }

      public List getRows() {
        return rows;
      }
    };
  }

  public Type getElementType() {
    return Object[].class;
  }

  public Expression getExpression(SchemaPlus schema, String tableName,
      Class clazz) {
    return Schemas.tableExpression(schema, getElementType(),
        tableName, clazz);
  }

  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return protoRowType.apply(typeFactory);
  }

  public List<Map<String, Object>> toMapList() {
    List<Map<String, Object>> list = new ArrayList<>();

    for (Object row : rows) {
      Map<String, Object> rowMap = Maps.newHashMap();
      if (rowType.getFieldList().size() == 1) {
        RelDataTypeField field = rowType.getFieldList().get(0);
        rowMap.put(field.getName().toLowerCase(), row);
      } else {
        Object[] rowArray = (Object[]) row;
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          RelDataTypeField field = rowType.getFieldList().get(i);
          rowMap.put(field.getName().toLowerCase(), rowArray[i]);
        }
      }
      list.add(rowMap);
    }
    return list;
  }

  @Override public String toString() {
    return "rowType="
        + rowType.toString()
        + ",size="
        + getRowCount()
        + ",rows=" + (rows.isEmpty()
        ? "[]"
        : "[" + rows + "  ...]");
  }

  /**
   * a simple list store
   */
  public static class DataTableStoreList extends ArrayList {
    @Override public boolean add(Object o) {
      return super.add(o);
    }
  }

  /**
   * @param <T> Element type
   */
  public abstract class DataTableQueryable<T> extends AbstractTableQueryable {
    public DataTableQueryable(QueryProvider queryProvider,
        SchemaPlus schema, QueryableTable table, String tableName) {
      super(queryProvider, schema, table, tableName);
    }

    public abstract List getRows();
  }

}

// End DataTable.java
