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
import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AggregateFunctionImpl;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import org.apache.commons.lang3.StringUtils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A TableEnv can be used to:
 * 1.register a Table in [TableEnv]'s catalog
 * 2.add subSchema and Function in [TableEnv]'s catalog
 * 3.convert a JDBC ResultSet to a Table
 * 5.convert a simple java pojo list to a Table
 * 6.execute a sql query to get a Table
 * <p> a TableEnv supports the calcite-sql query by default,
 * but developers can extend a TableEnv to implement
 * a HiveTableEnv or MysqlTableEnv etc...,to support other sql dialects.
 */
public class TableEnv {

  private static int maxSqlPlanCacheSize;
  private static Cache<String, TableExecutionPlan> sql2ExecutionPlanCache;

  /**
   * for user to conveniently inspect the generated code
   */
  private static final Cache<String, String> SQL_2_EXECUTION_PLAN_CODE_CACHE
      = CacheBuilder
      .newBuilder()
      .maximumSize(200)
      .build();

  private static final Map<String, Class> SQL_2_BINDABLE_CLASS_MAP = new
      ConcurrentHashMap<>();


  private String cacheKeyPrefix = "";

  public String getCacheKeyPrefix() {
    return cacheKeyPrefix;
  }

  public void setCacheKeyPrefix(
      String cacheKeyPrefix) {
    this.cacheKeyPrefix = cacheKeyPrefix;
  }

  public static void clearExecutionPlan() {
    if (sql2ExecutionPlanCache != null) {
      sql2ExecutionPlanCache.invalidateAll();
    }
  }

  public void registerSqlBindableClass(String sql, Class clazz) {
    SQL_2_BINDABLE_CLASS_MAP.put(cacheKeyPrefix + sql,
        clazz);
  }

  public static int getMaxSqlPlanCacheSize() {
    return maxSqlPlanCacheSize;
  }

  public static synchronized void enableSqlPlanCacheSize(
      int maxSqlPlanCacheSize) {
    if (sql2ExecutionPlanCache == null) {
      TableEnv.maxSqlPlanCacheSize = maxSqlPlanCacheSize;
      sql2ExecutionPlanCache = CacheBuilder
          .newBuilder()
          .maximumSize(maxSqlPlanCacheSize)
          .build();
    } else {
      throw new RuntimeException(
          "sql2ExecutionPlanCache has already been enabled,maxSqlPlanCacheSize="
              + maxSqlPlanCacheSize);
    }
  }

  public static TableEnv getTableEnv() {
    return new TableEnv(new TableConfig());
  }

  public static TableEnv getTableEnv(TableConfig tableConfig) {
    return new TableEnv(tableConfig);
  }

  protected TableConfig tableConfig;
  private CalciteSchema internalSchema = CalciteSchema.createRootSchema(false,
      false);
  protected SchemaPlus rootSchema = internalSchema.plus();
  protected FrameworkConfig frameworkConfig;
  protected CalciteCatalogReader calciteCatalogReader;


  public TableEnv(TableConfig tableConfig) {
    try {
      this.tableConfig = tableConfig;
      SqlParser.Config sqlParserConfig = tableConfig.getSqlParserConfig()
          != null ? tableConfig.getSqlParserConfig() : SqlParser
          .configBuilder().setCaseSensitive(false)
          .build();
      SqlOperatorTable sqlStdOperatorTable = tableConfig
          .getSqlOperatorTable()
          != null
          ? tableConfig.getSqlOperatorTable()
          : ChainedSqlOperatorTable.of(SqlStdOperatorTable.instance());
      CalciteConnectionConfig calciteConnectionConfig = tableConfig
          .getCalciteConnectionConfig()
          != null
          ? tableConfig.getCalciteConnectionConfig()
          : createDefaultConnectionConfig(sqlParserConfig);
      RelDataTypeSystem typeSystem = tableConfig.getRelDataTypeSystem() != null
          ? tableConfig.getRelDataTypeSystem()
          : calciteConnectionConfig.typeSystem(RelDataTypeSystem.class,
              RelDataTypeSystem.DEFAULT);
      SqlRexConvertletTable convertletTable = tableConfig
          .getConvertletTable()
          != null
          ? tableConfig
          .getConvertletTable()
          : StandardConvertletTable.INSTANCE;
      RexExecutor rexExecutor = tableConfig.getRexExecutor() != null
          ? tableConfig.getRexExecutor()
          : RexUtil.EXECUTOR;
      this.calciteCatalogReader = new CalciteCatalogReader(
          CalciteSchema.from(rootSchema),
          CalciteSchema.from(rootSchema).path(null),
          new JavaTypeFactoryImpl(typeSystem),
          calciteConnectionConfig);
      this.frameworkConfig = createFrameworkConfig(sqlParserConfig,
          ChainedSqlOperatorTable.of(sqlStdOperatorTable,
              calciteCatalogReader), convertletTable,
          calciteConnectionConfig, typeSystem, rexExecutor);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public TableConfig getTableConfig() {
    return tableConfig;
  }

  protected Program createProgram() {
    final Program program1 =
        (planner, rel, requiredOutputTraits, materializations, lattices) -> {
          //override the default ruleSet of planner here
          planner.setRoot(rel);
          for (RelOptMaterialization materialization : materializations) {
            planner.addMaterialization(materialization);
          }
          for (RelOptLattice lattice : lattices) {
            planner.addLattice(lattice);
          }

          final RelNode rootRel2 =
              rel.getTraitSet().equals(requiredOutputTraits)
                  ? rel
                  : planner.changeTraits(rel, requiredOutputTraits);
          assert rootRel2 != null;

          planner.setRoot(rootRel2);
          final RelOptPlanner planner2 = planner.chooseDelegate();
          final RelNode rootRel3 = planner2.findBestExp();
          assert rootRel3 != null : "could not implement exp";
          return rootRel3;
        };
    DefaultRelMetadataProvider metadataProvider = DefaultRelMetadataProvider
        .INSTANCE;
    return Programs.sequence(Programs.subQuery(metadataProvider),
        new Programs.DecorrelateProgram(),
        new Programs.TrimFieldsProgram(),
        program1,

        // Second planner pass to do physical "tweaks". This the first time
        // that EnumerableCalcRel is introduced.
        Programs.calc(metadataProvider));
  }

  private FrameworkConfig createFrameworkConfig(
      SqlParser.Config sqlParserConfig, SqlOperatorTable sqlOperatorTable,
      SqlRexConvertletTable convertletTable,
      CalciteConnectionConfig calciteConnectionConfig,
      RelDataTypeSystem relDataTypeSystem, RexExecutor rexExecutor) {
    return Frameworks
        .newConfigBuilder()
        .defaultSchema(rootSchema)
        .parserConfig(sqlParserConfig)
        .operatorTable(sqlOperatorTable)
        .convertletTable(convertletTable)
        .typeSystem(relDataTypeSystem)
        .executor(rexExecutor)
        .context(Contexts.of(calciteConnectionConfig))
        .build();
  }

  private CalciteConnectionConfig createDefaultConnectionConfig(
      SqlParser.Config sqlParserConfig) {
    Properties prop = new Properties();
    prop.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(sqlParserConfig.caseSensitive()));
    return new CalciteConnectionConfigImpl(prop);
  }

  public DataTable getTable(String tableName) {
    return (DataTable) rootSchema.getTable(tableName.toUpperCase());
  }

  public DataTable getTable(String subSchemaName, String tableName) {
    return (DataTable) rootSchema.getSubSchema(subSchemaName.toUpperCase())
        .getTable(tableName.toUpperCase());
  }

  public void registerTable(String tableName, DataTable dataTable) {
    rootSchema.add(tableName.toUpperCase(), dataTable);
  }

  public void registerTable(String subSchemaName, String tableName,
      DataTable dataTable) {
    rootSchema.getSubSchema(subSchemaName.toUpperCase())
        .add(tableName.toUpperCase(), dataTable);
  }

  public SchemaPlus addSubSchema(String subSchemaName) {
    return rootSchema.add(subSchemaName.toUpperCase(), new AbstractSchema());
  }

  public void addFunction(String schemaName, String functionName,
      String className, String methodName) {
    boolean upCase = true;
    SchemaPlus schema;
    schemaName = schemaName.toUpperCase();
    functionName = functionName.toUpperCase();
    if (StringUtils.isBlank(schemaName)) {
      schema = rootSchema;
    } else {
      schema = rootSchema.getSubSchema(schemaName);
      if (schema == null) {
        schema = addSubSchema(schemaName);
      }
    }
    final Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("UDF class '"
          + className + "' not found");
    }
    final TableFunction tableFunction =
        TableFunctionImpl.create(clazz, Util.first(methodName, "eval"));
    if (tableFunction != null) {
      schema.add(functionName, tableFunction);
      return;
    }
    // Must look for TableMacro before ScalarFunction. Both have an "eval"
    // method.
    final TableMacro macro = TableMacroImpl.create(clazz);
    if (macro != null) {
      schema.add(functionName, macro);
      return;
    }
    if (methodName != null && methodName.equals("*")) {
      for (Map.Entry<String, ScalarFunction> entry
          : ScalarFunctionImpl.createAll(clazz).entries()) {
        String name = entry.getKey();
        if (upCase) {
          name = name.toUpperCase(Locale.ROOT);
        }
        schema.add(name, entry.getValue());
      }
      return;
    } else {
      final ScalarFunction function =
          ScalarFunctionImpl.create(clazz, Util.first(methodName, "eval"));
      if (function != null) {
        final String name;
        if (functionName != null) {
          name = functionName;
        } else if (upCase) {
          name = methodName.toUpperCase(Locale.ROOT);
        } else {
          name = methodName;
        }
        schema.add(name, function);
        return;
      }
    }
    if (methodName == null) {
      final AggregateFunction aggFunction = AggregateFunctionImpl.create(clazz);
      if (aggFunction != null) {
        schema.add(functionName, aggFunction);
        return;
      }
    }
    throw new RuntimeException("Not a valid function class: " + clazz
        + ". Scalar functions and table macros have an 'eval' method; "
        + "aggregate functions have 'init' and 'add' methods, and optionally "
        + "'initAdd', 'merge' and 'result' methods.");
  }


  protected void executeBeforeSqlQuery(String sql) {
    //
  }

  protected void executeAfterSqlQuery(String sql) {
    //
  }

  public TableExecutionPlan getExecutionPlan(String sql) {
    return sql2ExecutionPlanCache == null
        ? null
        : sql2ExecutionPlanCache.getIfPresent(sql);
  }

  public String getExecutionCode(String sql) {
    return SQL_2_EXECUTION_PLAN_CODE_CACHE.getIfPresent(
        cacheKeyPrefix + sql);
  }

  public DataTable sqlQuery(String sql) {
    try {
      executeBeforeSqlQuery(sql);
      if (sql2ExecutionPlanCache != null) {
        TableExecutionPlan executionPlan = sql2ExecutionPlanCache
            .getIfPresent(
                cacheKeyPrefix + sql);
        if (executionPlan != null) {
          return implement(executionPlan.getBindable(),
              executionPlan.getRowType(),
              calciteCatalogReader.getTypeFactory());
        }
      }
      TableExecutionPlan executionPlan = toExecutionPlan(sql);
      DataTable dt = implement(executionPlan.getBindable(),
          executionPlan.rowType,
          calciteCatalogReader.getTypeFactory());
      if (sql2ExecutionPlanCache != null) {
        sql2ExecutionPlanCache.put(cacheKeyPrefix + sql,
            executionPlan);
      }
      executeAfterSqlQuery(sql);
      return dt;
    } catch (Throwable t) {
      throw new RuntimeException(
          "current rootSchema:" + rootSchema.getTableNames(), t);
    }
  }


  public TableExecutionPlan toExecutionPlan(String sql) throws Throwable {
    RelRoot root = getSqlPlanRel(sql);
    return toExecutionPlan(root, sql);
  }

  protected RelRoot getSqlPlanRel(String sql) throws Throwable {
    try (Planner planner = Frameworks.getPlanner(frameworkConfig)) {
      RelRoot root;
      final SqlNode parsedSqlNode = planner.parse(sql);
      final Pair<SqlNode, RelDataType> validatedSqlNodeAndType = planner
          .validateAndGetType(
              parsedSqlNode);
      root = planner.rel(validatedSqlNodeAndType.getKey());
      final Program program = createProgram();
      //getDesiredTraits
      final RelTraitSet desiredTraits = root.rel.getTraitSet()
          .replace(EnumerableConvention.INSTANCE)
          .replace(root.collation)
          .simplify();

      RelNode logicalRelNode = root.rel;
      final RelNode optimizedRelNode = program.run(
          root.rel.getCluster().getPlanner(), logicalRelNode, desiredTraits,
          Collections.emptyList(), Collections.emptyList());
      root = root.withRel(optimizedRelNode);
      return root;
    }

  }


  private TableExecutionPlan toExecutionPlan(RelRoot root, String sql) {
    EnumerableRel enumerable = (EnumerableRel) root.rel;
    if (!root.isRefTrivial()) {
      final List<RexNode> projects = new ArrayList<>();
      final RexBuilder rexBuilder = enumerable.getCluster().getRexBuilder();
      for (int field : Pair.left(root.fields)) {
        projects.add(rexBuilder.makeInputRef(enumerable, field));
      }
      RexProgram program = RexProgram.create(enumerable.getRowType(),
          projects, null, root.validatedRowType, rexBuilder);
      enumerable = EnumerableCalc.create(enumerable, program);
    }
    Bindable bindable = null;
    final TableExecutionPlan tableExecutionPlan = new TableExecutionPlan();
    Hook.Closeable planHookHandle = Hook.JAVA_PLAN.addThread((code) -> {
      SQL_2_EXECUTION_PLAN_CODE_CACHE.put(cacheKeyPrefix + sql,
          (String) code);
      tableExecutionPlan.setCode((String) code);
    });
    try {
      Prepare.CatalogReader.THREAD_LOCAL.set(calciteCatalogReader);
      try {
        Class clazz = SQL_2_BINDABLE_CLASS_MAP.get(
            cacheKeyPrefix + sql);
        if (clazz != null) {
          bindable = (Bindable) clazz.newInstance();
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
      if (bindable == null) {
        bindable = toBindable(enumerable);
      }
      tableExecutionPlan.setBindable(bindable);
      tableExecutionPlan.setRowType(root.validatedRowType);

    } finally {
      Prepare.CatalogReader.THREAD_LOCAL.remove();
      planHookHandle.close();
    }

    return tableExecutionPlan;
  }

  protected Bindable toBindable(EnumerableRel rel) {
    return EnumerableInterpretable.toBindable(Maps.newHashMap(),
        null, rel, EnumerableRel.Prefer.ARRAY);
  }

  private DataTable implement(Bindable bindable, RelDataType rowType,
      RelDataTypeFactory typeFactory) {
    final DataContext dataContext = new TableDataContexImpl(null, rootSchema,
        (JavaTypeFactory) typeFactory, tableConfig);
    Enumerable<Object[]> enumerableResult = bindable.bind(dataContext);
    List rows = EnumerableDefaults.toList(enumerableResult);
    return new DataTable(rowType, rows);
  }

  protected SqlTypeName getSqlTypeNameForJdbcType(int jdbcType) {
    //FIX Calcite jdbc type converting bug
    SqlTypeName typeName = SqlTypeName.getNameForJdbcType(jdbcType);
    if (jdbcType == Types.LONGVARCHAR) {
      typeName = SqlTypeName.VARCHAR;
    }
    if (jdbcType == Types.SMALLINT || jdbcType == Types.TINYINT) {
      //the type of jdbc value is INTEGER when jdbcType is SMALLINT or TINYINT
      typeName = SqlTypeName.INTEGER;
    }
    return typeName;
  }

  public DataTable fromJdbcResultSet(
      ResultSet resultSet) {
    try {
      final RelDataTypeFactory.Builder builder = calciteCatalogReader
          .getTypeFactory()
          .builder();
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        String columnName = metaData.getColumnLabel(i);
        int jdbcType = metaData.getColumnType(i);
        builder.add(columnName.toUpperCase(),
            getSqlTypeNameForJdbcType(jdbcType)).nullable(true);
      }
      RelDataType rowType = builder.build();
      List<Object[]> convertRows = new ArrayList<>();
      Calendar calendar = Calendar
          .getInstance(tableConfig.getTimeZone(), Locale.ROOT);
      while (resultSet.next()) {
        Object[] row = new Object[columnCount];
        for (int i = 1; i <= columnCount; i++) {
          Object jdbcObject = resultSet.getObject(i);
          Object convertedCalciteObject = TypedValue.ofJdbc(jdbcObject,
              calendar).value;
          row[i - 1] = convertedCalciteObject;
        }
        convertRows.add(row);
      }
      return new DataTable(rowType, convertRows);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public DataTable fromJavaPojoList(List pojoList) {
    return fromJavaPojoList(pojoList, null, false);
  }

  public DataTable fromJavaPojoList(List pojoList,
      Map<String, String> fieldName2SqlColumnNameMap, boolean upperUnderscore) {
    try {
      PojoTypeConverter pojoTypeConverter = new PojoTypeConverter(
          calciteCatalogReader.getTypeFactory(), fieldName2SqlColumnNameMap,
          upperUnderscore, tableConfig.getTimeZone());
      Class pojoClass = null;
      List<Field> fieldList = null;
      RelDataType relDataType = null;
      List rows = new DataTable.DataTableStoreList();
      for (Object pojo : pojoList) {
        if (pojoClass == null) {
          pojoClass = pojo.getClass();
        } else {
          if (!pojoClass.equals(pojo.getClass())) {
            throw new RuntimeException(
                "the classes of elements in pojoList must be same !");
          }
        }
        if (fieldList == null) {
          fieldList = PojoTypeConverter.getAllDeclaredFields(pojoClass);
        }
        PojoRelDataTypeValue relDataTypeValue = pojoTypeConverter
            .getPojoRelDataTypeValue(
                pojoClass, fieldList, pojo);
        rows.add(relDataTypeValue.getValue());
        if (relDataType == null && relDataTypeValue != null) {
          relDataType = relDataTypeValue.getRelDataType();
        }
      }
      return new DataTable(relDataType, rows);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public DataTable fromRowListWithSqlTypeMap(List<Map<String, Object>> rowList,
      Map<String, SqlTypeName> sqlTypeMap) {
    try {
      final RelDataTypeFactory.Builder builder = calciteCatalogReader
          .getTypeFactory()
          .builder();
      PojoTypeConverter pojoTypeConverter = new PojoTypeConverter(
          calciteCatalogReader.getTypeFactory(), null,
          true, tableConfig.getTimeZone());
      RelDataType rowType;
      List convertRows = new DataTable.DataTableStoreList();

      List<String> schemaKeys = new ArrayList<>(sqlTypeMap.keySet());
      schemaKeys.forEach(key -> {
        builder.add(key.toLowerCase(), sqlTypeMap.get(key)).nullable(true);
      });
      rowType = builder.build();

      for (Map<String, Object> map : rowList) {
        int i = 0;
        Object[] row = new Object[sqlTypeMap.size()];
        for (String key : schemaKeys) {

          Object o = map.get(key);
          PojoRelDataTypeValue calciteValue = pojoTypeConverter
              .javaObjectToCalciteObject(
                  sqlTypeMap.get(key), o, tableConfig.getTimeZone(), null);
          row[i] = calciteValue.getValue();
          i++;
        }
        convertRows.add(row);
      }
      return new DataTable(rowType, convertRows);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * the execution plan for a sql query
   */
  public static class TableExecutionPlan {
    private String code;
    private Bindable bindable;
    private RelDataType rowType;

    public TableExecutionPlan() {
    }

    public TableExecutionPlan(String code, Bindable bindable,
        RelDataType rowType) {
      this.code = code;
      this.bindable = bindable;
      this.rowType = rowType;
    }

    public Bindable getBindable() {
      return bindable;
    }

    public RelDataType getRowType() {
      return rowType;
    }

    public String getCode() {
      return code;
    }

    public void setCode(String code) {
      this.code = code;
    }

    public void setBindable(Bindable bindable) {
      this.bindable = bindable;
    }

    public void setRowType(RelDataType rowType) {
      this.rowType = rowType;
    }
  }

}

// End TableEnv.java
