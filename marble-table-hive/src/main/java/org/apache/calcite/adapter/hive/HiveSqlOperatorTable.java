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


import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.WindowFunctionDescription;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseUnary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLead;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDTIMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPDTIPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNumericMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNumericPlus;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Sets;

import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>register hive operators and define implementors for operators
 * if we can't look up a operator from it,then fallback to
 * {@link SqlStdOperatorTable}
 * <p>the parser impl will use a  HiveSqlOperatorTable instead of
 * SqlStdOperatorTable to create
 * {@link org.apache.calcite.sql.SqlCall}
 * see
 * {@link org.apache.calcite.adapter.hive.HiveSqlParserImpl#createCall(SqlIdentifier, SqlParserPos, SqlFunctionCategory, SqlLiteral, SqlNode[])}
 * we rewrite the method 'createCall' of
 * {@link org.apache.calcite.sql.parser.SqlAbstractParserImpl} in our Parser.jj
 */
public class HiveSqlOperatorTable extends ReflectiveSqlOperatorTable {


  private static HiveSqlOperatorTable instance;
  private static Set<String> hiveFunctionPackages = new HashSet<>();
  private ArrayListMultimap<String, Class> methodsUDF =
      ArrayListMultimap.create();
  private ArrayListMultimap<String, Class> methodsUDAF =
      ArrayListMultimap.create();

  private static final List<Class> EXCLUDED_HIVE_UDF_LIST = Arrays.asList(
      GenericUDFOPNumericMinus.class, GenericUDFOPDTIMinus.class,
      GenericUDFOPNumericPlus.class, GenericUDFOPDTIPlus.class,
      GenericUDFOPNot.class, GenericUDFLead.class, GenericUDFLag.class);

  private static final List<Class> EXCLUDED_HIVE_UDAF_LIST = Arrays.asList(
      GenericUDAFCount.class, GenericUDAFSum.class);

  private List<SqlOperator> operatorListOfSqlStdOperatorTable =
      SqlStdOperatorTable
          .instance()
          .getOperatorList();
  private ArrayListMultimap<String, SqlOperator>
      operatorMapOfSqlStdOperatorTable = ArrayListMultimap
      .create();

  /**
   * see Parser.jj
   * HiveSqlOperatorTable.CAST.createCall(s.end(this), args)
   */
  public static final SqlFunction CAST = new HiveSqlCastFunction();

  public static final SqlSpecialOperator RLIKE =
      new SqlLikeOperator("RLIKE", SqlKind.SIMILAR, false);

  public static final SqlSpecialOperator NOT_RLIKE =
      new SqlLikeOperator("NOT RLIKE", SqlKind.SIMILAR, true);

  public static final SqlSpecialOperator REGEXP =
      new SqlLikeOperator("REGEXP", SqlKind.SIMILAR, false);

  public static final SqlSpecialOperator NOT_REGEXP =
      new SqlLikeOperator("NOT REGEXP", SqlKind.SIMILAR, true);

  /**
   * register user-defined hive UDF packages to scan
   */
  public static synchronized void registerHiveFunctionPackages(
      String... udfPackages) {
    hiveFunctionPackages.addAll(Arrays.asList(udfPackages));
  }

  public HiveSqlOperatorTable() {
    for (SqlOperator op : operatorListOfSqlStdOperatorTable) {
      operatorMapOfSqlStdOperatorTable.put(op.getName() + "_" + op.getSyntax(),
          op);
    }
    //register hive operator
    ConfigurationBuilder builder = new ConfigurationBuilder();
    hiveFunctionPackages.add("org.apache.hadoop.hive.ql");
    builder.forPackages(hiveFunctionPackages.toArray(new String[0]));
    builder.setExpandSuperTypes(false);
    Reflections reflections = new Reflections(builder);
    registerUDF(reflections);
    registerUDAF(reflections);
  }


  public static synchronized HiveSqlOperatorTable instance() {
    if (instance == null) {
      instance = new HiveSqlOperatorTable();
      instance.init();
      instance.defineImplementors();

    }
    return instance;
  }

  private void defineImplementors() {
    //define implementors for hive operators
    final List<SqlOperator> operatorList = getOperatorList();
    RexImpTable.INSTANCE.defineImplementors((map, aggMap, winAggMap) -> {
      for (SqlOperator sqlOperator : operatorList) {
        if (sqlOperator instanceof HiveSqlAggFunction) {
          HiveSqlAggFunction aggFunction = (HiveSqlAggFunction) sqlOperator;
          aggMap.put(aggFunction, () -> new HiveUDAFImplementor(aggFunction));
        } else {
          /**since SqlOperator is identified by name and kind ,see
           *  {@link SqlOperator#equals(Object)} and
           *  {@link SqlOperator#hashCode()},
           *  we can override implementors of operators that declared in
           *  SqlStdOperatorTable
           *  */
          CallImplementor callImplementor;
          if (sqlOperator.getName().equals("NOT RLIKE") || sqlOperator.getName()
              .equals("NOT REGEXP")) {
            callImplementor =
                RexImpTable.createImplementor(
                    RexImpTable.NotImplementor.of(
                        new HiveUDFImplementor()), NullPolicy.STRICT, false);
          } else {
            callImplementor =
                RexImpTable.createImplementor(
                    new HiveUDFImplementor(), NullPolicy.NONE, false);
          }
          map.put(sqlOperator, callImplementor);

        }

      }
      // directly override some implementors of SqlOperator that declared in
      // SqlStdOperatorTable
      map.put(SqlStdOperatorTable.ITEM,
          new RexImpTable.ItemImplementor(true));
    });


  }

  private void registerUDF(Reflections reflections) {
    Set<Class> udfClasses = Sets.union(
        reflections.getSubTypesOf(GenericUDF.class),
        reflections.getSubTypesOf(UDF.class));
    for (Class clazz : udfClasses) {
      Description desc = (Description) clazz.getAnnotation(Description.class);
      //ignore the operators that need to be excluded
      if (desc == null || EXCLUDED_HIVE_UDF_LIST.contains(clazz)) {
        continue;
      }
      String[] names = desc.name().split(",");
      for (int i = 0; i < names.length; i++) {
        names[i] = names[i].trim();
      }
      for (String name : names) {
        String upName = name.toUpperCase();
        registerUDF(upName, clazz);
      }
    }
  }

  private void registerUDF(String upName, Class clazz) {
    SqlSyntax[] sqlSyntaxArray = getSqlSyntaxInCalcite(clazz);
    for (SqlSyntax sqlSyntax : sqlSyntaxArray) {
      methodsUDF.put(upName + "_" + sqlSyntax, clazz);
      SqlOperator operatorInSqlStdOperatorTable =
          getOperatorInSqlStdOperatorTable(
              upName, sqlSyntax, false);
      if (operatorInSqlStdOperatorTable == null) {
        register(
            new HiveSqlFunction(upName,
                HiveSqlUDFReturnTypeInference.INSTANCE));
      } else {
        /**
         * Calcite will try to lookup for a new SqlOperator from OP Table
         * to replace the default SqlOperator when deriving Type,
         * it gives the chance to inject customized operators.
         * see details in {@link SqlOperator#deriveType}:
         * <code>
         *      final SqlOperator sqlOperator =
         *         SqlUtil.lookupRoutine(validator.getOperatorTable(),
         *         getNameAsId(),
         *             argTypes, null, null, getSyntax(), getKind());
         *
         *     ((SqlBasicCall) call).setOperator(sqlOperator);
         * </code>
         */
        if (operatorInSqlStdOperatorTable.getSyntax()
            == SqlSyntax.FUNCTION) {
          /**
           * if the target operator already exists in SqlStdOperatorTable and
           * it's SqlSyntax is a Function, we build a new HiveSqlFunction to
           * decorate it and keep the original SqlKind of the function, because the
           * calcite query optimizer will use the kind to choose if to perform
           * optimizing.
           **/
          if (operatorInSqlStdOperatorTable == SqlStdOperatorTable.TRIM) {
            break;
          }
          SqlFunction functionInStd =
              (SqlFunction) operatorInSqlStdOperatorTable;
          SqlOperator newOp = new HiveSqlFunction(
              functionInStd.getNameAsId(),
              functionInStd.getKind(),
              HiveSqlUDFReturnTypeInference.INSTANCE,
              functionInStd.getFunctionType());
          register(newOp);
        } else {
          SqlOperator newOp;
          switch (sqlSyntax) {
          case BINARY:
            if (operatorInSqlStdOperatorTable
                == SqlStdOperatorTable.PERCENT_REMAINDER) {
              /**
               *  use the default operator, SqlStdOperatorTable
               *  .PERCENT_REMAINDER
               *  see{@link ReflectiveConvertletTable#addAlias}
               */
              break;
            }
            newOp = new SqlBinaryOperator(upName,
                operatorInSqlStdOperatorTable.getKind(),
                operatorInSqlStdOperatorTable.getLeftPrec(),
                operatorInSqlStdOperatorTable.getRightPrec(),
                HiveSqlUDFReturnTypeInference.INSTANCE, null,
                HiveSqlFunction.ArgChecker.INSTANCE);
            register(newOp);
            break;
          case PREFIX:
            newOp = new SqlPrefixOperator(upName,
                operatorInSqlStdOperatorTable.getKind(),
                operatorInSqlStdOperatorTable.getLeftPrec(),
                operatorInSqlStdOperatorTable.getRightPrec(),
                HiveSqlUDFReturnTypeInference.INSTANCE, null,
                HiveSqlFunction.ArgChecker.INSTANCE);
            register(newOp);
            break;
          case SPECIAL:
            break;
          default:
            break;
          }
        }
      }
    }
  }

  private void registerUDAF(Reflections reflections) {
    Set<Class> udafClasses = Sets.union(
        reflections.getSubTypesOf(GenericUDAFResolver2.class),
        reflections.getSubTypesOf(UDAF.class));
    for (Class clazz : udafClasses) {
      boolean isWindowFunc = false;
      Description desc = (Description) clazz.getAnnotation(Description.class);
      WindowFunctionDescription windowFunctionDescription = null;
      if (desc == null) {
        windowFunctionDescription =
            (WindowFunctionDescription) clazz
                .getAnnotation(WindowFunctionDescription.class);
        if (windowFunctionDescription != null
            && windowFunctionDescription.description() != null) {
          desc = windowFunctionDescription.description();
          isWindowFunc = true;
        }
      }
      //ignore the operators that need to be excluded
      if (desc == null || isWindowFunc || EXCLUDED_HIVE_UDAF_LIST.contains(
          clazz)) {
        continue;
      }
      String[] names = desc.name().split(",");
      for (int i = 0; i < names.length; i++) {
        String upName = names[i].toUpperCase();
        methodsUDAF.put(upName, clazz);
        SqlAggFunction aggOperatorInSqlStdOperatorTable =
            (SqlAggFunction) getOperatorInSqlStdOperatorTable(
                upName, SqlSyntax.FUNCTION, true);
        HiveSqlAggFunction sqlAggFunction;
        if (aggOperatorInSqlStdOperatorTable == null) {
          sqlAggFunction = new HiveSqlAggFunction(upName, false,
              false, HiveSqlUDAFReturnTypeInference.INSTANCE);

        } else {
          sqlAggFunction = new HiveSqlAggFunction(upName,
              aggOperatorInSqlStdOperatorTable.getNameAsId(),
              aggOperatorInSqlStdOperatorTable.getKind(),
              aggOperatorInSqlStdOperatorTable.getFunctionType(),
              false, false, HiveSqlUDAFReturnTypeInference.INSTANCE);
        }
        register(sqlAggFunction);
      }

    }
  }

  private SqlSyntax[] getSqlSyntaxInCalcite(Class hiveUDFClass) {
    if (GenericUDFBaseBinary.class.isAssignableFrom(hiveUDFClass)) {
      return new SqlSyntax[]{SqlSyntax.BINARY, SqlSyntax.FUNCTION};
    } else if (GenericUDFBaseUnary.class.isAssignableFrom(hiveUDFClass)) {
      return new SqlSyntax[]{SqlSyntax.PREFIX, SqlSyntax.FUNCTION};
    } else if (hiveUDFClass.equals(UDFRegExp.class)) {
      //rlike
      return new SqlSyntax[]{SqlSyntax.SPECIAL, SqlSyntax.FUNCTION};
    } else {
      //we treat other udf classes as SqlSyntax.FUNCTION
      return new SqlSyntax[]{SqlSyntax.FUNCTION};
    }
  }

  private SqlOperator getOperatorInSqlStdOperatorTable(String opName,
      SqlSyntax sqlSyntax, boolean isAgg) {
    List<SqlOperator> ops = operatorMapOfSqlStdOperatorTable.get(
        opName + "_" + sqlSyntax);
    for (SqlOperator op : ops) {
      if (isAgg && op instanceof SqlAggFunction) {
        return op;
      }
      if (!isAgg && !(op instanceof SqlAggFunction)) {
        return op;
      }
    }
    return null;
  }


  @Override public void lookupOperatorOverloads(final SqlIdentifier opName,
      final SqlFunctionCategory category, final SqlSyntax syntax,
      final List<SqlOperator> operatorList) {
    super.lookupOperatorOverloads(opName, category, syntax, operatorList);
    if (operatorList.size() == 0) {
      SqlStdOperatorTable.instance()
          .lookupOperatorOverloads(opName, category, syntax, operatorList);
    }

  }


  public Class getHiveUDFClass(String name, SqlSyntax sqlSyntax) {
    String key = name + "_" + sqlSyntax;
    if (methodsUDF.containsKey(key)) {
      return methodsUDF.get(key).get(0);
    }
    return null;
  }

  public Class getHiveUDAFClass(String name) {
    if (methodsUDAF.containsKey(name)) {
      return methodsUDAF.get(name).get(0);
    }
    return null;
  }

}

// End HiveSqlOperatorTable.java
