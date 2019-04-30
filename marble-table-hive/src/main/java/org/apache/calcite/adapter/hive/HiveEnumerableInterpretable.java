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

import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.Helper;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.Util;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;

import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

/**
 *
 */
public class HiveEnumerableInterpretable {

  private HiveEnumerableInterpretable() {
  }

  public static Bindable toBindable(Map<String, Object> parameters,
      CalcitePrepare.SparkHandler spark, EnumerableRel rel,
      EnumerableRel.Prefer prefer) {
    HiveEnumerableRelImplementor relImplementor =
        new HiveEnumerableRelImplementor(rel.getCluster().getRexBuilder(),
            parameters);

    final ClassDeclaration expr = relImplementor.implementRoot(rel, prefer);
    String s = Expressions.toString(expr.memberDeclarations, "\n", false);

    if (CalcitePrepareImpl.DEBUG) {
      Util.debugCode(System.out, s);
    }

    Hook.JAVA_PLAN.run(s);

    try {
      if (spark != null && spark.enabled()) {
        return spark.compile(expr, s);
      } else {
        return getBindable(expr, s,
            rel.getRowType().getFieldCount());
      }
    } catch (Exception e) {
      throw Helper.INSTANCE.wrap("Error while compiling generated Java code:\n"
          + s, e);
    }
  }

  static Bindable getBindable(ClassDeclaration expr, String s, int fieldCount)
      throws CompileException, IOException {
    ICompilerFactory compilerFactory;
    try {
      compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
    } catch (Exception e) {
      throw new IllegalStateException(
          "Unable to instantiate java compiler", e);
    }
    IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
    cbe.setClassName(expr.name);
    cbe.setExtendedClass(Utilities.class);
    cbe.setImplementedInterfaces(
        fieldCount == 1
            ? new Class[]{Bindable.class, Typed.class}
            : new Class[]{ArrayBindable.class});
    cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
    if (CalcitePrepareImpl.DEBUG) {
      // Add line numbers to the generated janino class
      cbe.setDebuggingInformation(true, true, true);
    }
    return (Bindable) cbe.createInstance(new StringReader(s));
  }


}

// End HiveEnumerableInterpretable.java
