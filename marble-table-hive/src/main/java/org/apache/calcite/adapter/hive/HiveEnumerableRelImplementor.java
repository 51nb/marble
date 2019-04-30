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

import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rex.RexBuilder;

import com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HiveEnumerableRelImplementor extends EnumerableRelImplementor {

  public HiveEnumerableRelImplementor(
      RexBuilder rexBuilder,
      Map<String, Object> internalParameters) {
    super(rexBuilder, internalParameters);
  }

  @Override protected Collection<Statement> buildInitialStatements() {
    List<Statement> initialStatements = new ArrayList<>();
    Type typeOfHiveUdfInstanceHolder = new Type() {
      @Override public String getTypeName() {
        return "HiveUDFInstanceHolder";
      }

      @Override public String toString() {
        return "HiveUDFInstanceHolder";
      }
    };
    DeclarationStatement hiveUdfInstanceHolder =
        Expressions.declare(Modifier.FINAL,
            Expressions.parameter(typeOfHiveUdfInstanceHolder,
                "hiveUDFInstanceHolder"),
            Expressions.new_(typeOfHiveUdfInstanceHolder));
    initialStatements.add(hiveUdfInstanceHolder);
    return initialStatements;
  }

  @Override protected void addMemberDeclaration(
      List<MemberDeclaration> memberDeclarations) {
    ClassDeclaration classDeclaration =
        Expressions.classDecl(
            Modifier.PUBLIC | Modifier.STATIC,
            "HiveUDFInstanceHolder",
            null,
            ImmutableList.of(Serializable.class),
            new ArrayList<>());
    classDeclaration.memberDeclarations.addAll(
        HiveUDFInstanceCollecterPerSqlQuery.get()
            .getStashedFieldsForHiveUDFInstanceHolder());
    memberDeclarations.add(classDeclaration);
  }
}

// End HiveEnumerableRelImplementor.java
