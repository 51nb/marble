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

import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.runtime.Typed;
import org.apache.calcite.runtime.Utilities;

/**
 *
 */
public class BazJoinExample extends Utilities implements Bindable, Typed {

  public org.apache.calcite.linq4j.Enumerable bind(
      final org.apache.calcite.DataContext root) {
    return org.apache.calcite.schema.Schemas.queryable(root,
        root.getRootSchema().getSubSchema("TEST"), java.lang.Object[].class,
        "T1")
        .asEnumerable()
        .join(org.apache.calcite.schema.Schemas.queryable(root,
            root.getRootSchema(), java.lang.Object[].class, "T2")
                .asEnumerable(),
            new org.apache.calcite.linq4j.function.Function1() {
              public Integer apply(Object[] v1) {
                return (Integer) v1[1];
              }

              public Object apply(Object v1) {
                return apply(
                    (Object[]) v1);
              }
            }
            , new org.apache.calcite.linq4j.function.Function1() {
              public Integer apply(Object[] v1) {
                return (Integer) v1[1];
              }

              public Object apply(Object v1) {
                return apply(
                    (Object[]) v1);
              }
            }
            , new org.apache.calcite.linq4j.function.Function2() {
              public Object[] apply(Object[] left, Object[] right) {
                return new Object[]{
                    left[0],
                    left[1],
                    left[2],
                    left[3],
                    left[4],
                    left[5],
                    left[6],
                    left[7],
                    left[8],
                    left[9],
                    left[10],
                    right[0],
                    right[1],
                    right[2],
                    right[3],
                    right[4],
                    right[5],
                    right[6],
                    right[7],
                    right[8],
                    right[9],
                    right[10]
                };
              }

              public Object[] apply(Object left, Object right) {
                return apply(
                    (Object[]) left,
                    (Object[]) right);
              }
            }
            , null, false, false);
  }

  public Class getElementType() {
    return java.lang.Object[].class;
  }
}

// End BazJoinExample.java
