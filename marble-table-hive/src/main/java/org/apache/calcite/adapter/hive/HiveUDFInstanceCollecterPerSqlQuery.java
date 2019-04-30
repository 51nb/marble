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

import org.apache.calcite.linq4j.tree.MemberDeclaration;

import java.util.ArrayList;
import java.util.List;

/**
 * collect all declared hive udf instance per sql query,to help
 * {@link HiveEnumerableRelImplementor#addMemberDeclaration(List)}
 * to generate code for a EnumerableRel
 */
public class HiveUDFInstanceCollecterPerSqlQuery {

  private HiveUDFInstanceCollecterPerSqlQuery() {
  }

  private static final ThreadLocal<HiveUDFInstanceCollecterPerSqlQuery> LOCAL
      = ThreadLocal
      .withInitial(HiveUDFInstanceCollecterPerSqlQuery::new);

  /**
   * get context.
   *
   * @return context
   */
  public static HiveUDFInstanceCollecterPerSqlQuery get() {
    return LOCAL.get();
  }

  /**
   * remove context.
   */
  public static void clear() {
    LOCAL.remove();
  }


  private List<MemberDeclaration> stashedFieldsForHiveUDFInstanceHolder = new
      ArrayList<>();

  private int sizeOfHiveUDFInstance = 0;


  public List<MemberDeclaration> getStashedFieldsForHiveUDFInstanceHolder() {
    return stashedFieldsForHiveUDFInstanceHolder;
  }


  public int getSizeOfStashedHiveUDFInstance() {
    return sizeOfHiveUDFInstance;
  }

  public void incrementSizeOfStashedHiveUDFInstance() {
    sizeOfHiveUDFInstance++;
  }


}

// End HiveUDFInstanceCollecterPerSqlQuery.java
