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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 *
 */
public class HiveUDAFParticipator implements Comparable {

  private GenericUDAFEvaluator genericUDAFEvaluator;
  private ObjectInspector outputObjectInspector;
  private GenericUDAFEvaluator.AggregationBuffer aggregationBuffer;

  public GenericUDAFEvaluator getGenericUDAFEvaluator() {
    return genericUDAFEvaluator;
  }

  public void setGenericUDAFEvaluator(
      final GenericUDAFEvaluator genericUDAFEvaluator) {
    this.genericUDAFEvaluator = genericUDAFEvaluator;
  }

  public ObjectInspector getOutputObjectInspector() {
    return outputObjectInspector;
  }

  public void setOutputObjectInspector(
      final ObjectInspector outputObjectInspector) {
    this.outputObjectInspector = outputObjectInspector;
  }

  public GenericUDAFEvaluator.AggregationBuffer getAggregationBuffer() {
    return aggregationBuffer;
  }

  public void setAggregationBuffer(
      final GenericUDAFEvaluator.AggregationBuffer aggregationBuffer) {
    this.aggregationBuffer = aggregationBuffer;
  }

  @Override public int compareTo(Object o) {
    int c = -1;
    if (o instanceof HiveUDAFParticipator && o == this) {
      c = 0;
    }
    return c;
  }
}
// End HiveUDAFParticipator.java
