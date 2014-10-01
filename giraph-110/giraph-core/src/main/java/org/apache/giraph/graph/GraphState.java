/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.graph;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Immutable global state of the graph.
 */
public class GraphState {
  /** Graph-wide superstep */
  private final long superstep;
  /** YH: Worker-local logical superstep */
  private final long logicalSuperstep;
  /** Graph-wide number of vertices */
  private final long numVertices;
  /** Graph-wide number of edges */
  private final long numEdges;
  /** Graph-wide map context */
  private final Mapper<?, ?, ?, ?>.Context context;

  // YH: If using asynchronous execution with ASYNC_DISABLE_BARRIERS
  // enabled, "superstep" is the current/cached global superstep while
  // "logicalSuperstep" is current local superstep. "superstep" is used
  // to coordinate workers when global barriers are needed.
  //
  // Otherwise, both values are identical---they both indicate current
  // or cached global superstep.
  //
  // Note that naming is SWITCHED in the user API!! (See graph.Computation)
  // We do this to avoid refactoring large chunks of code.

  /**
   * Constructor
   *
   * @param superstep Current superstep
   * @param logicalSuperstep Current logical superstep
   * @param numVertices Current graph-wide vertices
   * @param numEdges Current graph-wide edges
   * @param context Context
   *
   */
  public GraphState(long superstep, long logicalSuperstep,
                    long numVertices, long numEdges,
      Mapper<?, ?, ?, ?>.Context context) {
    this.superstep = superstep;
    this.logicalSuperstep = logicalSuperstep;
    this.numVertices = numVertices;
    this.numEdges = numEdges;
    this.context = context;
  }

  public long getSuperstep() {
    return superstep;
  }

  public long getLogicalSuperstep() {
    return logicalSuperstep;
  }

  public long getTotalNumVertices() {
    return numVertices;
  }

  public long getTotalNumEdges() {
    return numEdges;
  }

  public Mapper.Context getContext() {
    return context;
  }

  @Override
  public String toString() {
    return "(superstep=" + superstep + ",logicalSuperstep=" +
        logicalSuperstep + ",numVertices=" + numVertices + "," +
        "numEdges=" + numEdges + ",context=" + context + ")";
  }
}
