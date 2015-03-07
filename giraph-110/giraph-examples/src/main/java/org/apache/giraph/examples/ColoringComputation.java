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

package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

import java.io.IOException;

/**
 * YH: Greedy coloring algorithm. Requires async + serializability.
 * Input graph must be undirected.
 */
@Algorithm(
    name = "Coloring",
    description = "Colors vertices s.t. no neighbours have same color"
)
public class ColoringComputation extends BasicComputation<
    LongWritable, LongWritable, NullWritable, LongWritable> {

  /** Sentinel value for "no color" */
  public static final long NO_COLOR = -1;

  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(ColoringComputation.class);

  @Override
  public void compute(
      Vertex<LongWritable, LongWritable, NullWritable> vertex,
      Iterable<LongWritable> messages) throws IOException {

    // initialization (no serializability in first SS)
    if (getLogicalSuperstep() == 0) {
      vertex.getValue().set(NO_COLOR);
      return;
    }

    if (vertex.getValue().get() == NO_COLOR) {
      // get neighbour's colours
      LongOpenHashSet conflicts = new LongOpenHashSet(vertex.getNumEdges());
      for (LongWritable message : messages) {
        conflicts.add(message.get());
      }

      // acquire a new color
      for (long i = 0; i < conflicts.size() + 1; i++) {
        if (!conflicts.contains(i)) {
          vertex.getValue().set(i);
          break;
        }
      }

      if (vertex.getValue().get() == NO_COLOR) {
        LOG.fatal("[[COLOR]] vid=" + vertex.getId() + " no suitable colors!");
        throw new IllegalStateException("No suitable colors!");
      }

      // broadcast change to all neighbours
      for (Edge<LongWritable, NullWritable> e : vertex.getEdges()) {
        // skip self-loops
        if (e.getTargetVertexId().get() == vertex.getId().get()) {
          continue;
        }
        sendMessage(e.getTargetVertexId(), vertex.getValue());
      }
    } else {
      // we should NOT get a conflict any more! (this check is optional)
      for (LongWritable message : messages) {
        if (message.get() == vertex.getValue().get()) {
          LOG.fatal("[[COLOR]] vid=" + vertex.getId() +
                    " unexpected conflict!");
          throw new IllegalStateException("Unexpected conflict!");
        }
      }
    }

    vertex.voteToHalt();
  }
}
