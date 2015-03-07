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

import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.factories.DefaultVertexValueFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Demonstrates the delta PageRank implementation.
 */
@Algorithm(
    name = "Delta PageRank"
)
public class DeltaPageRankComputation extends BasicComputation<LongWritable,
    DoubleWritable, NullWritable, DoubleWritable> {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;

  /** Configurable max number of supersteps */
  public static final IntConfOption MAX_SS =
    new IntConfOption("DeltaPageRankComputation.maxSS", MAX_SUPERSTEPS,
                      "The maximum number of supersteps");

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(DeltaPageRankComputation.class);

  /** Number of active vertices (for error tolerance) */
  private static String NUM_ACTIVE_AGG = "num_active";

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {

    // NOTE: We follow GraphLab's alternative way of computing PageRank,
    // which is to not divide by |V|. To get the probability value at
    // each vertex, take its PageRank value and divide by |V|.
    double delta = 0;

    if (getLogicalSuperstep() == 0) {
      vertex.getValue().set(0.0);
      delta = 0.15;
    }

    for (DoubleWritable message : messages) {
      delta += message.get();
    }
    vertex.getValue().set(vertex.getValue().get() + delta);

    if (getLogicalSuperstep() < MAX_SS.get(getConf()) && delta > 0) {
      sendMessageToAllEdges(vertex,
          new DoubleWritable(0.85 * delta / vertex.getNumEdges()));
    }

    // always vote to halt
    vertex.voteToHalt();
  }

  /**
   * Value factory context used with {@link DeltaPageRankComputation}.
   *
   * NOTE: Without this, the results will be INCORRECT because missing
   * vertices are added with an initial value of 0 rather than 0.15.
   */
  public static class DeltaPageRankVertexValueFactory
    extends DefaultVertexValueFactory<DoubleWritable> {
    @Override
    public DoubleWritable newInstance() {
      return new DoubleWritable(0.15);
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link DeltaPageRankComputation}
   */
  public static class DeltaPageRankVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new DeltaPageRankVertexWriter();
    }

    /**
     * Simple VertexWriter that supports {@link DeltaPageRankComputation}
     */
    public class DeltaPageRankVertexWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<LongWritable, DoubleWritable, NullWritable> vertex)
        throws IOException, InterruptedException {
        getRecordWriter().write(
            new Text(vertex.getId().toString()),
            new Text(vertex.getValue().toString()));
      }
    }
  }
}
