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

import org.apache.giraph.conf.FloatConfOption;
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
 * Demonstrates the delta PageRank implementation with
 * error tolerance termination.
 */
@Algorithm(
    name = "Delta PageRank (error tolerance termination)"
)
public class DeltaTolPageRankComputation extends BasicComputation<LongWritable,
    DoubleWritable, NullWritable, DoubleWritable> {
  /** Minimum error tolerance; for async only */
  public static final float MIN_TOLERANCE = 1.0f;

  /** Configurable min error tolerance; for async only */
  public static final FloatConfOption MIN_TOL =
    new FloatConfOption("DeltaTolPageRankComputation.minTol", MIN_TOLERANCE,
                        "The delta/error tolerance to halt at");

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(DeltaTolPageRankComputation.class);

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {

    // NOTE: We follow GraphLab's alternative way of computing PageRank,
    // which is to not divide by |V|. To get the probability value at
    // each vertex, take its PageRank value and divide by |V|.
    double delta = 0;

    if (getLogicalSuperstep() == 0) {
      vertex.setValue(new DoubleWritable(0.0));
      delta = 0.15;
    }

    for (DoubleWritable message : messages) {
      delta += message.get();
    }
    vertex.setValue(new DoubleWritable(vertex.getValue().get() + delta));

    // if delta <= tolerance, don't send messages/wake up neighbours
    // (same behaviour as GraphLab async: scatter to no edges when delta < tol)
    if (delta > MIN_TOL.get(getConf())) {
      sendMessageToAllEdges(vertex,
          new DoubleWritable(0.85 * delta / vertex.getNumEdges()));
    }

    // always vote to halt
    vertex.voteToHalt();
  }

  /**
   * Value factory context used with {@link DeltaTolPageRankComputation}.
   *
   * NOTE: Without this, the results will be INCORRECT because missing
   * vertices are added with an initial value of 0 rather than 0.15.
   */
  public static class DeltaTolPageRankVertexValueFactory
    extends DefaultVertexValueFactory<DoubleWritable> {
    @Override
    public DoubleWritable newInstance() {
      return new DoubleWritable(0.15);
    }
  }

  /**
   * Simple VertexOutputFormat that supports {@link DeltaTolPageRankComputation}
   */
  public static class DeltaTolPageRankVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new DeltaTolPageRankVertexWriter();
    }

    /**
     * Simple VertexWriter that supports {@link DeltaTolPageRankComputation}
     */
    public class DeltaTolPageRankVertexWriter extends TextVertexWriter {
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
