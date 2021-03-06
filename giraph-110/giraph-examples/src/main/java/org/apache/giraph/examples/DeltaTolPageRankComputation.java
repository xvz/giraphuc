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

    // YH: We'll use a trick to match how GraphLab async performs
    // PageRank w/ error tolerance termination.
    //
    // Unlike GraphLab async, which can directly pull from neighbours,
    // we always need to send messages to keep neighbours up-to-date.
    // However, this also wakes up neighbours, which is not desirable.
    //
    // So we use two types of messages:
    // - update + signal => do more work to help me converge
    //   (equivalent to GraphLab's scatter/signal)
    // - update only => here's my final delta, I'm done
    //   (implicit in GraphLab's gather)
    //
    // Since deltas are always positive, we use positive value for
    // update+signal and negative for update-only.

    // NOTE: We follow GraphLab's alternative way of computing PageRank,
    // which is to not divide by |V|. To get the probability value at
    // each vertex, take its PageRank value and divide by |V|.
    double delta = 0;
    boolean signalled = false;

    if (getLogicalSuperstep() == 0) {
      vertex.getValue().set(0.0);
      delta = 0.15;
      signalled = true;
    }

    for (DoubleWritable message : messages) {
      if (message.get() > 0) {
        signalled = true;
      }
      delta += Math.abs(message.get());
    }

    vertex.getValue().set(vertex.getValue().get() + delta);
    boolean converged = delta <= MIN_TOL.get(getConf());

    // send messages only when signalled
    if (delta > 0 && signalled) {
      if (!converged) {
        // update+signal message (need more help)
        sendMessageToAllEdges(vertex,
          new DoubleWritable(0.85 * delta / vertex.getNumEdges()));
      } else {
        // update only (I'm done)
        sendMessageToAllEdges(vertex,
          new DoubleWritable(-0.85 * delta / vertex.getNumEdges()));
      }
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
