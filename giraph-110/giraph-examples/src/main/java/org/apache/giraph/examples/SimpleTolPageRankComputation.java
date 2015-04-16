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
 * PageRank with error tolerance termination
 */
@Algorithm(
    name = "PageRank (error tolerance termination)"
)
public class SimpleTolPageRankComputation extends BasicComputation<
    LongWritable, DoubleWritable, NullWritable, DoubleWritable> {
  /** Minimum error tolerance; for async only */
  public static final float MIN_TOLERANCE = 1.0f;

  /** Configurable min error tolerance; for async only */
  public static final FloatConfOption MIN_TOL =
    new FloatConfOption("SimpleTolPageRankComputation.minTol", MIN_TOLERANCE,
                        "The delta/error tolerance to halt at");

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(SimpleTolPageRankComputation.class);

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
    // Since messages (= vertex values) are always positive, we use
    // positive for update+signal and negative for update-only.

    // NOTE: We follow GraphLab's alternative way of computing PageRank,
    // which is to not divide by |V|. To get the probability value at
    // each vertex, take its PageRank value and divide by |V|.
    double oldVal = vertex.getValue().get();
    boolean signalled = false;

    if (getLogicalSuperstep() == 0) {
      vertex.getValue().set(1.0);
      oldVal = 0.0;     // so delta is > 0
      signalled = true;
    } else {
      double sum = 0;
      for (DoubleWritable message : messages) {
        if (message.get() > 0) {
          signalled = true;
        }
        sum += Math.abs(message.get());
      }

      vertex.getValue().set(0.15 + 0.85 * sum);
    }

    double delta = Math.abs(oldVal - vertex.getValue().get());
    boolean converged = delta <= MIN_TOL.get(getConf());

    // send messages only when signalled
    if (delta > 0 && signalled) {
      if (!converged) {
        // update+signal message (need more help)
        sendMessageToAllEdges(vertex,
          new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()));
      } else {
        // update only (I'm done)
        sendMessageToAllEdges(vertex,
          new DoubleWritable(-1.0 * vertex.getValue().get() /
                             vertex.getNumEdges()));
      }
    }

    // always vote to halt
    vertex.voteToHalt();
  }

  /**
   * Value factory context used with {@link SimpleTolPageRankComputation}.
   *
   * Not strictly necessary, since vertex values are overwritten.
   */
  public static class SimpleTolPageRankVertexValueFactory
    extends DefaultVertexValueFactory<DoubleWritable> {
    @Override
    public DoubleWritable newInstance() {
      return new DoubleWritable(1.0);
    }
  }

  /**
   * SimpleTol VertexOutputFormat that supports
   * {@link SimpleTolPageRankComputation}
   */
  public static class SimpleTolPageRankVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new SimpleTolPageRankVertexWriter();
    }

    /**
     * SimpleTol VertexWriter that supports
     * {@link SimpleTolPageRankComputation}
     */
    public class SimpleTolPageRankVertexWriter extends TextVertexWriter {
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
