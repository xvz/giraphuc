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
//import org.apache.giraph.conf.FloatConfOption;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.factories.DefaultVertexValueFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.master.DefaultMasterCompute;
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
  ///** Minimum error tolerance; for async only */
  //public static final float MIN_TOLERANCE = 1.0f;

  /** Configurable max number of supersteps */
  public static final IntConfOption MAX_SS =
    new IntConfOption("DeltaPageRankComputation.maxSS", MAX_SUPERSTEPS,
                      "The maximum number of supersteps");
  ///** Configurable min error tolerance; for async only */
  //public static final FloatConfOption MIN_TOL =
  //  new FloatConfOption("DeltaPageRankComputation.minTol", MIN_TOLERANCE,
  //                      "The delta/error tolerance to halt at");

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
      vertex.setValue(new DoubleWritable(0.0));
      delta = 0.15;
    }

    // termination when using error tolerance; must wait at least 1SS
    // b/c tolerances >1.0 will cause immediate termination
    //if (getLogicalSuperstep() > 1 &&
    //    ((LongWritable) getAggregatedValue(NUM_ACTIVE_AGG)).get() == 0) {
    //  vertex.voteToHalt();
    //  return;
    //}

    for (DoubleWritable message : messages) {
      delta += message.get();
    }

    // termination when using supersteps
    if (getLogicalSuperstep() < MAX_SS.get(getConf()) && delta > 0) {
      vertex.setValue(new DoubleWritable(vertex.getValue().get() + delta));
      sendMessageToAllEdges(vertex,
          new DoubleWritable(0.85 * delta / vertex.getNumEdges()));
    }

    // for error tolerance
    //if (delta > 0) {
    //  vertex.setValue(new DoubleWritable(vertex.getValue().get() + delta));
    //  sendMessageToAllEdges(vertex,
    //      new DoubleWritable(0.85 * delta / vertex.getNumEdges()));
    //}
    //
    //if (delta > MIN_TOL.get(getConf())) {
    //  aggregate(NUM_ACTIVE_AGG, new LongWritable(1));
    //}

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
   * Master compute associated with {@link DeltaPageRankComputation}.
   * It registers required aggregators.
   */
  public static class DeltaPageRankMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      // YH: for tolerance
      registerAggregator(NUM_ACTIVE_AGG, LongSumAggregator.class);
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
