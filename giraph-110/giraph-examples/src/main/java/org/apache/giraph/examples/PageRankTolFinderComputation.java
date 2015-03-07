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

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.aggregators.DoubleMaxAggregator;
import org.apache.giraph.factories.DefaultVertexValueFactory;
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
 * PageRank implementation that finds when the maximum error deltas
 * (between two supersteps) "plateaus".
 *
 * In other words, think of a plot of error-delta vs. superstep-number.
 * The goal is to determine when the function flattens out---this is
 * roughly where we should stop, as additional supersteps won't get
 * us any better of a convergence.
 *
 * As this "break even" point is different for different graphs, this
 * function helps determine what tolerance value should be used.
 */
@Algorithm(
    name = "PageRank Tolerance Finder"
)
public class PageRankTolFinderComputation extends BasicComputation<
    LongWritable, DoubleWritable, NullWritable, DoubleWritable> {
  /** Max number of supersteps */
  public static final IntConfOption MAX_SUPERSTEPS =
    new IntConfOption("PageRankTolFinderComputation.maxSS", 100,
                      "Maximum number of supersteps");

  /** Logger */
  private static final Logger LOG =
      Logger.getLogger(PageRankTolFinderComputation.class);

  /** Max aggregator name */
  private static String MAX_AGG = "max";

  @Override
  public void compute(
      Vertex<LongWritable, DoubleWritable, NullWritable> vertex,
      Iterable<DoubleWritable> messages) throws IOException {

    // NOTE: We follow GraphLab's alternative way of computing PageRank,
    // which is to not divide by |V|. To get the probability value at
    // each vertex, take its PageRank value and divide by |V|.
    double oldVal = vertex.getValue().get();

    if (getLogicalSuperstep() == 0) {
      // FIX: initial value is 1/|V| (or 1), not 0.15/|V| (or 0.15)
      vertex.getValue().set(1.0);
      //set(0.15f / getTotalNumVertices());

    } else {
      double sum = 0;
      for (DoubleWritable message : messages) {
        sum += message.get();
      }
      vertex.getValue().set(0.15f + 0.85f * sum);
      //set((0.15f / getTotalNumVertices()) + 0.85f * sum);
    }

    aggregate(MAX_AGG,
              new DoubleWritable(Math.abs(oldVal - vertex.getValue().get())));

    // Termination condition based on max supersteps
    if (getLogicalSuperstep() < MAX_SUPERSTEPS.get(getConf())) {
      sendMessageToAllEdges(vertex,
          new DoubleWritable(vertex.getValue().get() / vertex.getNumEdges()));
    } else {
      vertex.voteToHalt();
    }
  }

  /**
   * Value factory context used with {@link PageRankTolFinderComputation}.
   *
   * NOTE: Without this, the results will be INCORRECT because missing
   * vertices are added with an initial value of 0 rather than 1.0.
   */
  public static class PageRankTolFinderVertexValueFactory
    extends DefaultVertexValueFactory<DoubleWritable> {
    @Override
    public DoubleWritable newInstance() {
      return new DoubleWritable(1.0);
    }
  }

  /**
   * Master compute associated with {@link PageRankTolFinderComputation}.
   * It registers required aggregators.
   */
  public static class PageRankTolFinderMasterCompute extends
      DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
        IllegalAccessException {
      registerAggregator(MAX_AGG, DoubleMaxAggregator.class);
    }

    @Override
    public void compute() {
      // this is result of aggregators from the *previous* superstep
      if (getSuperstep() >= 0) {
        LOG.info("SS " + getSuperstep() + " max change: " +
                 ((DoubleWritable) getAggregatedValue(MAX_AGG)).get());
      }
    }
  }

  /**
   * Simple VertexOutputFormat that supports
   * {@link PageRankTolFinderComputation}
   */
  public static class PageRankTolFinderVertexOutputFormat extends
      TextVertexOutputFormat<LongWritable, DoubleWritable, NullWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      return new PageRankTolFinderVertexWriter();
    }

    /**
     * Simple VertexWriter that supports {@link PageRankTolFinderComputation}
     */
    public class PageRankTolFinderVertexWriter extends TextVertexWriter {
      @Override
      public void writeVertex(
          Vertex<LongWritable, DoubleWritable, NullWritable> vertex)
        throws IOException, InterruptedException {
        // YH: can be commented out if results are not needed
        getRecordWriter().write(
            new Text(vertex.getId().toString()),
            new Text(vertex.getValue().toString()));
      }
    }
  }
}
