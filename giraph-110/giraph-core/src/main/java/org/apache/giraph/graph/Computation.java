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

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.TypesHolder;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.worker.WorkerAggregatorUsage;
import org.apache.giraph.worker.WorkerContext;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * Interface for an application for computation.
 *
 * During the superstep there can be several instances of this interface,
 * each doing computation on one partition of the graph's vertices.
 *
 * Note that each thread will have its own {@link Computation},
 * so accessing any data from this class is thread-safe.
 * However, accessing global data (like data from {@link WorkerContext})
 * is not thread-safe.
 *
 * Objects of this interface only live for a single superstep.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public interface Computation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable,
    M2 extends Writable>
    extends TypesHolder<I, V, E, M1, M2>,
    ImmutableClassesGiraphConfigurable<I, V, E>,
    WorkerAggregatorUsage {
  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   *                 a life expectancy as long as next() is not called.
   */
  void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
    throws IOException;

  /**
   * Prepare for computation. This method is executed exactly once prior to
   * {@link #compute(Vertex, Iterable)} being called for any of the vertices
   * in the partition.
   */
  void preSuperstep();

  /**
   * Finish computation. This method is executed exactly once after computation
   * for all vertices in the partition is complete.
   */
  void postSuperstep();

  /**
   * Initialize, called by infrastructure before the superstep starts.
   * Shouldn't be called by user code.
   *
   * @param graphState Graph state
   * @param workerClientRequestProcessor Processor for handling requests
   * @param graphTaskManager Graph-wide BSP Mapper for this Vertex
   * @param workerAggregatorUsage Worker aggregator usage
   * @param workerContext Worker context
   */
  void initialize(GraphState graphState,
      WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor,
      GraphTaskManager<I, V, E> graphTaskManager,
      WorkerAggregatorUsage workerAggregatorUsage, WorkerContext workerContext);

  /**
   * Retrieves the current (global) superstep.
   *
   * YH: This is always the number of global supersteps---i.e., supersteps
   * separated by global barriers. This differs from getLogicalSuperstep()
   * only with asynchronous execution and ASYNC_DISABLE_BARRIERS enabled.
   * Otherwise it is identical to getLogicalSuperstep().
   *
   * @return Current (global) superstep
   */
  long getSuperstep();

  /**
   * YH: Retrieves the current logical superstep.
   *
   * If using asynchronous execution with ASYNC_DISABLE_BARRIERS enabled,
   * this will be the LOCAL superstep counter for this worker, which CAN
   * differ from other workers---even if they are in the same global superstep.
   * Otherwise, this is the regular global superstep (same for all workers).
   *
   * @return Current logical superstep
   */
  long getLogicalSuperstep();

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  long getTotalNumVertices();

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  long getTotalNumEdges();

  /**
   * YH: Flag all subsequent messages as being either for the current
   * phase (false) or for the next phase (true).
   *
   * @param forNextPhase True if message should be processed in next phase.
   */
  void setMessagesAreForNextPhase(boolean forNextPhase);

  /**
   * YH: Set the source vertex id for all messages that will be sent.
   * By default, this is the current vertex being processed, or null
   * if no vertex is currently being processed.
   *
   * Shouldn't be called by user code, UNLESS the current vertex being
   * processed is not the source of messages to be sent. In that case,
   * use this to set the correct source id before calling sendMessage*().
   *
   * @param id Id of vertex that will be sending messages.
   */
  void setCurrentSourceId(I id);

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  void sendMessage(I id, M2 message);

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message);

  /**
   * Send a message to multiple target vertex ids in the iterator.
   *
   * @param vertexIdIterator An iterator to multiple target vertex ids.
   * @param message Message sent to all targets in the iterator.
   */
  void sendMessageToMultipleEdges(Iterator<I> vertexIdIterator, M2 message);

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Initial edges
   */
  void addVertexRequest(I id, V value, OutEdges<I, E> edges) throws IOException;

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  void addVertexRequest(I id, V value) throws IOException;

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param vertexId Id of the vertex to be removed.
   */
  void removeVertexRequest(I vertexId) throws IOException;

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next superstep)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   */
  void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) throws IOException;

  /**
   * Request to remove all edges from a given source vertex to a given target
   * vertex (processed just prior to the next superstep).
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   */
  void removeEdgesRequest(I sourceVertexId, I targetVertexId)
    throws IOException;

  /**
   * Get the mapper context
   *
   * @return Mapper context
   */
  Mapper.Context getContext();

  /**
   * Get the worker context
   *
   * @param <W> WorkerContext class
   * @return WorkerContext context
   */
  @SuppressWarnings("unchecked")
  <W extends WorkerContext> W getWorkerContext();
}
