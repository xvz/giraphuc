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

package org.apache.giraph.partition;

import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Set;

/**
 * YH: Stores and tracks vertex types according to vertex id.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class VertexTypeStore<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /**
   * The possible types of vertices. Used for token serializability.
   */
  public static enum VertexType {
    /** all neighbours are in same partition */
    INTERNAL,
    /** all neighbours are in same worker (but different partitions) */
    LOCAL_BOUNDARY,
    /** all neighbours are in remote workers (NONE in same worker) */
    REMOTE_BOUNDARY,
    /** neighbours in both remote and local workers */
    BOTH_BOUNDARY;
  }

  /** Provided configuration */
  private ImmutableClassesGiraphConfiguration conf;

  // With hash partitioning, edge cuts are extremely high, meaning
  // 99%+ of vertices are usually local/remote/both boundary.
  /** Set of internal vertex ids (owned by this worker only) */
  private Set internalVertices;
  /** Set of local boundary vertex ids (owned by this worker only) */
  private Set localBoundaryVertices;
  /** Set of remote boundary vertex ids (owned by this worker only) */
  private Set remoteBoundaryVertices;

  /**
   * Constructor.
   *
   * @param conf Configuration used.
   */
  public VertexTypeStore(ImmutableClassesGiraphConfiguration conf) {
    this.conf = conf;

    if (conf.getAsyncConf().tokenSerialized()) {
      Class<I> vertexIdClass = conf.getVertexIdClass();
      if (vertexIdClass.equals(IntWritable.class)) {
        internalVertices = new IntOpenHashSet();
        localBoundaryVertices = new IntOpenHashSet();
        remoteBoundaryVertices = new IntOpenHashSet();
      } else if (vertexIdClass.equals(LongWritable.class)) {
        internalVertices = new LongOpenHashSet();
        localBoundaryVertices = new LongOpenHashSet();
        remoteBoundaryVertices = new LongOpenHashSet();
      } else {
        internalVertices = new ObjectOpenHashSet<I>();
        localBoundaryVertices = new ObjectOpenHashSet<I>();
        remoteBoundaryVertices = new ObjectOpenHashSet<I>();
      }
    }
  }

  /**
   * Add/track a vertex and its type.
   *
   * @param vertex Vertex to add
   */
  public void addVertex(Vertex<I, V, E> vertex) {
    boolean isRemoteBoundary = false;
    boolean isLocalBoundary = false;

    int partitionId = conf.getServiceWorker().
      getVertexPartitionOwner(vertex.getId()).getPartitionId();
    WorkerInfo myWorker = conf.getServiceWorker().getWorkerInfo();

    // TODO-YH: assumes undirected graph... for directed graph,
    // need to broadcast to all neighbours + check in-edges
    for (Edge<I, E> e : vertex.getEdges()) {
      PartitionOwner dstOwner = conf.getServiceWorker().
        getVertexPartitionOwner(e.getTargetVertexId());
      int dstPartitionId = dstOwner.getPartitionId();
      WorkerInfo dstWorker = dstOwner.getWorkerInfo();

      // check if neighbour is remote; if not,
      // check if neighbour is in another local partition
      if (!myWorker.equals(dstWorker)) {
        isRemoteBoundary = true;
      } else if (dstPartitionId != partitionId) {
        isLocalBoundary = true;
      }

      // need to check all edges before concluding vertex
      // is ONLY local or remote boundary, but if it's
      // already both, we can quit early
      if (isRemoteBoundary && isLocalBoundary) {
        break;
      }
    }

    if (!isRemoteBoundary && !isLocalBoundary) {
      setVertexType(vertex.getId(), VertexType.INTERNAL);
    } else if (!isRemoteBoundary && isLocalBoundary) {
      setVertexType(vertex.getId(), VertexType.LOCAL_BOUNDARY);
    } else if (isRemoteBoundary && !isLocalBoundary) {
      setVertexType(vertex.getId(), VertexType.REMOTE_BOUNDARY);
    }
    // else BOTH_BOUNDARY is implicit
  }

  /**
   * Set/tag a vertex id with the specified type.
   *
   * @param vertexId Vertex id
   * @param type Vertex type
   */
  public void setVertexType(I vertexId, VertexType type) {
    switch (type) {
    case INTERNAL:
      synchronizedAdd(internalVertices, vertexId);
      return;
    case LOCAL_BOUNDARY:
      synchronizedAdd(localBoundaryVertices, vertexId);
      return;
    case REMOTE_BOUNDARY:
      synchronizedAdd(remoteBoundaryVertices, vertexId);
      return;
    case BOTH_BOUNDARY:
      // local+remote boundary will not be on any of the sets
      // (i.e., absence on all sets indicates this)
      return;
    default:
      throw new RuntimeException("Invalid vertex type!");
    }
  }

  /**
   * Whether a vertex id belongs to a particular vertex type.
   *
   * Thread-safe for concurrent is/getVertexType() calls ONLY.
   * This is NOT thread-safe with concurrent setVertexType() calls!
   *
   * @param vertexId Vertex id
   * @param type Vertex type
   * @return True if vertexId has a matching vertex type
   */
  public boolean isVertexType(I vertexId, VertexType type) {
    // don't need synchronize b/c only time we write
    // to these sets is during input loading or mutations,
    // when no compute threads are running, and we read only
    // during computation (when compute threads are running)
    switch (type) {
    case INTERNAL:
      return contains(internalVertices, vertexId);
    case LOCAL_BOUNDARY:
      return contains(localBoundaryVertices, vertexId);
    case REMOTE_BOUNDARY:
      return contains(remoteBoundaryVertices, vertexId);
    case BOTH_BOUNDARY:
      return !(contains(internalVertices, vertexId) ||
               contains(localBoundaryVertices, vertexId) ||
               contains(remoteBoundaryVertices, vertexId));
    default:
      throw new RuntimeException("Invalid vertex type!");
    }
  }

  /**
   * Get vertex type for specified vertex id.
   *
   * Thread-safe for concurrent is/getVertexType() calls ONLY.
   * This is NOT thread-safe with concurrent setVertexType() calls!
   *
   * @param vertexId Vertex id
   * @return Type of vertex
   */
  public VertexType getVertexType(I vertexId) {
    if (contains(internalVertices, vertexId)) {
      return VertexType.INTERNAL;
    } else if (contains(localBoundaryVertices, vertexId)) {
      return VertexType.LOCAL_BOUNDARY;
    } else if (contains(remoteBoundaryVertices, vertexId)) {
      return VertexType.REMOTE_BOUNDARY;
    } else {
      return VertexType.BOTH_BOUNDARY;
    }
  }

  /**
   * Checks if vertex id exists in a set.
   *
   * @param set Set to check
   * @param vertexId Vertex id to look for
   * @return True if vertexId is in set
   */
  private boolean contains(Set set, I vertexId) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (vertexIdClass.equals(IntWritable.class)) {
      return set.contains(((IntWritable) vertexId).get());
    } else if (vertexIdClass.equals(LongWritable.class)) {
      return set.contains(((LongWritable) vertexId).get());
    } else {
      return set.contains(vertexId);
    }
  }

  /**
   * Adds vertexId to set, synchronizing on set.
   *
   * @param set Set to add to
   * @param vertexId Vertex id to add
   */
  private void synchronizedAdd(Set set, I vertexId) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    synchronized (set) {
      if (vertexIdClass.equals(IntWritable.class)) {
        set.add(((IntWritable) vertexId).get());
      } else if (vertexIdClass.equals(LongWritable.class)) {
        set.add(((LongWritable) vertexId).get());
      } else {
        // need to clone id when not primitive
        set.add(WritableUtils.clone(vertexId, conf));
      }
    }
  }


  /**
   * @return Number of internal vertices.
   */
  public int numInternalVertices() {
    return internalVertices.size();
  }

  /**
   * @return Number of local (only) boundary vertices.
   */
  public int numLocalBoundaryVertices() {
    return localBoundaryVertices.size();
  }

  /**
   * @return Number of remote (only) boundary vertices.
   */
  public int numRemoteBoundaryVertices() {
    return remoteBoundaryVertices.size();
  }
}
