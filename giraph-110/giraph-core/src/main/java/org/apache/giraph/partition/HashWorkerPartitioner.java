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

import com.google.common.collect.Lists;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.Collection;
import java.util.List;

/**
 * Implements hash-based partitioning from the id hash code.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class HashWorkerPartitioner<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements WorkerGraphPartitioner<I, V, E> {
  /**
   * Mapping of the vertex ids to {@link PartitionOwner}.
   */
  protected List<PartitionOwner> partitionOwnerList =
      Lists.newArrayList();

  // YH: w/ hash partitioning, edge cuts are extremely high,
  // meaning 99%+ of vertices are usually local+remote boundary.
  /** YH: Set of internal vertex ids (owned by this worker only) */
  private IntOpenHashSet internalVertices = new IntOpenHashSet();
  /** YH: Set of local boundary vertex ids (owned by this worker only) */
  private IntOpenHashSet localBoundaryVertices = new IntOpenHashSet();
  /** YH: Set of remote boundary vertex ids (owned by this worker only) */
  private IntOpenHashSet remoteBoundaryVertices = new IntOpenHashSet();

  @Override
  public PartitionOwner createPartitionOwner() {
    return new BasicPartitionOwner();
  }

  /**
   * YH: Whether a vertex id belongs to a particular vertex type.
   *
   * Thread-safe for concurrent is/getVertexType() calls ONLY.
   * This is NOT thread-safe with concurrent setVertexType() calls!
   *
   * @param vertexId Vertex id
   * @param type Vertex type
   * @return True if vertexId has a matching vertex type
   */
  public boolean isVertexType(I vertexId, VertexType type) {
    boolean isType;

    // YH: don't need synchronize b/c only time we write
    // to these sets is during input loading or mutations,
    // when no compute threads are running, and we read only
    // during computation (when compute threads are running)
    switch (type) {
    case INTERNAL:
      isType = internalVertices.contains(vertexId.hashCode());
      break;
    case LOCAL_BOUNDARY:
      isType = localBoundaryVertices.contains(vertexId.hashCode());
      break;
    case REMOTE_BOUNDARY:
      isType = remoteBoundaryVertices.contains(vertexId.hashCode());
      break;
    case BOTH_BOUNDARY:
      isType = !(internalVertices.contains(vertexId.hashCode()) ||
                 localBoundaryVertices.contains(vertexId.hashCode()) ||
                 remoteBoundaryVertices.contains(vertexId.hashCode()));
      break;
    default:
      throw new RuntimeException("Invalid vertex type!");
    }
    // CHECKSTYLE: resume IndentationCheck

    return isType;
  }

  /**
   * YH: Get vertex type for specified vertex id.
   *
   * Thread-safe for concurrent is/getVertexType() calls ONLY.
   * This is NOT thread-safe with concurrent setVertexType() calls!
   *
   * @param vertexId Vertex id
   * @return Type of vertex
   */
  public VertexType getVertexType(I vertexId) {
    if (internalVertices.contains(vertexId.hashCode())) {
      return VertexType.INTERNAL;
    } else if (localBoundaryVertices.contains(vertexId.hashCode())) {
      return VertexType.LOCAL_BOUNDARY;
    } else if (remoteBoundaryVertices.contains(vertexId.hashCode())) {
      return VertexType.REMOTE_BOUNDARY;
    } else {
      return VertexType.BOTH_BOUNDARY;
    }
  }

  /**
   * YH: Set/tag a vertex id with the specified type.
   *
   * @param vertexId Vertex id
   * @param type Vertex type
   */
  public void setVertexType(I vertexId, VertexType type) {
    // checkstyle has a stupid requirement for nested {}s in cases
    // CHECKSTYLE: stop IndentationCheck
    switch (type) {
    case INTERNAL:
      // TODO-YH: collisions if I is not numeric?
      synchronized (internalVertices) {
        internalVertices.add(vertexId.hashCode());
      }
      return;
    case LOCAL_BOUNDARY:
      synchronized (localBoundaryVertices) {
        localBoundaryVertices.add(vertexId.hashCode());
      }
      return;
    case REMOTE_BOUNDARY:
      synchronized (remoteBoundaryVertices) {
        remoteBoundaryVertices.add(vertexId.hashCode());
      }
      return;
    case BOTH_BOUNDARY:
      // local+remote boundary will not be on any of the sets
      // (i.e., absence on all sets indicates this)
      return;
    default:
      throw new RuntimeException("Invalid vertex type!");
    }
    // CHECKSTYLE: resume IndentationCheck
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

  @Override
  public PartitionOwner getPartitionOwner(I vertexId) {
    return partitionOwnerList.get(
        Math.abs(vertexId.hashCode() % partitionOwnerList.size()));
  }

  @Override
  public Collection<PartitionStats> finalizePartitionStats(
      Collection<PartitionStats> workerPartitionStats,
      PartitionStore<I, V, E> partitionStore) {
    // No modification necessary
    return workerPartitionStats;
  }

  @Override
  public PartitionExchange updatePartitionOwners(
      WorkerInfo myWorkerInfo,
      Collection<? extends PartitionOwner> masterSetPartitionOwners,
      PartitionStore<I, V, E> partitionStore) {
    return PartitionBalancer.updatePartitionOwners(partitionOwnerList,
        myWorkerInfo, masterSetPartitionOwners, partitionStore);
  }

  @Override
  public Collection<? extends PartitionOwner> getPartitionOwners() {
    return partitionOwnerList;
  }
}
