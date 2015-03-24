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

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.requests.SendTokenDepRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerInfo;
import org.apache.giraph.utils.ByteArrayVertexIdNullData;
import org.apache.giraph.utils.VertexIdData;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
    MIXED_BOUNDARY;
  }

  /** Class logger */
  private static final Logger LOG = Logger.getLogger(VertexTypeStore.class);

  /** Provided configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  // With hash partitioning, edge cuts are extremely high, meaning
  // 99%+ of vertices are usually local/remote/mixed boundary.
  //
  // However, we can only implicitly track internal vertices
  // (i.e., not being in the sets => vertex is internal), because
  // we choose to send messages only for notifying dependencies
  // and no messages otherwise.
  /** Set of local boundary vertex ids (owned by this worker only) */
  private Set localBoundaryVertices;
  /** Set of remote boundary vertex ids (owned by this worker only) */
  private Set remoteBoundaryVertices;
  /** Set of mixed boundary vertex ids (owned by this worker only) */
  private Set mixedBoundaryVertices;

  /**
   * Constructor.
   *
   * @param conf Configuration used.
   * @param serviceWorker Service worker
   */
  public VertexTypeStore(
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.serviceWorker = serviceWorker;

    if (conf.getAsyncConf().tokenSerialized()) {
      Class<I> vertexIdClass = conf.getVertexIdClass();
      if (vertexIdClass.equals(IntWritable.class)) {
        localBoundaryVertices = new IntOpenHashSet();
        remoteBoundaryVertices = new IntOpenHashSet();
        mixedBoundaryVertices = new IntOpenHashSet();
      } else if (vertexIdClass.equals(LongWritable.class)) {
        localBoundaryVertices = new LongOpenHashSet();
        remoteBoundaryVertices = new LongOpenHashSet();
        mixedBoundaryVertices = new LongOpenHashSet();
      } else {
        localBoundaryVertices = new ObjectOpenHashSet<I>();
        remoteBoundaryVertices = new ObjectOpenHashSet<I>();
        mixedBoundaryVertices = new ObjectOpenHashSet<I>();
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

    int partitionId = serviceWorker.
      getVertexPartitionOwner(vertex.getId()).getPartitionId();
    WorkerInfo myWorker = serviceWorker.getWorkerInfo();

    // handle out-edge depedencies first
    // in-edge handled later via sendDependencies()
    for (Edge<I, E> e : vertex.getEdges()) {
      PartitionOwner dstOwner = serviceWorker.
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
      // already both/mixed, we can quit early
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
    } else {
      setVertexType(vertex.getId(), VertexType.MIXED_BOUNDARY);
    }
  }

  /**
   * For every vertex, send message to neighbours that are not
   * on the same partition. Required for directed graphs.
   *
   * This ensures that every vertex is categorized based on
   * both their in-edge and out-edge neighbours.
   */
  public void sendDependencies() {
    // when to flush cache/send message to particular worker
    int maxMessagesSizePerWorker =
      GiraphConfiguration.MAX_MSG_REQUEST_SIZE.get(conf);

    // cache for messages that will be sent to neighbours
    // (much more efficient than using SendDataCache b/c
    //  we don't need to store/know dst partition ids)
    Int2ObjectOpenHashMap<VertexIdData<I, NullWritable>> msgCache =
      new Int2ObjectOpenHashMap<VertexIdData<I, NullWritable>>();

    int taskId = serviceWorker.getWorkerInfo().getTaskId();
    PartitionStore<I, V, E> pStore = serviceWorker.getPartitionStore();

    // don't need to synchronize, b/c this is all single threaded
    for (int partitionId : pStore.getPartitionIds()) {
      for (Vertex<I, V, E> vertex : pStore.getOrCreatePartition(partitionId)) {
        for (Edge<I, E> e : vertex.getEdges()) {
          PartitionOwner dstOwner = serviceWorker.
            getVertexPartitionOwner(e.getTargetVertexId());
          int dstPartitionId = dstOwner.getPartitionId();
          int dstTaskId = dstOwner.getWorkerInfo().getTaskId();

          if (dstPartitionId == partitionId) {
            continue;    // skip, no dependency
          } else if (taskId == dstTaskId) {
            // local dependency
            receiveDependency(e.getTargetVertexId(), true);
          } else {
            // remote dependency
            // We may send redundant dependency messages, but
            // it's faster to send it than to prune it.

            VertexIdData<I, NullWritable> messages = msgCache.get(dstTaskId);
            if (messages == null) {
              messages = new ByteArrayVertexIdNullData<I>();
              messages.setConf(conf);
              messages.initialize();
              msgCache.put(dstTaskId, messages);
            }

            // no data---messages are vertex id only
            messages.add(e.getTargetVertexId(), NullWritable.get());

            if (messages.getSize() > maxMessagesSizePerWorker) {
              msgCache.remove(dstTaskId);
              serviceWorker.getWorkerClient().sendWritableRequest(
                  dstTaskId, new SendTokenDepRequest(messages));
            }
          }
        }
      }
    }

    // send remaining messages
    for (int dstTaskId : msgCache.keySet()) {
      // no need to remove, map will be trashed entirely
      VertexIdData<I, NullWritable> messages = msgCache.get(dstTaskId);
      serviceWorker.getWorkerClient().sendWritableRequest(
          dstTaskId, new SendTokenDepRequest(messages));
    }

    // flush network
    serviceWorker.getWorkerClient().waitAllRequests();
  }

  /**
   * Process a received dependency.
   *
   * @param vertexId Id of vertex that has dependency
   * @param isLocal True of dependency is local (within same worker)
   */
  public synchronized void receiveDependency(I vertexId, boolean isLocal) {
    // Function must be synchronized b/c of concurrent
    // gets and mutations to multiple sets
    VertexType type = getVertexType(vertexId);

    if (isLocal) {
      // remote -> mixed, internal -> local
      // (remote -> mixed is b/c we now have local dependency)
      switch (type) {
      case REMOTE_BOUNDARY:
        synchronizedRemove(remoteBoundaryVertices, vertexId);
        synchronizedAdd(mixedBoundaryVertices, vertexId);
        return;
      case LOCAL_BOUNDARY:
        // fall through
      case MIXED_BOUNDARY:
        return;
      default:
        synchronizedAdd(localBoundaryVertices, vertexId);
        return;
      }
    } else {
      // local -> mixed, internal -> remote
      // (local -> mixed is b/c we now have remote dependency)
      switch(type) {
      case LOCAL_BOUNDARY:
        synchronizedRemove(localBoundaryVertices, vertexId);
        synchronizedAdd(mixedBoundaryVertices, vertexId);
        return;
      case REMOTE_BOUNDARY:
        // fall through
      case MIXED_BOUNDARY:
        return;
      default:
        synchronizedAdd(remoteBoundaryVertices, vertexId);
        return;
      }
    }
  }

  /**
   * Prints all stored dependencies. Not thread safe.
   */
  public void printAll() {
    for (Object i : localBoundaryVertices) {
      LOG.info("[[VSTORE]] loc " + i);
    }
    for (Object i : remoteBoundaryVertices) {
      LOG.info("[[VSTORE]] rem " + i);
    }
    for (Object i : mixedBoundaryVertices) {
      LOG.info("[[VSTORE]] mix " + i);
    }
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
      // implicit (absence from all sets indicates internal)
      return;
    case LOCAL_BOUNDARY:
      synchronizedAdd(localBoundaryVertices, vertexId);
      return;
    case REMOTE_BOUNDARY:
      synchronizedAdd(remoteBoundaryVertices, vertexId);
      return;
    case MIXED_BOUNDARY:
      synchronizedAdd(mixedBoundaryVertices, vertexId);
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
      return !(contains(localBoundaryVertices, vertexId) ||
               contains(remoteBoundaryVertices, vertexId) ||
               contains(mixedBoundaryVertices, vertexId));
    case LOCAL_BOUNDARY:
      return contains(localBoundaryVertices, vertexId);
    case REMOTE_BOUNDARY:
      return contains(remoteBoundaryVertices, vertexId);
    case MIXED_BOUNDARY:
      return contains(mixedBoundaryVertices, vertexId);
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
    if (contains(localBoundaryVertices, vertexId)) {
      return VertexType.LOCAL_BOUNDARY;
    } else if (contains(remoteBoundaryVertices, vertexId)) {
      return VertexType.REMOTE_BOUNDARY;
    } else if (contains(mixedBoundaryVertices, vertexId)) {
      return VertexType.MIXED_BOUNDARY;
    } else {
      // TODO-YH: this does NOT currently support graph mutations!
      // Mutations can add vertices, which will be erroneously treated
      // as internal. Handling mutations requires proper re-check of
      // all out-edge AND in-edge neighbours.
      return VertexType.INTERNAL;
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
   * Removes vertexId from set, synchronizing on set.
   *
   * @param set Set to remove from
   * @param vertexId Vertex id to remove
   */
  private void synchronizedRemove(Set set, I vertexId) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    synchronized (set) {
      if (vertexIdClass.equals(IntWritable.class)) {
        set.remove(((IntWritable) vertexId).get());
      } else if (vertexIdClass.equals(LongWritable.class)) {
        set.remove(((LongWritable) vertexId).get());
      } else {
        set.remove(vertexId);
      }
    }
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

  /**
   * @return Number of mixed boundary vertices.
   */
  public int numMixedBoundaryVertices() {
    return mixedBoundaryVertices.size();
  }
}
