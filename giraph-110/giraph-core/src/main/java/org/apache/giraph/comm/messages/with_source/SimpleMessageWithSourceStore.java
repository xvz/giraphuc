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

package org.apache.giraph.comm.messages.with_source;

import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Abstract class for {@link MessageWithSourceStore} which allows any kind
 * of object to hold messages (and source) for one vertex. Simple in memory
 * message store implemented with a three level concurrent hash map.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 * @param <T> Type of object holding messages (WITHOUT source) for one vertex
 */
public abstract class SimpleMessageWithSourceStore<
  I extends WritableComparable, M extends Writable, T>
  implements MessageWithSourceStore<I, M> {

  /** Message class */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Service worker */
  protected final CentralizedServiceWorker<I, ?, ?> service;
  /**
   * YH: Map from partition id to map from destination vertex id
   * to map from source vertex id to corresponding message(s)
   *
   * Notationally: map -> partitionMap/dstMap -> srcMap -> msg
   */
  protected final ConcurrentMap<Integer,
    ConcurrentMap<I, ConcurrentMap<I, T>>> map;
  /** Giraph configuration */
  protected final ImmutableClassesGiraphConfiguration<I, ?, ?> config;

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param config Giraph configuration
   */
  public SimpleMessageWithSourceStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.config = config;
    map = new MapMaker().concurrencyLevel(
        config.getNettyServerExecutionConcurrency()).makeMap();
  }

  /**
   * Get messages as an iterable from message storage
   *
   * @param sourceMap Map of source vertex ids to messages
   * @return Messages (with source) as an iterable
   */
  protected abstract Iterable<MessageWithSource<I, M>> getMessagesAsIterable(
      ConcurrentMap<I, T> sourceMap);

  /**
   * Get messages as an iterable from message storage
   *
   * @param sourceMap Map of source vertex ids to messages
   * @return Messages (no source) as an iterable
   */
  protected abstract Iterable<M> getMessagesWithoutSourceAsIterable(
      ConcurrentMap<I, T> sourceMap);

  /**
   * Get number of messages in partition map
   *
   * @param partitionMap Partition map in which to count messages
   * @return Number of messages in partition map
   */
  protected abstract int getNumberOfMessagesIn(
      ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap);

  /**
   * Write message storage to {@link DataOutput}
   *
   * @param messages Message storage
   * @param out Data output
   * @throws IOException
   */
  protected abstract void writeMessages(T messages, DataOutput out)
    throws IOException;

  /**
   * Read message storage from {@link DataInput}
   *
   * @param in Data input
   * @return Message storage
   * @throws IOException
   */
  protected abstract T readFieldsForMessages(DataInput in) throws IOException;

  /**
   * Get id of partition which holds vertex with selected id
   *
   * @param vertexId Id of vertex
   * @return Id of partiton
   */
  protected int getPartitionId(I vertexId) {
    return service.getVertexPartitionOwner(vertexId).getPartitionId();
  }

  /**
   * If there is already a map of messages related to the partition id
   * return that map, otherwise create a new one, put it in global map and
   * return it.
   *
   * @param partitionId Id of partition
   * @return Message map for this partition
   */
  protected ConcurrentMap<I, ConcurrentMap<I, T>> getOrCreatePartitionMap(
      int partitionId) {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap = map.get(partitionId);
    if (partitionMap == null) {
      ConcurrentMap<I, ConcurrentMap<I, T>> tmpMap =
        new MapMaker().concurrencyLevel(
          config.getNettyServerExecutionConcurrency()).makeMap();
      partitionMap = map.putIfAbsent(partitionId, tmpMap);
      if (partitionMap == null) {
        partitionMap = tmpMap;
      }
    }
    return partitionMap;
  }

  /**
   * YH: If there is already a map of sources related to the destination
   * vertex id, then return that map. Otherwise create a new one, put it
   * in the partition map, and return it.
   *
   * @param partitionMap Message map for this partition
   * @param dstId Id of the destination vertex (receiver)
   * @param cloneDstId True if dstId must be cloned/copied before storage
   * @return Source map for the destination vertex
   */
  protected ConcurrentMap<I, T> getOrCreateSourceMap(
      ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap,
      I dstId, boolean cloneDstId) {
    ConcurrentMap<I, T> srcMap = partitionMap.get(dstId);

    if (srcMap == null) {
      ConcurrentMap<I, T> tmpMap = new MapMaker().concurrencyLevel(
          config.getNettyServerExecutionConcurrency()).makeMap();

      // YH: if needed, clone the dest vertex id. This is needed when the
      // original object is user-accessible and/or can be invalidated on
      // some iterator's next() call up the call chain.
      I safeDstId = cloneDstId ? WritableUtils.clone(dstId, config) : dstId;

      srcMap = partitionMap.putIfAbsent(safeDstId, tmpMap);
      if (srcMap == null) {
        srcMap = tmpMap;
      }
    }

    return srcMap;
  }

  @Override
  public Iterable<I> getPartitionDestinationVertices(int partitionId) {
    ConcurrentMap<I, ?> partitionMap = map.get(partitionId);
    return (partitionMap == null) ? EmptyIterable.<I>get() :
        partitionMap.keySet();
  }

  @Override
  public boolean hasMessagesForVertex(I vertexId) {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    return partitionMap != null && partitionMap.containsKey(vertexId);
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    ConcurrentMap<I, ?> partitionMap = map.get(partitionId);
    return partitionMap != null && !partitionMap.isEmpty();
  }

  @Override
  public boolean hasMessages() {
    for (ConcurrentMap<I, ?> partitionMap : map.values()) {
      if (!partitionMap.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  // TODO-YH: lots of duplicated code... consolidate?
  // YH: MessageStore's overriden functions return MessageWithSource<I, M>
  @Override
  public Iterable<MessageWithSource<I, M>> getVertexMessages(I vertexId)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap =
      map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return EmptyIterable.<MessageWithSource<I, M>>get();
    }

    // srcMap (partitionMap.get(vertexId)) is always non-null, as
    // destination vertex only exists if it received a message,
    // and a message always has a source
    ConcurrentMap<I, T> srcMap = partitionMap.get(vertexId);
    return (srcMap == null) ?
      EmptyIterable.<MessageWithSource<I, M>>get() :
      getMessagesAsIterable(srcMap);
  }

  @Override
  public Iterable<MessageWithSource<I, M>> removeVertexMessages(I vertexId)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap =
      map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return EmptyIterable.<MessageWithSource<I, M>>get();
    }

    ConcurrentMap<I, T> srcMap = partitionMap.remove(vertexId);
    return (srcMap == null) ?
      EmptyIterable.<MessageWithSource<I, M>>get() :
      getMessagesAsIterable(srcMap);
  }


  // YH: MessageWithSourceStore's overriden functions return only M
  @Override
  public Iterable<M> getVertexMessagesWithoutSource(I vertexId)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap =
      map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return EmptyIterable.<M>get();
    }

    ConcurrentMap<I, T> srcMap = partitionMap.get(vertexId);
    return (srcMap == null) ? EmptyIterable.<M>get() :
      getMessagesWithoutSourceAsIterable(srcMap);
  }

  @Override
  public Iterable<M> removeVertexMessagesWithoutSource(I vertexId)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap =
      map.get(getPartitionId(vertexId));
    if (partitionMap == null) {
      return EmptyIterable.<M>get();
    }

    ConcurrentMap<I, T> srcMap = partitionMap.remove(vertexId);
    return (srcMap == null) ? EmptyIterable.<M>get() :
      getMessagesWithoutSourceAsIterable(srcMap);
  }


  @Override
  public void writePartition(DataOutput out, int partitionId)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap = map.get(partitionId);
    out.writeBoolean(partitionMap != null);
    if (partitionMap != null) {
      out.writeInt(partitionMap.size());
      for (Map.Entry<I, ConcurrentMap<I, T>> entry : partitionMap.entrySet()) {
        entry.getKey().write(out);
        ConcurrentMap<I, T> srcMap = entry.getValue();

        // srcMap is always non-null, as destination vertex only exists
        // if it received a message, and a message always has a source
        out.writeInt(srcMap.size());
        for (Map.Entry<I, T> srcEntry : srcMap.entrySet()) {
          srcEntry.getKey().write(out);
          writeMessages(srcEntry.getValue(), out);
        }
      }
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in, int partitionId)
    throws IOException {
    if (in.readBoolean()) {
      ConcurrentMap<I, ConcurrentMap<I, T>> partitionMap =
        Maps.newConcurrentMap();
      int numVertices = in.readInt();
      for (int v = 0; v < numVertices; v++) {
        I dstId = config.createVertexId();
        dstId.readFields(in);
        ConcurrentMap<I, T> srcMap = Maps.newConcurrentMap();

        int numSrc = in.readInt();
        for (int s = 0; s < numSrc; s++) {
          I srcId = config.createVertexId();
          srcId.readFields(in);
          srcMap.put(srcId, readFieldsForMessages(in));
        }
        partitionMap.put(dstId, srcMap);
      }
      map.put(partitionId, partitionMap);
    }
  }

  @Override
  public void clearVertexMessages(I vertexId) throws IOException {
    ConcurrentMap<I, ?> partitionMap =
        map.get(getPartitionId(vertexId));
    if (partitionMap != null) {
      partitionMap.remove(vertexId);
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    map.remove(partitionId);
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }
}
