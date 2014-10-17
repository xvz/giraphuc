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

package org.apache.giraph.comm.messages.primitives;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Special message store to be used when ids are LongWritable and messages
 * are LongWritable and messageCombiner is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 */
public class LongLongMessageStore
    implements MessageStore<LongWritable, LongWritable> {
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<Long2LongOpenHashMap> map;
  /** Message messageCombiner */
  private final MessageCombiner<LongWritable, LongWritable> messageCombiner;
  /** Service worker */
  private final CentralizedServiceWorker<LongWritable, ?, ?> service;

  /**
   * Constructor
   *
   * @param service Service worker
   * @param messageCombiner Message messageCombiner
   */
  public LongLongMessageStore(
      CentralizedServiceWorker<LongWritable, Writable, Writable> service,
      MessageCombiner<LongWritable, LongWritable> messageCombiner) {
    this.service = service;
    this.messageCombiner =
        messageCombiner;

    map = new Int2ObjectOpenHashMap<Long2LongOpenHashMap>();
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Partition<LongWritable, Writable, Writable> partition =
          service.getPartitionStore().getOrCreatePartition(partitionId);
      Long2LongOpenHashMap partitionMap =
          new Long2LongOpenHashMap((int) partition.getVertexCount());
      map.put(partitionId, partitionMap);
      service.getPartitionStore().putPartition(partition);
    }
  }

  /**
   * Get map which holds messages for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds messages for partition which vertex belongs to.
   */
  private Long2LongOpenHashMap getPartitionMap(LongWritable vertexId) {
    return map.get(service.getPartitionId(vertexId));
  }

  @Override
  public void addPartitionMessage(int partitionId,
      LongWritable destVertexId, LongWritable message) throws
      IOException {
    // TODO-YH: this creates a new object for EVERY message, which
    // can result in substantial overheads.
    //
    // A better solution is to have a resuable LongWritable for each
    // partition id (i.e., per-instance map), which are then automatically
    // protected when synchronized on partitionMap below.
    LongWritable reusableCurrentMessage = new LongWritable();

    Long2LongOpenHashMap partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      long vertexId = destVertexId.get();
      long msg = message.get();
      if (partitionMap.containsKey(vertexId)) {
        reusableCurrentMessage.set(partitionMap.get(vertexId));
        messageCombiner.combine(destVertexId, reusableCurrentMessage, message);
        msg = reusableCurrentMessage.get();
      }
      partitionMap.put(vertexId, msg);
    }
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<LongWritable, LongWritable> messages) throws
      IOException {
    LongWritable reusableVertexId = new LongWritable();
    LongWritable reusableMessage = new LongWritable();
    LongWritable reusableCurrentMessage = new LongWritable();

    Long2LongOpenHashMap partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageIterator<LongWritable, LongWritable> iterator =
        messages.getVertexIdMessageIterator();
      while (iterator.hasNext()) {
        iterator.next();
        long vertexId = iterator.getCurrentVertexId().get();
        long message = iterator.getCurrentMessage().get();
        if (partitionMap.containsKey(vertexId)) {
          reusableVertexId.set(vertexId);
          reusableMessage.set(message);
          reusableCurrentMessage.set(partitionMap.get(vertexId));
          messageCombiner.combine(reusableVertexId, reusableCurrentMessage,
              reusableMessage);
          message = reusableCurrentMessage.get();
        }
        partitionMap.put(vertexId, message);
      }
    }
  }

  @Override
  public void restore(int partitionId, LongWritable vertexId,
                      Writable messages) throws IOException {
    // this function should never be called
    throw new UnsupportedOperationException("restore: Not supported");
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    // YH: not used in async, but synchronize anyway
    Long2LongOpenHashMap partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      partitionMap.clear();
    }
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    Long2LongOpenHashMap partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      return partitionMap.containsKey(vertexId.get());
    }
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Long2LongOpenHashMap partitionMap = map.get(partitionId);

    if (partitionMap == null) {
      return false;
    }

    synchronized (partitionMap) {
      return !partitionMap.isEmpty();
    }
  }

  @Override
  public boolean hasMessages() {
    for (Long2LongOpenHashMap partitionMap : map.values()) {
      synchronized (partitionMap) {
        if (!partitionMap.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Iterable<LongWritable> getVertexMessages(
      LongWritable vertexId) throws IOException {
    Long2LongOpenHashMap partitionMap = getPartitionMap(vertexId);

    // YH: must synchronize, as writes are concurrent w/ reads in async
    synchronized (partitionMap) {
      if (!partitionMap.containsKey(vertexId.get())) {
        return EmptyIterable.get();
      } else {
        return Collections.singleton(
            new LongWritable(partitionMap.get(vertexId.get())));
      }
    }
  }

  @Override
  public Iterable<LongWritable> removeVertexMessages(
      LongWritable vertexId) throws IOException {
    Long2LongOpenHashMap partitionMap = getPartitionMap(vertexId);

    // YH: must synchronize, as writes are concurrent w/ reads in async
    synchronized (partitionMap) {
      if (!partitionMap.containsKey(vertexId.get())) {
        return EmptyIterable.get();
      } else {
        return Collections.singleton(
            new LongWritable(partitionMap.remove(vertexId.get())));
      }
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) throws IOException {
    // YH: not used in async, but synchronize anyway
    Long2LongOpenHashMap partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      partitionMap.remove(vertexId.get());
    }
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  @Override
  public Iterable<LongWritable> getPartitionDestinationVertices(
      int partitionId) {
    Long2LongOpenHashMap partitionMap = map.get(partitionId);
    // YH: used by single thread
    List<LongWritable> vertices =
        Lists.newArrayListWithCapacity(partitionMap.size());
    LongIterator iterator = partitionMap.keySet().iterator();
    while (iterator.hasNext()) {
      vertices.add(new LongWritable(iterator.nextLong()));
    }
    return vertices;
  }

  @Override
  public void writePartition(DataOutput out,
      int partitionId) throws IOException {
    Long2LongOpenHashMap partitionMap = map.get(partitionId);
    // YH: used by single thread
    out.writeInt(partitionMap.size());
    ObjectIterator<Long2LongMap.Entry> iterator =
        partitionMap.long2LongEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2LongMap.Entry entry = iterator.next();
      out.writeLong(entry.getLongKey());
      out.writeLong(entry.getLongValue());
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Long2LongOpenHashMap partitionMap = new Long2LongOpenHashMap(size);
    // YH: used by single thread
    while (size-- > 0) {
      long vertexId = in.readLong();
      long message = in.readLong();
      partitionMap.put(vertexId, message);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
