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
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessagesIterable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.VertexIdMessageBytesIterator;
import org.apache.giraph.utils.VertexIdMessageIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Special message store to be used when ids are LongWritable and no combiner
 * is used.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 *
 * @param <M> Message type
 */
public class LongByteArrayMessageStore<M extends Writable>
    implements MessageStore<LongWritable, M> {
  /** Message value factory */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Map from partition id to map from vertex id to message */
  private final
  Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<DataInputOutput>> map;
  /** Service worker */
  private final CentralizedServiceWorker<LongWritable, ?, ?> service;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<LongWritable, ?, ?> config;

  /**
   * Constructor
   *
   * @param messageValueFactory Factory for creating message values
   * @param service      Service worker
   * @param config       Hadoop configuration
   */
  public LongByteArrayMessageStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<LongWritable, Writable, Writable> service,
      ImmutableClassesGiraphConfiguration<LongWritable, Writable, Writable>
        config) {
    this.messageValueFactory = messageValueFactory;
    this.service = service;
    this.config = config;

    map =
        new Int2ObjectOpenHashMap<Long2ObjectOpenHashMap<DataInputOutput>>();
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Partition<LongWritable, Writable, Writable> partition =
          service.getPartitionStore().getOrCreatePartition(partitionId);
      Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
          new Long2ObjectOpenHashMap<DataInputOutput>(
              (int) partition.getVertexCount());
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
  private Long2ObjectOpenHashMap<DataInputOutput> getPartitionMap(
      LongWritable vertexId) {
    return map.get(service.getPartitionId(vertexId));
  }

  /**
   * Get the DataInputOutput for a vertex id, creating if necessary.
   *
   * @param partitionMap Partition map to look in
   * @param vertexId Id of the vertex
   * @return DataInputOutput for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      Long2ObjectOpenHashMap<DataInputOutput> partitionMap,
      long vertexId) {
    // YH: called only in synchronized blocks, so safe
    DataInputOutput dataInputOutput = partitionMap.get(vertexId);
    if (dataInputOutput == null) {
      dataInputOutput = config.createMessagesInputOutput();
      partitionMap.put(vertexId, dataInputOutput);
    }
    return dataInputOutput;
  }

  @Override
  public void addPartitionMessage(int partitionId,
      LongWritable destVertexId, M message) throws
      IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);

    synchronized (partitionMap) {
      DataInputOutput dataInputOutput =
        getDataInputOutput(partitionMap, destVertexId.get());
      message.write(dataInputOutput.getDataOutput());
    }
  }

  @Override
  public void addPartitionMessages(int partitionId,
      VertexIdMessages<LongWritable, M> messages) throws
      IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    synchronized (partitionMap) {
      VertexIdMessageBytesIterator<LongWritable, M>
          vertexIdMessageBytesIterator =
          messages.getVertexIdMessageBytesIterator();
      // Try to copy the message buffer over rather than
      // doing a deserialization of a message just to know its size.  This
      // should be more efficient for complex objects where serialization is
      // expensive.  If this type of iterator is not available, fall back to
      // deserializing/serializing the messages
      if (vertexIdMessageBytesIterator != null) {
        while (vertexIdMessageBytesIterator.hasNext()) {
          vertexIdMessageBytesIterator.next();
          DataInputOutput dataInputOutput = getDataInputOutput(partitionMap,
              vertexIdMessageBytesIterator.getCurrentVertexId().get());
          vertexIdMessageBytesIterator.writeCurrentMessageBytes(
              dataInputOutput.getDataOutput());
        }
      } else {
        VertexIdMessageIterator<LongWritable, M>
            iterator = messages.getVertexIdMessageIterator();
        while (iterator.hasNext()) {
          iterator.next();
          DataInputOutput dataInputOutput = getDataInputOutput(partitionMap,
              iterator.getCurrentVertexId().get());
          VerboseByteStructMessageWrite.verboseWriteCurrentMessage(iterator,
              dataInputOutput.getDataOutput());
        }
      }
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    // YH: not used in async, but synchronize anyway
    Long2ObjectOpenHashMap<?> partitionMap = map.get(partitionId);

    if (partitionMap == null) {
      return;
    }

    synchronized (partitionMap) {
      partitionMap.clear();
    }
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    Long2ObjectOpenHashMap<?> partitionMap = getPartitionMap(vertexId);

    if (partitionMap == null) {
      return false;
    }

    synchronized (partitionMap) {
      return partitionMap.containsKey(vertexId.get());
    }
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Long2ObjectOpenHashMap<?> partitionMap = map.get(partitionId);

    if (partitionMap == null) {
      return false;
    }

    synchronized (partitionMap) {
      return !partitionMap.isEmpty();
    }
  }

  @Override
  public boolean hasMessages() {
    for (Long2ObjectOpenHashMap<?> partitionMap : map.values()) {
      synchronized (partitionMap) {
        if (!partitionMap.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Iterable<M> getVertexMessages(
      LongWritable vertexId) throws IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        getPartitionMap(vertexId);

    if (partitionMap == null) {
      return EmptyIterable.get();
    }

    // YH: must synchronize, as writes are concurrent w/ reads in async
    synchronized (partitionMap) {
      DataInputOutput dataInputOutput = partitionMap.get(vertexId.get());
      if (dataInputOutput == null) {
        return EmptyIterable.get();
      } else {
        return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
      }
    }
  }

  @Override
  public Iterable<M> removeVertexMessages(
      LongWritable vertexId) throws IOException {
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        getPartitionMap(vertexId);

    if (partitionMap == null) {
      return EmptyIterable.get();
    }

    // YH: must synchronize, as writes are concurrent w/ reads in async
    synchronized (partitionMap) {
      DataInputOutput dataInputOutput = partitionMap.remove(vertexId.get());
      if (dataInputOutput == null) {
        return EmptyIterable.get();
      } else {
        return new MessagesIterable<M>(dataInputOutput, messageValueFactory);
      }
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) throws IOException {
    // YH: not used in async, but synchronize anyway
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
      getPartitionMap(vertexId);

    if (partitionMap == null) {
      return;
    }

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
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);

    if (partitionMap == null) {
      return EmptyIterable.get();
    }

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
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        map.get(partitionId);
    // YH: used by single thread
    out.writeInt(partitionMap.size());
    ObjectIterator<Long2ObjectMap.Entry<DataInputOutput>> iterator =
        partitionMap.long2ObjectEntrySet().fastIterator();
    while (iterator.hasNext()) {
      Long2ObjectMap.Entry<DataInputOutput> entry = iterator.next();
      out.writeLong(entry.getLongKey());
      entry.getValue().write(out);
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Long2ObjectOpenHashMap<DataInputOutput> partitionMap =
        new Long2ObjectOpenHashMap<DataInputOutput>(size);
    // YH: used by single thread
    while (size-- > 0) {
      long vertexId = in.readLong();
      DataInputOutput dataInputOutput = config.createMessagesInputOutput();
      dataInputOutput.readFields(in);
      partitionMap.put(vertexId, dataInputOutput);
    }
    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
