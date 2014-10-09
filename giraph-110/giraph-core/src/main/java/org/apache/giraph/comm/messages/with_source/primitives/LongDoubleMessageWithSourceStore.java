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

package org.apache.giraph.comm.messages.with_source.primitives;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.with_source.MessageWithSourceStore;
import org.apache.giraph.comm.messages.with_source.MessageWithSource;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.SourceVertexIdIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.VertexIdMessageWithSourceIterator;
import org.apache.giraph.utils.VertexIdMessagesWithSource;
import org.apache.giraph.utils.EmptyIterable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
//import org.apache.log4j.Logger;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * YH: Special message store to be used when ids are LongWritable and messages
 * are DoubleWritable and have sources.
 * Uses fastutil primitive maps in order to decrease number of objects and
 * get better performance.
 */
public class LongDoubleMessageWithSourceStore
    implements MessageWithSourceStore<LongWritable, DoubleWritable> {

  ///** Class logger */
  //private static final Logger LOG =
  //    Logger.getLogger(LongDoubleMessageWithSourceStore.class);
  /** Map from partition id to map from vertex id to message */
  private final Int2ObjectOpenHashMap<
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap>> map;
  /** Service worker */
  private final CentralizedServiceWorker<LongWritable, ?, ?> service;

  /**
   * Constructor
   *
   * @param service Service worker
   */
  public LongDoubleMessageWithSourceStore(
      CentralizedServiceWorker<LongWritable, Writable, Writable> service) {
    this.service = service;

    map = new Int2ObjectOpenHashMap<
      Long2ObjectOpenHashMap<Long2DoubleOpenHashMap>>();
    for (int partitionId : service.getPartitionStore().getPartitionIds()) {
      Partition<LongWritable, Writable, Writable> partition =
          service.getPartitionStore().getOrCreatePartition(partitionId);
      Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
        new Long2ObjectOpenHashMap<Long2DoubleOpenHashMap>(
          (int) partition.getVertexCount());
      map.put(partitionId, partitionMap);
      service.getPartitionStore().putPartition(partition);
    }
  }

  /**
   * Get map which holds source maps for partition which vertex belongs to.
   *
   * @param vertexId Id of the vertex
   * @return Map which holds source maps for partition which vertex belongs to.
   */
  private Long2ObjectOpenHashMap<Long2DoubleOpenHashMap>
  getPartitionMap(LongWritable vertexId) {
    return map.get(service.getPartitionId(vertexId));
  }

  /**
   * If there is already a map of sources related to the destination
   * vertex id, then return that map. Otherwise create a new one, put it
   * in the partition map, and return it.
   *
   * @param partitionMap Message map for this partition
   * @param dstId Id of the destination vertex (receiver)
   * @return Source map for the destination vertex
   */
  private Long2DoubleOpenHashMap getOrCreateSourceMap(
      Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap,
      LongWritable dstId) {
    synchronized (partitionMap) {
      Long2DoubleOpenHashMap srcMap = partitionMap.get(dstId.get());

      if (srcMap == null) {
        // YH: can't use number of dstId's neighbours as hint,
        // b/c that counts its OUT-edges rather than IN-edges
        Long2DoubleOpenHashMap tmpMap = new Long2DoubleOpenHashMap();
        srcMap = partitionMap.put(dstId.get(), tmpMap);
        if (srcMap == null) {
          srcMap = tmpMap;
        }
      }
      return srcMap;
    }
  }

  @Override
  public void addPartitionMessage(
      int partitionId, LongWritable destVertexId,
      MessageWithSource<LongWritable, DoubleWritable> message)
    throws IOException {

    Long2DoubleOpenHashMap srcMap =
      getOrCreateSourceMap(map.get(partitionId), destVertexId);

    synchronized (srcMap) {
      // always overwrite existing message
      srcMap.put(message.getSource().get(), message.getMessage().get());
    }
  }

  @Override
  public void addPartitionMessages(
      int partitionId, VertexIdMessages<LongWritable,
      MessageWithSource<LongWritable, DoubleWritable>> messages)
    throws IOException {
    LongWritable reusableVertexId = new LongWritable();
    MessageWithSource<LongWritable, DoubleWritable> reusableMessage =
      new MessageWithSource(new LongWritable(), new DoubleWritable());

    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      map.get(partitionId);

    VertexIdMessageWithSourceIterator<LongWritable, DoubleWritable> itr =
      ((VertexIdMessagesWithSource<LongWritable, DoubleWritable>) messages).
      getVertexIdMessageWithSourceIterator();

    while (itr.hasNext()) {
      itr.next();
      long srcId = ((SourceVertexIdIterator<LongWritable>) itr).
        getCurrentSourceVertexId().get();
      double msg = itr.getCurrentMessage().get();

      // no need to release (all primitives)
      Long2DoubleOpenHashMap srcMap =
        getOrCreateSourceMap(map.get(partitionId),
                             itr.getCurrentVertexId());

      synchronized (srcMap) {
        // always overwrite existing message
        srcMap.put(srcId, msg);
      }
    }
  }

  @Override
  public void clearPartition(int partitionId) throws IOException {
    // YH: not used in async, so no need to synchronize
    map.get(partitionId).clear();
  }

  @Override
  public boolean hasMessagesForVertex(LongWritable vertexId) {
    Long2ObjectOpenHashMap<?> partitionMap = getPartitionMap(vertexId);
    synchronized (partitionMap) {
      return partitionMap.containsKey(vertexId.get());
    }
  }

  @Override
  public boolean hasMessagesForPartition(int partitionId) {
    Long2ObjectOpenHashMap<?> partitionMap = map.get(partitionId);
    synchronized (partitionMap) {
      return partitionMap != null && !partitionMap.isEmpty();
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

  ///**
  // * Prints everything in the message store.
  // */
  //private void printAll() {
  //  for (Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap :
  //         map.values()) {
  //    synchronized (partitionMap) {
  //      LOG.info("[[PR-store-all]] new partition");
  //
  //      ObjectIterator<Long2ObjectMap.Entry<Long2DoubleOpenHashMap>> itr =
  //        partitionMap.long2ObjectEntrySet().fastIterator();
  //
  //      while (itr.hasNext()) {
  //        Long2ObjectMap.Entry<Long2DoubleOpenHashMap> entry = itr.next();
  //        LOG.info("[[PR-store-all]]   dst=" + entry.getLongKey());
  //
  //        Long2DoubleOpenHashMap srcMap = entry.getValue();
  //        ObjectIterator<Long2DoubleMap.Entry> srcItr =
  //          srcMap.long2DoubleEntrySet().fastIterator();
  //
  //        while (srcItr.hasNext()) {
  //          Long2DoubleMap.Entry srcEntry = srcItr.next();
  //          LOG.info("[[PR-store-all]]     src=" + srcEntry.getLongKey() +
  //                   ", msg=" + srcEntry.getDoubleValue());
  //        }
  //      }
  //    }
  //  }
  //}

  @Override
  public Iterable<MessageWithSource<LongWritable, DoubleWritable>>
  getVertexMessages(LongWritable vertexId) throws IOException {
    // YH: throw unsupported exception, as returning an iterator over
    // a non-concurrent hash map is just asking for trouble
    throw new UnsupportedOperationException(
      "Unsupported, use getVertexMessagesWithoutSource instead.");
  }

  @Override
  public Iterable<MessageWithSource<LongWritable, DoubleWritable>>
  removeVertexMessages(LongWritable vertexId) throws IOException {
    // TODO-YH: removing is okay, will implement later
    throw new UnsupportedOperationException(
      "Unsupported, use removeVertexMessagesWithoutSource instead.");

    //Long2DoubleOpenHashMap partitionMap = getPartitionMap(vertexId);
    //
    //// YH: must synchronize, as writes are concurrent w/ reads in async
    //synchronized (partitionMap) {
    //  if (!partitionMap.containsKey(vertexId.get())) {
    //    return EmptyIterable.get();
    //  } else {
    //    return new SomeIterator(partitionMap.remove(vertexId.get()));
    //  }
    //}
  }

  @Override
  public Iterable<DoubleWritable>
  getVertexMessagesWithoutSource(LongWritable vertexId)
    throws IOException {
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      getPartitionMap(vertexId);

    synchronized (partitionMap) {
      Long2DoubleOpenHashMap srcMap = partitionMap.get(vertexId.get());
      if (srcMap == null) {
        return EmptyIterable.get();
      } else {
        return new DoubleMessagesIterable(srcMap);
      }
    }
  }

  @Override
  public Iterable<DoubleWritable>
  removeVertexMessagesWithoutSource(LongWritable vertexId)
    throws IOException {
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      getPartitionMap(vertexId);

    synchronized (partitionMap) {
      Long2DoubleOpenHashMap srcMap = partitionMap.remove(vertexId);
      if (srcMap == null) {
        return EmptyIterable.get();
      } else {
        return new DoubleMessagesIterable(srcMap);
      }
    }
  }

  @Override
  public void clearVertexMessages(LongWritable vertexId) throws IOException {
    // YH: not used in async, so no need to synchronize
    getPartitionMap(vertexId).remove(vertexId.get());
  }

  @Override
  public void clearAll() throws IOException {
    map.clear();
  }

  @Override
  public Iterable<LongWritable> getPartitionDestinationVertices(
      int partitionId) {
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      map.get(partitionId);
    // YH: used with single thread
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
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      map.get(partitionId);
    // YH: used with single thread
    out.writeInt(partitionMap.size());

    ObjectIterator<Long2ObjectMap.Entry<Long2DoubleOpenHashMap>> itr =
        partitionMap.long2ObjectEntrySet().fastIterator();

    while (itr.hasNext()) {
      Long2ObjectMap.Entry<Long2DoubleOpenHashMap> entry = itr.next();
      out.writeLong(entry.getLongKey());

      // srcMap is always non-null, as destination vertex only exists
      // if it received a message, and a message always has a source
      Long2DoubleOpenHashMap srcMap = entry.getValue();
      out.writeInt(srcMap.size());

      ObjectIterator<Long2DoubleMap.Entry> srcItr =
        srcMap.long2DoubleEntrySet().fastIterator();

      while (srcItr.hasNext()) {
        Long2DoubleMap.Entry srcEntry = srcItr.next();
        out.writeLong(srcEntry.getLongKey());
        out.writeDouble(srcEntry.getDoubleValue());
      }
    }
  }

  @Override
  public void readFieldsForPartition(DataInput in,
      int partitionId) throws IOException {
    int size = in.readInt();
    Long2ObjectOpenHashMap<Long2DoubleOpenHashMap> partitionMap =
      new Long2ObjectOpenHashMap<Long2DoubleOpenHashMap>(size);

    while (size-- > 0) {
      long dstId = in.readLong();
      int srcMapSize = in.readInt();
      Long2DoubleOpenHashMap srcMap = new Long2DoubleOpenHashMap(srcMapSize);

      while (srcMapSize-- > 0) {
        srcMap.put(in.readLong(), in.readDouble());
      }
      partitionMap.put(dstId, srcMap);
    }

    synchronized (map) {
      map.put(partitionId, partitionMap);
    }
  }
}
