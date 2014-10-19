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
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedDataOutput;
import org.apache.giraph.utils.SourceVertexIdIterator;
import org.apache.giraph.utils.VertexIdIterator;
import org.apache.giraph.utils.VertexIdMessageWithSourceBytesIterator;
import org.apache.giraph.utils.VertexIdMessageWithSourceIterator;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.VertexIdMessagesWithSource;
import org.apache.giraph.utils.RepresentativeByteStructIterator;
import org.apache.giraph.utils.VerboseByteStructMessageWrite;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
//import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

/**
 * Implementation of {@link SimpleMessageWithSourceStore} where multiple
 * messages are stored per source vertex as byte backed datastructures.
 * Used when there is no combiner provided and when each source sends
 * multiple messages to a single destination.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class ByteArrayMessagesPerSourceVertexStore<
    I extends WritableComparable, M extends Writable> extends
    SimpleMessageWithSourceStore<I, M, DataInputOutput> {
  ///** Class logger */
  //private static final Logger LOG =
  //    Logger.getLogger(ByteArrayMessagesPerSourceVertexStore.class);

  /**
   * Constructor
   *
   * @param messageValueFactory Message class held in the store
   * @param service Service worker
   * @param config Hadoop configuration
   */
  public ByteArrayMessagesPerSourceVertexStore(
      MessageValueFactory<M> messageValueFactory,
      CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    super(messageValueFactory, service, config);
  }

  /**
   * Get the extended data output for a vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param srcMap Source map to look in
   * @param iterator Special iterator that can release ownerships of vertex ids
   * @return Extended data output for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      ConcurrentMap<I, DataInputOutput> srcMap,
      SourceVertexIdIterator<I> iterator) {
    DataInputOutput dataInputOutput =
        srcMap.get(iterator.getCurrentSourceVertexId());
    if (dataInputOutput == null) {
      DataInputOutput newDataOutput = config.createMessagesInputOutput();
      dataInputOutput = srcMap.putIfAbsent(
          iterator.releaseCurrentSourceVertexId(), newDataOutput);
      if (dataInputOutput == null) {
        dataInputOutput = newDataOutput;
      }
    }
    return dataInputOutput;
  }

  /**
   * Get the extended data output for a vertex id. Similar to the iterator
   * version, with no need to worry about ownership.
   *
   * @param srcMap Source map to look in
   * @param vertexId The vertex id of interest
   * @return Extended data output for this vertex id (created if necessary)
   */
  private DataInputOutput getDataInputOutput(
      ConcurrentMap<I, DataInputOutput> srcMap,
      I vertexId) {
    DataInputOutput dataInputOutput = srcMap.get(vertexId);
    if (dataInputOutput == null) {
      DataInputOutput newDataOutput = config.createMessagesInputOutput();
      // YH: vertexId is held by internal graph, so no need to clone it
      // (see NettyWorkerClientRequestProcessor's "new MessageWithSource")
      dataInputOutput = srcMap.putIfAbsent(vertexId, newDataOutput);
      if (dataInputOutput == null) {
        dataInputOutput = newDataOutput;
      }
    }
    return dataInputOutput;
  }

  /**
   * Get the source map for a destination vertex id from the iterator, creating
   * if necessary.  This method will take ownership of the vertex id from the
   * iterator if necessary (if used in the partition map entry).
   *
   * @param partitionMap Partition map to look in
   * @param iterator Special iterator that can release ownerships of vertex ids
   * @return Source map for this destination vertex id (created if necessary)
   */
  private ConcurrentMap<I, DataInputOutput> getOrCreateSourceMap(
      ConcurrentMap<I, ConcurrentMap<I, DataInputOutput>> partitionMap,
      VertexIdIterator<I> iterator) {
    ConcurrentMap<I, DataInputOutput> srcMap =
        partitionMap.get(iterator.getCurrentVertexId());
    if (srcMap == null) {
      ConcurrentMap<I, DataInputOutput> tmpMap =
        new MapMaker().concurrencyLevel(
          config.getNettyServerExecutionConcurrency()).makeMap();
      srcMap = partitionMap.putIfAbsent(
          iterator.releaseCurrentVertexId(), tmpMap);
      if (srcMap == null) {
        srcMap = tmpMap;
      }
    }
    return srcMap;
  }

  @Override
  public void addPartitionMessage(
      int partitionId, I destVertexId, MessageWithSource<I, M> message)
    throws IOException {

    ConcurrentMap<I, DataInputOutput> srcMap =
      getOrCreateSourceMap(getOrCreatePartitionMap(partitionId),
                           destVertexId, true);
    DataInputOutput dataInputOutput =
      getDataInputOutput(srcMap, message.getSource());

    // YH: as per utils.ByteArrayVertexIdMessages...
    // a little messy to decouple serialization like this
    synchronized (dataInputOutput) {
      // YH: always resetting means we only ever write one message
      // TODO-YH: also implement reset for BigDataOutput
      if (dataInputOutput.getDataOutput() instanceof ExtendedDataOutput) {
        ((ExtendedDataOutput) dataInputOutput.getDataOutput()).reset();
      }
      message.getMessage().write(dataInputOutput.getDataOutput());
    }
  }

  @Override
  public void addPartitionMessages(
      int partitionId, VertexIdMessages<I, MessageWithSource<I, M>> messages)
    throws IOException {
    ConcurrentMap<I, ConcurrentMap<I, DataInputOutput>> partitionMap =
        getOrCreatePartitionMap(partitionId);

    VertexIdMessageWithSourceBytesIterator<I, M> msgBytesItr =
      ((VertexIdMessagesWithSource<I, M>) messages).
      getVertexIdMessageWithSourceBytesIterator();

    // Try to copy the message buffer over rather than
    // doing a deserialization of a message just to know its size.  This
    // should be more efficient for complex objects where serialization is
    // expensive.  If this type of iterator is not available, fall back to
    // deserializing/serializing the messages

    if (msgBytesItr != null) {
      while (msgBytesItr.hasNext()) {
        msgBytesItr.next();

        ConcurrentMap<I, DataInputOutput> srcMap =
          getOrCreateSourceMap(partitionMap, msgBytesItr);
        DataInputOutput dataInputOutput = getDataInputOutput(
            srcMap, (SourceVertexIdIterator<I>) msgBytesItr);

        synchronized (dataInputOutput) {
          // YH: always resetting means we only ever write one message
          if (dataInputOutput.getDataOutput() instanceof ExtendedDataOutput) {
            ((ExtendedDataOutput) dataInputOutput.getDataOutput()).reset();
          }
          msgBytesItr.writeCurrentMessageBytes(dataInputOutput.getDataOutput());
        }
      }
    } else {
      VertexIdMessageWithSourceIterator<I, M> msgItr =
        ((VertexIdMessagesWithSource<I, M>) messages).
        getVertexIdMessageWithSourceIterator();

      while (msgItr.hasNext()) {
        msgItr.next();

        ConcurrentMap<I, DataInputOutput> srcMap =
          getOrCreateSourceMap(partitionMap, msgItr);
        DataInputOutput dataInputOutput =
          getDataInputOutput(srcMap, (SourceVertexIdIterator<I>) msgItr);

        synchronized (dataInputOutput) {
          // YH: always resetting means we only ever write one message
          if (dataInputOutput.getDataOutput() instanceof ExtendedDataOutput) {
            ((ExtendedDataOutput) dataInputOutput.getDataOutput()).reset();
          }
          VerboseByteStructMessageWrite.verboseWriteCurrentMessage(
              msgItr, dataInputOutput.getDataOutput());
        }
      }
    }
  }

  @Override
  protected Iterable<MessageWithSource<I, M>> getMessagesAsIterable(
      ConcurrentMap<I, DataInputOutput> sourceMap) {
    // YH: returned iterator will be synchronized
    return new MessagesWithSourceIterable<I, M>(
        sourceMap.entrySet().iterator(), messageValueFactory);
  }

  @Override
  protected Iterable<M> getMessagesWithoutSourceAsIterable(
      ConcurrentMap<I, DataInputOutput> sourceMap) {
    // YH: returned iterator will be synchronized
    return new MessagesIterable<M>(sourceMap.values().iterator(),
                                   messageValueFactory);
  }

  ///**
  // * Prints everything in the message store.
  // */
  //private void printAll() {
  //  M tmp = messageValueFactory.newInstance();
  //
  //  for (ConcurrentMap<I, ConcurrentMap<I, DataInputOutput>>
  //         partitionMap : map.values()) {
  //    LOG.info("[[PR-store-all]] new partition");
  //    for (Map.Entry<I, ConcurrentMap<I, DataInputOutput>>
  //           pm : partitionMap.entrySet()) {
  //      LOG.info("[[PR-store-all]]   dst=" + pm.getKey());
  //      for (Map.Entry<I, DataInputOutput> e : pm.getValue().entrySet()) {
  //        try {
  //          // YH: this does NOT deserialize all the messages!!
  //          tmp.readFields(e.getValue().createDataInput());
  //        } catch (IOException ioe) {
  //          throw new RuntimeException("Deserialization failed!");
  //        }
  //        LOG.info("[[PR-store-all]]     src=" +
  //                 e.getKey() + ", msg=" + tmp);
  //      }
  //    }
  //  }
  //}

  @Override
  protected int getNumberOfMessagesIn(
      ConcurrentMap<I, ConcurrentMap<I, DataInputOutput>> partitionMap) {
    int numberOfMessages = 0;
    for (ConcurrentMap<I, DataInputOutput> srcMap : partitionMap.values()) {
      for (DataInputOutput dataInputOutput : srcMap.values()) {
        synchronized (dataInputOutput) {
          numberOfMessages += Iterators.size(
              new RepresentativeByteStructIterator<M>(
                  dataInputOutput.createDataInput()) {
                @Override
                protected M createWritable() {
                  return messageValueFactory.newInstance();
                }
              });
        }
      }
    }
    return numberOfMessages;
  }

  @Override
  protected void writeMessages(DataInputOutput dataInputOutput,
      DataOutput out) throws IOException {
    // YH: used by single thread
    dataInputOutput.write(out);
  }

  @Override
  protected DataInputOutput readFieldsForMessages(DataInput in) throws
      IOException {
    // YH: used by single thread
    DataInputOutput dataInputOutput = config.createMessagesInputOutput();
    dataInputOutput.readFields(in);
    return dataInputOutput;
  }

  /**
   * Create new factory for this message store
   *
   * @param service Worker service
   * @param config  Hadoop configuration
   * @param <I>     Vertex id
   * @param <M>     Message data
   * @return Factory
   */
  public static <I extends WritableComparable, M extends Writable>
  MessageStoreFactory<I, M, MessageWithSourceStore<I, M>>
  newFactory(CentralizedServiceWorker<I, ?, ?> service,
             ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    return new Factory<I, M>(service, config);
  }

  /**
   * Factory for {@link ByteArrayMessagesPerSourceVertexStore}
   *
   * @param <I> Vertex id
   * @param <M> Message data
   */
  private static class Factory<I extends WritableComparable, M extends Writable>
      implements MessageStoreFactory<I, M, MessageWithSourceStore<I, M>> {
    /** Service worker */
    private CentralizedServiceWorker<I, ?, ?> service;
    /** Hadoop configuration */
    private ImmutableClassesGiraphConfiguration<I, ?, ?> config;

    /**
     * @param service Worker service
     * @param config  Hadoop configuration
     */
    public Factory(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
      this.service = service;
      this.config = config;
    }

    @Override
    public MessageWithSourceStore<I, M> newStore(
        MessageValueFactory<M> messageValueFactory) {
      return new ByteArrayMessagesPerSourceVertexStore<I, M>(
          messageValueFactory, service, config);
    }

    @Override
    public void initialize(CentralizedServiceWorker<I, ?, ?> service,
        ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
      this.service = service;
      this.config = conf;
    }

    @Override
    public boolean shouldTraverseMessagesInOrder() {
      return false;
    }
  }
}
