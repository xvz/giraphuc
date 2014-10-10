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

package org.apache.giraph.comm.messages;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.combiner.MessageCombiner;
import org.apache.giraph.comm.messages.primitives.IntByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.IntFloatMessageStore;
import org.apache.giraph.comm.messages.primitives.LongByteArrayMessageStore;
import org.apache.giraph.comm.messages.primitives.LongDoubleMessageStore;
import org.apache.giraph.comm.messages.primitives.LongLongMessageStore;
import org.apache.giraph.comm.messages.with_source.ByteArrayMessagesPerSourceVertexStore;
import org.apache.giraph.comm.messages.with_source.primitives.LongDoubleMessageWithSourceStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

/**
 * Message store factory which produces message stores which hold all
 * messages in memory. Depending on whether or not combiner is currently used,
 * this factory creates {@link OneMessagePerVertexStore} or
 * {@link ByteArrayMessagesPerVertexStore}
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
// TODO-YH: to accomodate MessageWithSourceStore, which uses/returns
// MessageStore<I, MessageWithStore<I, M>>, we change MS to use Writable
// (This change isn't totally needed, since ServerData assumes M=Writable)
public class InMemoryMessageStoreFactory<I extends WritableComparable,
    M extends Writable>
    implements MessageStoreFactory<I, M, MessageStore<I, Writable>> {
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(InMemoryMessageStoreFactory.class);

  /** Service worker */
  private CentralizedServiceWorker<I, ?, ?> service;
  /** Hadoop configuration */
  private ImmutableClassesGiraphConfiguration<I, ?, ?> conf;

  /**
   * Default constructor allowing class invocation via Reflection.
   */
  public InMemoryMessageStoreFactory() {
  }

  @Override
  public MessageStore<I, Writable> newStore(
      MessageValueFactory<M> messageValueFactory) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    Class<M> messageClass = messageValueFactory.getValueClass();
    MessageStore messageStore;
    if (conf.useMessageCombiner()) {
      if (vertexIdClass.equals(IntWritable.class) &&
          messageClass.equals(FloatWritable.class)) {
        messageStore = new IntFloatMessageStore(
            (CentralizedServiceWorker<IntWritable, Writable, Writable>) service,
            (MessageCombiner<IntWritable, FloatWritable>)
                conf.<FloatWritable>createMessageCombiner());
      } else if (vertexIdClass.equals(LongWritable.class) &&
                 messageClass.equals(DoubleWritable.class)) {
        messageStore = new LongDoubleMessageStore(
          (CentralizedServiceWorker<LongWritable, Writable, Writable>) service,
          (MessageCombiner<LongWritable, DoubleWritable>)
              conf.<DoubleWritable>createMessageCombiner());
      } else if (vertexIdClass.equals(LongWritable.class) &&
                 messageClass.equals(LongWritable.class)) {
        messageStore = new LongLongMessageStore(
          (CentralizedServiceWorker<LongWritable, Writable, Writable>) service,
          (MessageCombiner<LongWritable, LongWritable>)
              conf.<LongWritable>createMessageCombiner());
      } else {
        messageStore = new OneMessagePerVertexStore<I, M>(messageValueFactory,
          service, conf.<M>createMessageCombiner(), conf);
      }
    } else {
      if (vertexIdClass.equals(IntWritable.class)) {
        messageStore = new IntByteArrayMessageStore<M>(messageValueFactory,
          (CentralizedServiceWorker<IntWritable, Writable, Writable>) service,
          (ImmutableClassesGiraphConfiguration<IntWritable, Writable, Writable>)
            conf);
      } else if (vertexIdClass.equals(LongWritable.class)) {
        messageStore = new LongByteArrayMessageStore<M>(messageValueFactory,
          (CentralizedServiceWorker<LongWritable, Writable, Writable>) service,
          (ImmutableClassesGiraphConfiguration<LongWritable, Writable,
           Writable>) conf);
      } else {
        messageStore = new ByteArrayMessagesPerVertexStore<I, M>(
          messageValueFactory, service, conf);
      }
    }

    if (conf.getAsyncConf().needAllMsgs()) {
      // TODO-YH: Note that this returns
      // MessageStore<I, MessageWithSource<I, M>> rather than
      // MessageStore<I, M>!!
      if (vertexIdClass.equals(LongWritable.class) &&
          messageClass.equals(DoubleWritable.class)) {
        messageStore = new LongDoubleMessageWithSourceStore(
          (CentralizedServiceWorker<LongWritable, Writable, Writable>) service);
      } else {
        messageStore = new ByteArrayMessagesPerSourceVertexStore<I, M>(
          messageValueFactory, service, conf);
      }
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("newStore: Created " + messageStore.getClass() +
          " for vertex id " + conf.getVertexIdClass() +
          " and message value " + messageClass + " and" +
          (conf.useMessageCombiner() ? " message combiner " +
              conf.getMessageCombinerClass() : " no combiner"));
    }
    return messageStore;
  }

  @Override
  public void initialize(CentralizedServiceWorker<I, ?, ?> service,
      ImmutableClassesGiraphConfiguration<I, ?, ?> conf) {
    this.service = service;
    this.conf = conf;
  }

  @Override
  public boolean shouldTraverseMessagesInOrder() {
    return false;
  }
}
