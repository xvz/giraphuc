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

package org.apache.giraph.comm;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.aggregators.AllAggregatorServerData;
import org.apache.giraph.comm.aggregators.OwnerAggregatorServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.EdgeStore;
import org.apache.giraph.edge.EdgeStoreFactory;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.partition.DiskBackedPartitionStore;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.SimplePartitionStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Anything that the server stores
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class ServerData<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Partition store for this worker. */
  private volatile PartitionStore<I, V, E> partitionStore;
  /** Edge store for this worker. */
  private final EdgeStore<I, V, E> edgeStore;
  /** Message store factory */
  private final MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
  messageStoreFactory;

  /**
   * Message store for incoming messages (messages which will be consumed
   * in the next super step)
   */
  private volatile MessageStore<I, Writable> incomingMessageStore;
  /**
   * Message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   */
  private volatile MessageStore<I, Writable> currentMessageStore;

  /**
   * YH: Message store for messages received from remote neighbours.
   * Messages are received in the current OR previous superstep, and are
   * consumed in the current OR next superstep.
   */
  private volatile MessageStore<I, Writable> remoteMessageStore;

  /**
   * YH: Message store for local messages (messages which we received in
   * the current OR previous superstep and is consumed in the current OR
   * next superstep).
   */
  private volatile MessageStore<I, Writable> localMessageStore;

  /**
   * Map of partition ids to incoming vertex mutations from other workers.
   * (Synchronized access to values)
   */
  private final ConcurrentHashMap<I, VertexMutations<I, V, E>>
  vertexMutations = new ConcurrentHashMap<I, VertexMutations<I, V, E>>();
  /**
   * Holds aggregtors which current worker owns from current superstep
   */
  private final OwnerAggregatorServerData ownerAggregatorData;
  /**
   * Holds old aggregators from previous superstep
   */
  private final AllAggregatorServerData allAggregatorData;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  /**
   * Constructor.
   *
   * @param service Service worker
   * @param conf Configuration
   * @param messageStoreFactory Factory for message stores
   * @param context Mapper context
   */
  public ServerData(
      CentralizedServiceWorker<I, V, E> service,
      ImmutableClassesGiraphConfiguration<I, V, E> conf,
      MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
          messageStoreFactory,
      Mapper<?, ?, ?, ?>.Context context) {
    this.serviceWorker = service;
    this.conf = conf;
    this.messageStoreFactory = messageStoreFactory;
    if (GiraphConstants.USE_OUT_OF_CORE_GRAPH.get(conf)) {
      partitionStore =
          new DiskBackedPartitionStore<I, V, E>(conf, context,
              getServiceWorker());
    } else {
      partitionStore =
          new SimplePartitionStore<I, V, E>(conf, context);
    }
    EdgeStoreFactory<I, V, E> edgeStoreFactory = conf.createEdgeStoreFactory();
    edgeStoreFactory.initialize(service, conf, context);
    edgeStore = edgeStoreFactory.newStore();
    ownerAggregatorData = new OwnerAggregatorServerData(context, conf);
    allAggregatorData = new AllAggregatorServerData(context, conf);
  }

  public EdgeStore<I, V, E> getEdgeStore() {
    return edgeStore;
  }

  /**
   * Return the partition store for this worker.
   *
   * @return The partition store
   */
  public PartitionStore<I, V, E> getPartitionStore() {
    return partitionStore;
  }

  /**
   * Get message store for incoming messages (messages which will be consumed
   * in the next super step)
   *
   * @param <M> Message data
   * @return Incoming message store
   */
  public <M extends Writable> MessageStore<I, M> getIncomingMessageStore() {
    return (MessageStore<I, M>) incomingMessageStore;
  }

  /**
   * Get message store for current messages (messages which we received in
   * previous super step and which will be consumed in current super step)
   *
   * @param <M> Message data
   * @return Current message store
   */
  public <M extends Writable> MessageStore<I, M> getCurrentMessageStore() {
    return (MessageStore<I, M>) currentMessageStore;
  }

  /**
   * YH: Get message store for remote messages (messages which we received in
   * the current OR previous superstep and is consumed in the current OR
   * next superstep).
   *
   * @param <M> Message data
   * @return Remote message store
   */
  public <M extends Writable> MessageStore<I, M> getRemoteMessageStore() {
    return (MessageStore<I, M>) remoteMessageStore;
  }

  /**
   * YH: Get message store for local messages (messages which we received in
   * the current OR previous superstep and is consumed in the current OR
   * next superstep).
   *
   * @param <M> Message data
   * @return Local message store
   */
  public <M extends Writable> MessageStore<I, M> getLocalMessageStore() {
    return (MessageStore<I, M>) localMessageStore;
  }

  /** Prepare for next super step */
  public void prepareSuperstep() {
    // YH: if doing immediate remote reads, use our own message store
    // rather than the default current/incoming message stores
    if (conf.getAsyncConf().doRemoteRead()) {
      if (remoteMessageStore == null) {
        remoteMessageStore =
          messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
      }
    } else {
      // these stores will never be initialized if doRemoteRead() is true
      if (currentMessageStore != null) {
        try {
          currentMessageStore.clearAll();
        } catch (IOException e) {
          throw new IllegalStateException(
            "Failed to clear previous message store");
        }
      }
      currentMessageStore =
        incomingMessageStore != null ? incomingMessageStore :
        messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
      incomingMessageStore =
        messageStoreFactory.newStore(conf.getOutgoingMessageValueFactory());
    }


    // YH: create localMessageStore if needed; this persists across supersteps
    if (conf.getAsyncConf().doLocalRead()) {
      // TODO-YH: this breaks if Incoming and Outgoing factories are not same!!
      // this should PROBABLY be Incoming... so is there bug above?
      // (i.e., current uses incoming but incoming uses outgoing)
      if (localMessageStore == null) {
        localMessageStore =
          messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
      }
      //if (localMessageStore == null) {
      //  localMessageStoreA =
      //    messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
      //  localMessageStoreB =
      //    messageStoreFactory.newStore(conf.getIncomingMessageValueFactory());
      //
      //  localMessageStore = localMessageStoreA;
      //}
      //
      //// TODO-YH: ????.. double check if this is even needed
      //if (conf.getAsyncConf().isNewPhase()) {
      //  if (localMessageStore == localMessageStoreA) {
      //    localMessageStoreB.clearAll();
      //    localMessageStore = localMessageStoreB;
      //  } else {
      //    localMessageStoreA.clearAll();
      //    localMessageStore = localMessageStoreA;
      //  }
      //}
    }
  }

  /**
   * Get the vertex mutations (synchronize on the values)
   *
   * @return Vertex mutations
   */
  public ConcurrentHashMap<I, VertexMutations<I, V, E>>
  getVertexMutations() {
    return vertexMutations;
  }

  /**
   * Get holder for aggregators which current worker owns
   *
   * @return Holder for aggregators which current worker owns
   */
  public OwnerAggregatorServerData getOwnerAggregatorData() {
    return ownerAggregatorData;
  }

  /**
   * Get holder for aggregators from previous superstep
   *
   * @return Holder for aggregators from previous superstep
   */
  public AllAggregatorServerData getAllAggregatorData() {
    return allAggregatorData;
  }

  /**
   * Get the reference of the service worker.
   *
   * @return CentralizedServiceWorker
   */
  public CentralizedServiceWorker<I, V, E> getServiceWorker() {
    return this.serviceWorker;
  }
}
