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

package org.apache.giraph.comm.netty;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageStoreFactory;
import org.apache.giraph.comm.netty.handler.WorkerRequestServerHandler;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexMutations;
import org.apache.giraph.graph.VertexResolver;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.utils.ReflectionUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map.Entry;

import static org.apache.giraph.conf.GiraphConstants.MESSAGE_STORE_FACTORY_CLASS;

/**
 * Netty worker server that implement {@link WorkerServer} and contains
 * the actual {@link ServerData}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class NettyWorkerServer<I extends WritableComparable,
    V extends Writable, E extends Writable>
    implements WorkerServer<I, V, E> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(NettyWorkerServer.class);
  /** Hadoop configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> service;
  /** Netty server that does that actual I/O */
  private final NettyServer nettyServer;
  /** Server data storage */
  private final ServerData<I, V, E> serverData;
  /** Mapper context */
  private final Mapper<?, ?, ?, ?>.Context context;

  /**
   * Constructor to start the server.
   *
   * @param conf Configuration
   * @param service Service to get partition mappings
   * @param context Mapper context
   */
  public NettyWorkerServer(ImmutableClassesGiraphConfiguration<I, V, E> conf,
      CentralizedServiceWorker<I, V, E> service,
      Mapper<?, ?, ?, ?>.Context context) {
    this.conf = conf;
    this.service = service;
    this.context = context;

    serverData =
        new ServerData<I, V, E>(service, conf, createMessageStoreFactory(),
            context);

    nettyServer = new NettyServer(conf,
        new WorkerRequestServerHandler.Factory<I, V, E>(serverData),
        service.getWorkerInfo(), context);
    nettyServer.start();
  }

  /**
   * Decide which message store should be used for current application,
   * and create the factory for that store
   *
   * @return Message store factory
   */
  private MessageStoreFactory<I, Writable, MessageStore<I, Writable>>
  createMessageStoreFactory() {
    Class<? extends MessageStoreFactory> messageStoreFactoryClass =
        MESSAGE_STORE_FACTORY_CLASS.get(conf);

    MessageStoreFactory messageStoreFactoryInstance =
        ReflectionUtils.newInstance(messageStoreFactoryClass);
    messageStoreFactoryInstance.initialize(service, conf);

    return messageStoreFactoryInstance;
  }

  @Override
  public InetSocketAddress getMyAddress() {
    return nettyServer.getMyAddress();
  }

  @Override
  public void prepareSuperstep() {
    serverData.prepareSuperstep();

    // YH: this is safe even when we read messages immediately, wrt part
    // where missing vertices are added. This is b/c if vertex doesn't
    // exist yet, we won't encounter them in compute loop, so their messages
    // are correctly delayed until subsequent superstep.
    resolveMutations();
  }

  /**
   * Resolve mutation requests.
   */
  private void resolveMutations() {
    Multimap<Integer, I> resolveVertexIndices = HashMultimap.create(
        service.getPartitionStore().getNumPartitions(), 100);
    // Add any mutated vertex indices to be resolved
    for (Entry<I, VertexMutations<I, V, E>> e :
        serverData.getVertexMutations().entrySet()) {
      I vertexId = e.getKey();
      Integer partitionId = service.getPartitionId(vertexId);
      if (!resolveVertexIndices.put(partitionId, vertexId)) {
        throw new IllegalStateException(
            "resolveMutations: Already has missing vertex on this " +
                "worker for " + vertexId);
      }
    }
    // Keep track of the vertices which are not here but have received messages
    for (Integer partitionId : service.getPartitionStore().getPartitionIds()) {
      // YH: if either immediate local or remote reads are not used,
      // we have to include BSP message store
      Iterable<I> destinations;
      int firstItrSize;

      // YH: less intuitive but avoids unnecessary Iterables.concat()
      // if doing immediate remote reads, select remote message store
      // otherwise, select BSP message store
      if (conf.getAsyncConf().doRemoteRead()) {
        destinations = serverData.getRemoteMessageStore().
          getPartitionDestinationVertices(partitionId);
        // TODO-YH: generally these are all Collections, but
        // this degrades to O(n) performance if not
        firstItrSize = Iterables.size(destinations);
      } else {
        destinations = serverData.getCurrentMessageStore().
          getPartitionDestinationVertices(partitionId);
        firstItrSize = Iterables.size(destinations);
      }

      // if doing immediate local reads, concat local message store
      // if not, AND remote read is being used, concat BSP message store
      // otherwise, destination already uses BSP message store
      if (conf.getAsyncConf().doLocalRead()) {
        destinations = Iterables.concat(destinations,
                          serverData.getLocalMessageStore().
                          getPartitionDestinationVertices(partitionId));
      } else {
        if (conf.getAsyncConf().doRemoteRead()) {
          destinations = Iterables.concat(destinations,
                            serverData.getCurrentMessageStore().
                            getPartitionDestinationVertices(partitionId));
        }
      }

      if (!Iterables.isEmpty(destinations)) {
        Partition<I, V, E> partition =
            service.getPartitionStore().getOrCreatePartition(partitionId);
        int done = 0;
        for (I vertexId : destinations) {
          if (partition.getVertex(vertexId) == null) {
            // TODO-YH: this will throw error with any async mode enabled,
            // because we're using TWO message stores, so it can easily be
            // the case that a missing vertex is repeated on both stores.
            //
            // Best solution is to do one message store with checks, and
            // then do the other store WITHOUT checks.
            //
            // Note: multimap => same K can map to multiple Vs; put()
            // returns false if K, V pair already exists
            if (!resolveVertexIndices.put(partitionId, vertexId) &&
                done < firstItrSize) {
              throw new IllegalStateException(
                  "resolveMutations: Already has missing vertex on this " +
                      "worker for " + vertexId);
            }
          }
          done++;
        }
        service.getPartitionStore().putPartition(partition);
      }
    }
    // Resolve all graph mutations
    VertexResolver<I, V, E> vertexResolver = conf.createVertexResolver();
    for (Entry<Integer, Collection<I>> e :
        resolveVertexIndices.asMap().entrySet()) {
      Partition<I, V, E> partition =
          service.getPartitionStore().getOrCreatePartition(e.getKey());
      for (I vertexIndex : e.getValue()) {
        Vertex<I, V, E> originalVertex =
            partition.getVertex(vertexIndex);

        VertexMutations<I, V, E> mutations = null;
        VertexMutations<I, V, E> vertexMutations =
            serverData.getVertexMutations().get(vertexIndex);
        if (vertexMutations != null) {
          synchronized (vertexMutations) {
            mutations = vertexMutations.copy();
          }
          serverData.getVertexMutations().remove(vertexIndex);
        }

        // YH: check remote and local message stores if needed
        boolean hasMessages = false;

        if (!conf.getAsyncConf().doLocalRead() ||
            !conf.getAsyncConf().doRemoteRead()) {
          hasMessages |= serverData.getCurrentMessageStore().
            hasMessagesForVertex(vertexIndex);
        }

        if (conf.getAsyncConf().doRemoteRead()) {
          hasMessages |= serverData.getRemoteMessageStore().
            hasMessagesForVertex(vertexIndex);
        }

        if (conf.getAsyncConf().doLocalRead()) {
          hasMessages |= serverData.getLocalMessageStore().
            hasMessagesForVertex(vertexIndex);
        }

        Vertex<I, V, E> vertex = vertexResolver.resolve(
            vertexIndex, originalVertex, mutations, hasMessages);
        context.progress();

        if (LOG.isDebugEnabled()) {
          LOG.debug("resolveMutations: Resolved vertex index " +
              vertexIndex + " with original vertex " +
              originalVertex + ", returned vertex " + vertex +
              " on superstep " + service.getSuperstep() +
              " with mutations " +
              mutations);
        }
        if (vertex != null) {
          partition.putVertex(vertex);
        } else if (originalVertex != null) {
          partition.removeVertex(originalVertex.getId());
        }
      }
      service.getPartitionStore().putPartition(partition);
    }
    if (!serverData.getVertexMutations().isEmpty()) {
      throw new IllegalStateException("resolveMutations: Illegally " +
          "still has " + serverData.getVertexMutations().size() +
          " mutations left.");
    }
  }

  @Override
  public ServerData<I, V, E> getServerData() {
    return serverData;
  }

  @Override
  public void close() {
    nettyServer.stop();
  }
}
