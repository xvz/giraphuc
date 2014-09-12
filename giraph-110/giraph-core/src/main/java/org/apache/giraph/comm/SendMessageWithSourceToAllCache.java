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
import org.apache.giraph.comm.messages.with_source.MessageWithSource;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessagesWithSource;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * YH: Aggregates the messages, with source, to be sent to workers
 * so they can be sent in bulk. Not thread-safe.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendMessageWithSourceToAllCache<
    I extends WritableComparable, M extends Writable>
    extends SendMessageToAllCache<I, MessageWithSource<I, M>> {
  /**
   * Constructor
   *
   * @param conf Giraph configuration
   * @param serviceWorker Service worker
   * @param processor NettyWorkerClientRequestProcessor
   * @param maxMsgSize Max message size sent to a worker
   */
  public SendMessageWithSourceToAllCache(
      ImmutableClassesGiraphConfiguration conf,
      CentralizedServiceWorker<?, ?, ?> serviceWorker,
      NettyWorkerClientRequestProcessor<I, ?, ?> processor,
      int maxMsgSize) {
    super(conf, serviceWorker, processor, maxMsgSize);
  }

  @Override
  public VertexIdMessages<I, MessageWithSource<I, M>>
  createVertexIdData() {
    return new ByteArrayVertexIdMessagesWithSource<I, M>(
        getConf().getOutgoingMessageValueFactory());
  }
}
