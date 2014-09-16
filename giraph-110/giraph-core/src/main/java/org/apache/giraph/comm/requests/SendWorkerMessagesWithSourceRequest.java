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

package org.apache.giraph.comm.requests;

import org.apache.giraph.comm.messages.with_source.MessageWithSource;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessagesWithSource;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * YH: Send a collection of vertex messages with source for a partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerMessagesWithSourceRequest<
    I extends WritableComparable, M extends Writable>
    extends SendWorkerMessagesRequest<I, MessageWithSource<I, M>> {

  /** Default constructor */
  public SendWorkerMessagesWithSourceRequest() {
  }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =>
   *                     VertexIdMessages
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendWorkerMessagesWithSourceRequest(
      PairList<Integer, VertexIdMessages<I, MessageWithSource<I, M>>>
      partVertMsgs, ImmutableClassesGiraphConfiguration conf) {
    super(partVertMsgs, conf);
  }

  @Override
  public VertexIdMessages<I, MessageWithSource<I, M>> createVertexIdData() {
    return new ByteArrayVertexIdMessagesWithSource<I, M>(
        getConf().getOutgoingMessageValueFactory());
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_WITH_SOURCE_REQUEST;
  }
}
