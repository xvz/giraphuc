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

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageWithPhase;
import org.apache.giraph.conf.AsyncConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.VertexIdMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.PairList;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Send a collection of vertex messages for a partition.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerMessagesRequest<I extends WritableComparable,
    M extends Writable> extends SendWorkerDataRequest<I, M,
    VertexIdMessages<I, M>> {

  /** Default constructor */
  public SendWorkerMessagesRequest() {
  }

  /**
   * Constructor used to send request.
   *
   * @param partVertMsgs Map of remote partitions =>
   *                     VertexIdMessages
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendWorkerMessagesRequest(
      PairList<Integer, VertexIdMessages<I, M>> partVertMsgs,
      ImmutableClassesGiraphConfiguration conf) {
    this.partitionVertexData = partVertMsgs;
    setConf(conf);
  }

  @Override
  public VertexIdMessages<I, M> createVertexIdData() {
    return new ByteArrayVertexIdMessages<I, M>(
        getConf().getOutgoingMessageValueFactory());
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_MESSAGES_REQUEST;
  }

  @Override
  public void doRequest(ServerData serverData) {
    doRequest(serverData, false);  // YH: wrapper call

    if (getConf().getAsyncConf().disableBarriers()) {
      // YH: signal to notify worker that remote message has arrived
      // (in case worker is blocking on "ready to finish" barrier)
      ((BspService) serverData.getServiceWorker()).
        getSuperstepReadyToFinishEvent().signal();
    }
  }

  @Override
  public void doLocalRequest(ServerData serverData) {
    doRequest(serverData, true);  // YH: wrapper call
  }

  /**
   * Helper function for doRequest() and doLocalRequest()
   *
   * @param serverData ServerData
   * @param isLocal Whether request is local or not
   */
  private void doRequest(ServerData serverData, boolean isLocal) {
    PairList<Integer, VertexIdMessages<I, M>>.Iterator
        iterator = partitionVertexData.getIterator();

    AsyncConfiguration asyncConf = getConf().getAsyncConf();

    MessageStore msgStore;
    if (isLocal && asyncConf.doLocalRead()) {
      // YH: use local message store if doing async and request is local
      msgStore = serverData.getLocalMessageStore();
    } else if (!isLocal && asyncConf.doRemoteRead()) {
      // YH: use remote message store if doing async and request is remote
      msgStore = serverData.getRemoteMessageStore();
    } else {
      // otherwise use default BSP incoming message store
      msgStore = serverData.getIncomingMessageStore();
    }

    MessageStore nextPhaseMsgStore = null;
    if (asyncConf.isMultiPhase()) {
      if (isLocal && asyncConf.doLocalRead()) {
        nextPhaseMsgStore = serverData.getNextPhaseLocalMessageStore();
      } else if (!isLocal && asyncConf.doRemoteRead()) {
        nextPhaseMsgStore = serverData.getNextPhaseRemoteMessageStore();
      } else {
        // YH: if BSP store is needed, this is still the one to use
        nextPhaseMsgStore = serverData.getIncomingMessageStore();
      }
    }

    // YH: if not using barriers, we have to track the number of
    // received bytes. This is the "counterpart" to counting sent
    // bytes in SendMessageCache#sendMessageRequest().
    //
    // Note: this is prone to comm thread contention, but there's nowhere
    // else to easily track this statistic---received messages go straight
    // from raw channel read to decoding to request processing (here).
    if (!isLocal && asyncConf.disableBarriers()) {
      asyncConf.addRecvBytes(this.getSerializedSize());
    }

    MessageStore currStore = msgStore;
    while (iterator.hasNext()) {
      iterator.next();
      int partitionId = iterator.getCurrentFirst();

      if (asyncConf.isMultiPhase()) {
        currStore = MessageWithPhase.forNextPhase(partitionId) ?
          nextPhaseMsgStore : msgStore;
        partitionId = MessageWithPhase.decode(partitionId);
      }

      try {
        currStore.addPartitionMessages(partitionId,
                                       iterator.getCurrentSecond());
      } catch (IOException e) {
        throw new RuntimeException(isLocal ? "doRequest" : "doLocalRequest" +
                                   ": Got IOException ", e);
      }
    }
  }
}
