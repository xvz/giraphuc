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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.comm.messages.MessageWithPhaseUtils;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of vertex messages for a partition. It adds messages to
 * current message store and it should be used only during partition exchange.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message data
 */
public class SendPartitionCurrentMessagesRequest<I extends WritableComparable,
  V extends Writable, E extends Writable, M extends Writable> extends
  WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {
  /** Destination partition for these vertices' messages*/
  private int partitionId;
  /** Map of destination vertex ID's to message lists */
  private ByteArrayVertexIdMessages<I, M> vertexIdMessageMap;

  /** Constructor used for reflection only */
  public SendPartitionCurrentMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param partitionId Partition to send the request to
   * @param vertexIdMessages Map of messages to send
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendPartitionCurrentMessagesRequest(int partitionId,
    ByteArrayVertexIdMessages<I, M> vertexIdMessages,
    ImmutableClassesGiraphConfiguration conf) {
    super();
    this.partitionId = partitionId;
    this.vertexIdMessageMap = vertexIdMessages;
    setConf(conf);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_PARTITION_CURRENT_MESSAGES_REQUEST;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    partitionId = input.readInt();
    // At this moment the Computation class have already been replaced with
    // the new one, and we deal with messages from previous superstep
    vertexIdMessageMap = new ByteArrayVertexIdMessages<I, M>(
        getConf().<M>getIncomingMessageValueFactory());
    vertexIdMessageMap.setConf(getConf());
    vertexIdMessageMap.initialize();
    vertexIdMessageMap.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(partitionId);
    vertexIdMessageMap.write(output);
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    doRequest(serverData, false);  // YH: wrapper call
  }

  @Override
  public void doLocalRequest(ServerData<I, V, E> serverData) {
    doRequest(serverData, true);   // YH: wrapper call
  }

  /**
   * Helper function for doRequest() and doLocalRequest()
   *
   * @param serverData ServerData
   * @param isLocal Whether request is local or not
   */
  private void doRequest(ServerData<I, V, E> serverData, boolean isLocal) {
    // YH: destinations should always be to remote workers
    if (isLocal) {
      throw new IllegalStateException("doLocalRequest: " +
                                      "Destination is the local worker.");
    }

    MessageStore msgStore;
    if (getConf().getAsyncConf().isAsync()) {
      msgStore = serverData.<M>getRemoteMessageStore();
    } else {
      // otherwise use default BSP incoming message store
      msgStore = serverData.<M>getIncomingMessageStore();
    }

    MessageStore nextPhaseMsgStore = null;
    if (getConf().getAsyncConf().isMultiPhase()) {
      nextPhaseMsgStore = serverData.<M>getNextPhaseRemoteMessageStore();
    }

    MessageStore currStore;
    int partId = partitionId;

    if (MessageWithPhaseUtils.forNextPhase(partId)) {
      currStore = nextPhaseMsgStore;
      partId = MessageWithPhaseUtils.decode(partId);
    } else {
      currStore = msgStore;
    }

    try {
      currStore.addPartitionMessages(partId, vertexIdMessageMap);
    } catch (IOException e) {
      throw new RuntimeException("doRequest: Got IOException ", e);
    }
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() + 4 +
        vertexIdMessageMap.getSerializedSize();
  }
}
