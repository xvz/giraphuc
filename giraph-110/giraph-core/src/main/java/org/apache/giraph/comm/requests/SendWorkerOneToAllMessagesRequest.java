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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map.Entry;

import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.utils.ByteArrayOneToAllMessages;
import org.apache.giraph.utils.ByteArrayVertexIdMessages;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Send a collection of one-to-all messages to a worker.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class SendWorkerOneToAllMessagesRequest<I extends WritableComparable,
    M extends Writable> extends WritableRequest<I, Writable, Writable>
    implements WorkerRequest<I, Writable, Writable> {
  /** The byte array of one-to-all messages */
  private ByteArrayOneToAllMessages<I, M> oneToAllMsgs;

  /**
   * Constructor used for reflection only.
   */
  public SendWorkerOneToAllMessagesRequest() { }

  /**
   * Constructor used to send request.
   *
   * @param oneToAllMsgs A byte array of all one-to-all messages
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendWorkerOneToAllMessagesRequest(
      ByteArrayOneToAllMessages<I, M> oneToAllMsgs,
      ImmutableClassesGiraphConfiguration conf) {
    this.oneToAllMsgs = oneToAllMsgs;
    setConf(conf);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_WORKER_ONETOALL_MESSAGES_REQUEST;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    oneToAllMsgs = new ByteArrayOneToAllMessages<I, M>(
      getConf().<M>getOutgoingMessageValueFactory());
    oneToAllMsgs.setConf(getConf());
    oneToAllMsgs.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    this.oneToAllMsgs.write(output);
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() + this.oneToAllMsgs.getSerializedSize();
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
    doRequest(serverData, true);   // YH: wrapper call
  }

  /**
   * Helper function for doRequest() and doLocalRequest()
   *
   * @param serverData ServerData
   * @param isLocal Whether request is local or not
   */
  private void doRequest(ServerData serverData, boolean isLocal) {
    CentralizedServiceWorker<I, ?, ?> serviceWorker =
      serverData.getServiceWorker();
    // Get the initial size of ByteArrayVertexIdMessages per partition
    // on this worker. To make sure every ByteArrayVertexIdMessages to have
    // enough space to store the messages, we divide the original one-to-all
    // message size by the number of partitions and double the size
    // (Assume the major component in one-to-all message is a id list.
    // Now each target id has a copy of message,
    // therefore we double the buffer size)
    // to get the initial size of ByteArrayVertexIdMessages.
    int initialSize = oneToAllMsgs.getSize() /
      serverData.getPartitionStore().getNumPartitions() * 2;
    // Create ByteArrayVertexIdMessages for
    // message reformatting.
    Int2ObjectOpenHashMap<ByteArrayVertexIdMessages>
      partitionIdMsgs =
        new Int2ObjectOpenHashMap<ByteArrayVertexIdMessages>();

    // Put data from ByteArrayOneToAllMessages to ByteArrayVertexIdMessages
    ExtendedDataInput reader = oneToAllMsgs.getOneToAllMessagesReader();
    I vertexId = getConf().createVertexId();
    M msg = oneToAllMsgs.createMessage();
    int idCount = 0;
    int partitionId = 0;
    try {
      while (reader.available() != 0) {
        msg.readFields(reader);
        idCount = reader.readInt();
        for (int i = 0; i < idCount; i++) {
          vertexId.readFields(reader);
          PartitionOwner owner =
            serviceWorker.getVertexPartitionOwner(vertexId);
          partitionId = owner.getPartitionId();
          ByteArrayVertexIdMessages<I, M> idMsgs =
            partitionIdMsgs.get(partitionId);
          if (idMsgs == null) {
            idMsgs = new ByteArrayVertexIdMessages<I, M>(
              getConf().<M>getOutgoingMessageValueFactory());
            idMsgs.setConf(getConf());
            idMsgs.initialize(initialSize);
            partitionIdMsgs.put(partitionId, idMsgs);
          }
          idMsgs.add(vertexId, msg);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("doRequest: Got IOException ", e);
    }

    // Read ByteArrayVertexIdMessages and write to message store
    MessageStore msgStore;
    if (getConf().getAsyncConf().isAsync()) {
      msgStore = isLocal ?
        serverData.getLocalMessageStore() :
        serverData.getRemoteMessageStore();
    } else {
      msgStore = serverData.getIncomingMessageStore();
    }

    // TODO-YH: implement multi-phase stuff

    // YH: if not using barriers, we have to track the number of
    // received bytes. This is the "counterpart" to counting sent
    // bytes in SendMessageCache#sendMessageRequest().
    //
    // Note: this is prone to comm thread contention, but there's nowhere
    // else to easily track this statistic---received messages go straight
    // from raw channel read to decoding to request processing (here).
    if (!isLocal && getConf().getAsyncConf().disableBarriers()) {
      getConf().getAsyncConf().addRecvBytes(this.getSerializedSize());
    }

    try {
      for (Entry<Integer, ByteArrayVertexIdMessages> idMsgs :
          partitionIdMsgs.entrySet()) {
        if (!idMsgs.getValue().isEmpty()) {
          msgStore.addPartitionMessages(idMsgs.getKey(), idMsgs.getValue());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(isLocal ? "doRequest" : "doLocalRequest" +
                                 ": Got IOException ", e);
    }
  }
}
