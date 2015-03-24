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

import org.apache.giraph.comm.ServerData;
import org.apache.giraph.utils.ByteArrayVertexIdVertexId;
import org.apache.giraph.utils.VertexIdData;
import org.apache.giraph.utils.VertexIdDataIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * YH: Send a vertex dependency to another worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class SendVertexDLDepRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {

  /** Message data */
  private VertexIdData<I, I> messages;

  /**
   * Constructor used for reflection only
   */
  public SendVertexDLDepRequest() {
  }

  /**
   * Constructor.
   *
   * @param messages Messages to send
   */
  public SendVertexDLDepRequest(VertexIdData<I, I> messages) {
    this.messages = messages;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    messages = new ByteArrayVertexIdVertexId<I>();
    messages.setConf(getConf());
    messages.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    messages.write(output);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_VERTEX_DL_DEP_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    VertexIdDataIterator<I, I> itr = messages.getVertexIdDataIterator();
    while (itr.hasNext()) {
      itr.next();
      serverData.getServiceWorker().getVertexPhilosophersTable().
        receiveDependency(itr.getCurrentVertexId(), itr.getCurrentData());
    }
  }

  @Override
  public void doLocalRequest(ServerData<I, V, E> serverData) {
    throw new RuntimeException("This is NEVER a local request!");
  }

  @Override
  public int getSerializedSize() {
    return super.getSerializedSize() + messages.getSerializedSize();
  }
}
