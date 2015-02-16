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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * YH: Send the global token to another worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class SendGlobalTokenRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {
  /**
   * Default constructor.
   */
  public SendGlobalTokenRequest() { }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_GLOBAL_TOKEN_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    // Server handler instantiates received requests using
    // ReflectionUtils(class, config), which will set conf properly
    // since WritableRequest is configurable (see RequestDecoder)
    getConf().getAsyncConf().getGlobalToken();

    // signal worker if needed
    if (getConf().getAsyncConf().disableBarriers()) {
      ((BspService) serverData.getServiceWorker()).
        getSuperstepReadyToFinishEvent().signal();
    }
  }

  @Override
  public void doLocalRequest(ServerData<I, V, E> serverData) {
    throw new RuntimeException("This is NEVER a local request!");
  }

  // this request has no data, so use super.getSerializedSize()
}
