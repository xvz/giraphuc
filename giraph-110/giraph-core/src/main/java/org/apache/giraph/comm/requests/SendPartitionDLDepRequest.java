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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * YH: Send a partition dependency to another worker.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public class SendPartitionDLDepRequest<I extends WritableComparable,
    V extends Writable, E extends Writable> extends
    WritableRequest<I, V, E> implements WorkerRequest<I, V, E> {

  /** Partition id */
  private int pId;
  /** Dependency id */
  private int depId;

  /**
   * Constructor used for reflection only
   */
  public SendPartitionDLDepRequest() {
  }

  /**
   * Constructor.
   *
   * @param pId Partition id of philosopher
   * @param depId Partition id of new dependency
   */
  public SendPartitionDLDepRequest(int pId, int depId) {
    this.pId = pId;
    this.depId = depId;
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    pId = input.readInt();
    depId = input.readInt();
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    output.writeInt(pId);
    output.writeInt(depId);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_PARTITION_DL_DEP_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    serverData.getServiceWorker().getPartitionPhilosophersTable().
      receiveDependency(pId, depId);
  }

  @Override
  public void doLocalRequest(ServerData<I, V, E> serverData) {
    throw new RuntimeException("This is NEVER a local request!");
  }

  @Override
  public int getSerializedSize() {
    // two integers = 8 bytes
    return super.getSerializedSize() + 8;
  }
}
