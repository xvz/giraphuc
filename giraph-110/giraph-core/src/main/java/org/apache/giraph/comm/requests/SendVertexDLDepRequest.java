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
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

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

  /** Vertex id */
  private I vertexId;
  /** Dependency id */
  private I depId;

  /**
   * Constructor used for reflection only
   */
  public SendVertexDLDepRequest() {
  }

  /**
   * Constructor.
   *
   * @param vertexId Vertex id of philosopher
   * @param depId Vertex id of new dependency
   * @param conf ImmutableClassesGiraphConfiguration
   */
  public SendVertexDLDepRequest(
      I vertexId, I depId,
      ImmutableClassesGiraphConfiguration conf) {
    setConf(conf);    // getConf() is null until properly set
    this.vertexId = WritableUtils.clone(vertexId, getConf());
    this.depId = WritableUtils.clone(depId, getConf());
  }

  @Override
  public void readFieldsRequest(DataInput input) throws IOException {
    // conf will be set by server handler
    vertexId = getConf().createVertexId();
    depId = getConf().createVertexId();
    vertexId.readFields(input);
    depId.readFields(input);
  }

  @Override
  public void writeRequest(DataOutput output) throws IOException {
    vertexId.write(output);
    depId.write(output);
  }

  @Override
  public RequestType getType() {
    return RequestType.SEND_VERTEX_DL_DEP_REQUEST;
  }

  @Override
  public void doRequest(ServerData<I, V, E> serverData) {
    serverData.getServiceWorker().getVertexPhilosophersTable().
      receiveDependency(vertexId, depId);
  }

  @Override
  public void doLocalRequest(ServerData<I, V, E> serverData) {
    throw new RuntimeException("This is NEVER a local request!");
  }

  @Override
  public int getSerializedSize() {
    // ids are not serialized beforehand, so can't determine size
    return WritableRequest.UNKNOWN_SIZE;
  }
}
