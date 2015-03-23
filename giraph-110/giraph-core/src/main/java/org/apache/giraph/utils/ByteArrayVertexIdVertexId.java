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

package org.apache.giraph.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * YH: Stores vertex id and vertex id (data) pairs in a single byte array.
 *
 * @param <I> Vertex id
 */
public class ByteArrayVertexIdVertexId<I extends WritableComparable>
  extends ByteArrayVertexIdData<I, I> {

  @Override
  public I createData() {
    return getConf().createVertexId();
  }

  @Override
  public void writeData(ExtendedDataOutput out, I vertexId) throws IOException {
    vertexId.write(out);
  }

  @Override
  public void readData(ExtendedDataInput in, I vertexId) throws IOException {
    vertexId.readFields(in);
  }
}
