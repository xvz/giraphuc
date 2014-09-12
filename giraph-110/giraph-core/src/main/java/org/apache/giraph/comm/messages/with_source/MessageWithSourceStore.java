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

package org.apache.giraph.comm.messages.with_source;

import java.io.IOException;
import org.apache.giraph.comm.messages.MessageStore;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * YH: Interface for message store that also records source ids.
 *
 * Note that MessageStore's "message" type is MessageWithSource<I, M>!!
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public interface MessageWithSourceStore<I extends WritableComparable,
    M extends Writable> extends MessageStore<I, MessageWithSource<I, M>> {

  /**
   * Gets messages (without source) for a vertex. The lifetime of every
   * message and source is only guaranteed until the iterator's next()
   * method is called. Do not hold references to objects returned by
   * this iterator.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Iterable of messages for a vertex id (without source)
   * @throws java.io.IOException
   */
  Iterable<M> getVertexMessagesWithoutSource(I vertexId) throws IOException;

  /**
   * Gets messages (without source) for a vertex and removes it from the
   * underlying store.
   * The lifetime of every message is only guaranteed until the iterator's
   * next() method is called. Do not hold references to objects returned
   * by this iterator.
   *
   * Similar to getVertexMessagesWithoutSource() followed by
   * clearVertexMessages(), but this is "atomic" and returns an iterable
   * after clearing.
   *
   * @param vertexId Vertex id for which we want to get messages
   * @return Iterable of messages for a vertex id (without source)
   * @throws java.io.IOException
   */
  Iterable<M> removeVertexMessagesWithoutSource(I vertexId) throws IOException;
}
