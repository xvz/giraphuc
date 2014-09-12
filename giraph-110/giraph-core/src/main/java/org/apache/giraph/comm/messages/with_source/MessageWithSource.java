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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * YH: Writable encapsulating a message with a source id.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class MessageWithSource<I extends WritableComparable,
    M extends Writable> implements Writable {

  /** Message class */
  protected final MessageValueFactory<M> messageValueFactory;
  /** Giraph configuration */
  private final ImmutableClassesGiraphConfiguration<I, ?, ?> config;

  /** Source vertex id */
  private I srcId;
  /** Message */
  private M message;

  /**
   * Constructor when deserialization is not required.
   *
   * Attempting to deserialize into returned instance will result
   * in NullPointerException.
   */
  public MessageWithSource() {
    this.messageValueFactory = null;
    this.config = null;
  }

  /**
   * Constructor when deserialization is not required.
   *
   * Attempting to deserialize into returned instance will result
   * in NullPointerException.
   *
   * @param srcId Vertex id of the source (sender)
   * @param message Message being sent
   */
  public MessageWithSource(I srcId, M message) {
    this.messageValueFactory = null;
    this.config = null;
    this.srcId = srcId;
    this.message = message;
  }

  /**
   * Constructor for when deserialization is required.
   *
   * @param messageValueFactory Message class held in the store
   * @param config Hadoop configuration
   */
  public MessageWithSource(
      MessageValueFactory<M> messageValueFactory,
      ImmutableClassesGiraphConfiguration<I, ?, ?> config) {
    this.messageValueFactory = messageValueFactory;
    this.config = config;
  }

  /**
   * Set the source id and message.
   *
   * NOTE: The content of these parameters will be overwritten with
   * readFields(), so do not pass in user data when deserializing.
   *
   * @param srcId Vertex id of the source (sender)
   * @param message Message being sent
   */
  public void set(I srcId, M message) {
    this.srcId = srcId;
    this.message = message;
  }

  /**
   * Get the source vertex id. Reference is invalidated when set() or
   * readFields() is called.
   *
   * @return Vertex id of the source (sender)
   */
  public I getSource() {
    return srcId;
  }

  /**
   * Get the message. Reference is invalidated when set() or
   * readFields() is called.
   *
   * @return Message
   */
  public M getMessage() {
    return message;
  }

  /**
   * Get the source vertex id and release reference held by this class.
   *
   * @return Vertex id of the source (sender)
   */
  public I removeSource() {
    I tmp = srcId;
    this.srcId = null;
    return tmp;
  }

  /**
   * Get the message and release reference held by this class.
   *
   * @return Message
   */
  public M removeMessage() {
    M tmp = message;
    this.message = null;
    return tmp;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // NOTE: messageValueFactory and config WILL be null when readFields()
    // is called on an object not intended for deserialization. Instead of
    // silent failure, we let NullPointerException take care of it.

    // reuse objects whenever possible
    if (srcId == null) {
      srcId = config.createVertexId();
    }
    srcId.readFields(in);

    if (message == null) {
      message = messageValueFactory.newInstance();
    }
    message.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    srcId.write(out);
    message.write(out);
  }

  @Override
  public String toString() {
    return "MessageWithSource{srcId=" + srcId + ",msg=" + message + "}";
  }
}
