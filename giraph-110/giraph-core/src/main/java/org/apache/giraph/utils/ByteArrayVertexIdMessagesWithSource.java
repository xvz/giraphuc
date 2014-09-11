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

import org.apache.giraph.comm.messages.with_source.MessageWithSource;
import org.apache.giraph.factories.MessageValueFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * YH: Stores vertex id and message-with-source pairs in a single byte array.
 *
 * Note that ByteArrayVertexIdData is <I, M>, since all iterators process
 * away the presence of the source vertex id.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
@SuppressWarnings("unchecked")
public class ByteArrayVertexIdMessagesWithSource<
  I extends WritableComparable, M extends Writable>
  extends ByteArrayVertexIdData<I, MessageWithSource<I, M>>
  implements VertexIdMessages<I, MessageWithSource<I, M>>,
             VertexIdMessagesWithSource<I, M> {

  /** Message value class */
  private MessageValueFactory<M> messageValueFactory;
  /** Add the message size to the stream? (Depends on the message store) */
  private boolean useMessageSizeEncoding = false;

  /**
   * Constructor
   *
   * @param messageValueFactory Class for messages
   */
  public ByteArrayVertexIdMessagesWithSource(
      MessageValueFactory<M> messageValueFactory) {
    this.messageValueFactory = messageValueFactory;
  }

  /**
   * Set whether message sizes should be encoded.  This should only be a
   * possibility when not combining.  When combining, all messages need to be
   * de-serialized right away, so this won't help.
   */
  private void setUseMessageSizeEncoding() {
    if (!getConf().useMessageCombiner()) {
      useMessageSizeEncoding = getConf().useMessageSizeEncoding();
    } else {
      useMessageSizeEncoding = false;
    }
  }

  @Override
  public MessageWithSource<I, M> createData() {
    return new MessageWithSource(messageValueFactory, getConf());
  }

  @Override
  public void writeData(ExtendedDataOutput out,
                        MessageWithSource<I, M> message) throws IOException {
    message.write(out);
  }

  @Override
  public void readData(ExtendedDataInput in,
                       MessageWithSource<I, M> message) throws IOException {
    message.readFields(in);
  }

  @Override
  public void initialize() {
    super.initialize();
    setUseMessageSizeEncoding();
  }

  @Override
  public void initialize(int expectedSize) {
    super.initialize(expectedSize);
    setUseMessageSizeEncoding();
  }

  @Override
  public ByteStructVertexIdMessageIterator<I, MessageWithSource<I, M>>
  getVertexIdMessageIterator() {
    throw new UnsupportedOperationException(
        "Use getVertexIdMessageWithSourceIterator() instead");
  }

  @Override
  public ByteStructVertexIdMessageBytesIterator<I, MessageWithSource<I, M>>
  getVertexIdMessageBytesIterator() {
    throw new UnsupportedOperationException(
        "Use getVertexIdMessageWithSourceBytesIterator() instead");
  }

  @Override
  public ByteStructVertexIdMessageWithSourceIterator<I, M>
  getVertexIdMessageWithSourceIterator() {
    return new ByteStructVertexIdMessageWithSourceIterator<I, M>(this);
  }

  @Override
  public ByteStructVertexIdMessageWithSourceBytesIterator<I, M>
  getVertexIdMessageWithSourceBytesIterator() {
    if (!useMessageSizeEncoding) {
      return null;
    }
    return new ByteStructVertexIdMessageWithSourceBytesIterator<I, M>(this) {
      @Override
      public void writeCurrentMessageBytes(DataOutput dataOutput) {
        try {
          dataOutput.write(extendedDataOutput.getByteArray(),
            messageOffset, messageBytes);
        } catch (NegativeArraySizeException e) {
          VerboseByteStructMessageWrite.handleNegativeArraySize(vertexId);
        } catch (IOException e) {
          throw new IllegalStateException("writeCurrentMessageBytes: Got " +
              "IOException", e);
        }
      }
    };
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeBoolean(useMessageSizeEncoding);
    super.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    useMessageSizeEncoding = dataInput.readBoolean();
    super.readFields(dataInput);
  }
}
