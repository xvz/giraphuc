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

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.Factory;
import org.apache.giraph.utils.RepresentativeByteStructIterable;
import org.apache.giraph.utils.RepresentativeByteStructIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;
import java.io.IOException;

/**
 * YH: Special iterable that recycles the source vertex id and message.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class MessageWithSourceIterable<
    I extends WritableComparable, M extends Writable>
    extends RepresentativeByteStructIterable<MessageWithSource<I, M>> {
  /** Source vertex id */
  private final I srcId;
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor
   *
   * @param srcId Source vertex id of the message
   * @param dataInputFactory Factory for data inputs
   * @param messageValueFactory factory for creating message values
   */
  public MessageWithSourceIterable(
      I srcId,
      Factory<? extends ExtendedDataInput> dataInputFactory,
      MessageValueFactory<M> messageValueFactory) {
    super(dataInputFactory);
    this.srcId = srcId;
    this.messageValueFactory = messageValueFactory;
  }

  @Override
  protected MessageWithSource<I, M> createWritable() {
    return new MessageWithSource(srcId, messageValueFactory.newInstance());
  }

  @Override
  public Iterator<MessageWithSource<I, M>> iterator() {
    return new RepresentativeByteStructIterator<MessageWithSource<I, M>>(
        dataInputFactory.create()) {
      /** Representative writable */
      private final MessageWithSource<I, M> representativeWritable =
        createWritable();

      @Override
      public MessageWithSource<I, M> next() {
        try {
          // YH: handles case where user (erroneously) calls removeMessage()
          if (representativeWritable.getMessage() == null) {
            representativeWritable.set(MessageWithSourceIterable.this.srcId,
                                       MessageWithSourceIterable.this.
                                       messageValueFactory.newInstance());
          }
          representativeWritable.getMessage().readFields(extendedDataInput);
        } catch (IOException e) {
          throw new IllegalStateException(
              "next: readFields got IOException", e);
        }
        return representativeWritable;
      }

      @Override
      protected MessageWithSource<I, M> createWritable() {
        return MessageWithSourceIterable.this.createWritable();
      }
    };
  }
}
