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
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;
import java.util.Map;
import java.io.IOException;

/**
 * YH: Special iterable that recycles the source vertex id and message.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class MessagesWithSourceIterable<I extends WritableComparable,
    M extends Writable> implements Iterable<MessageWithSource<I, M>> {

  /** Iterator over source map entries */
  private final Iterator<Map.Entry<I, DataInputOutput>> itr;
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor
   *
   * @param itr Iterator over collection of source map entries
   * @param messageValueFactory factory for creating message values
   */
  public MessagesWithSourceIterable(
      Iterator<Map.Entry<I, DataInputOutput>> itr,
      MessageValueFactory<M> messageValueFactory) {
    this.itr = itr;
    this.messageValueFactory = messageValueFactory;
  }

  @Override
  public Iterator<MessageWithSource<I, M>> iterator() {
    return new MessagesWithSourceIterator();
  }

  /**
   * Iterator that returns messages with sources as objects
   * whose lifetimes are only until next() is called.
   */
  private class MessagesWithSourceIterator
    implements Iterator<MessageWithSource<I, M>> {

    /** Representative writable */
    private final MessageWithSource<I, M> representativeWritable =
      new MessageWithSource(null, messageValueFactory.newInstance());

    @Override
    public boolean hasNext() {
      return itr.hasNext();
    }

    @Override
    public MessageWithSource<I, M> next() {
      try {
        Map.Entry<I, DataInputOutput> entry = itr.next();
        M msg;

        // YH: in case user (erroneously) calls removeMessage()
        if (representativeWritable.getMessage() == null) {
          msg = messageValueFactory.newInstance();
        } else {
          msg = representativeWritable.getMessage();
        }

        DataInputOutput dio = entry.getValue();
        // YH: possible race condition (message store is concurrently modified)
        synchronized (dio) {
          msg.readFields((ExtendedDataInput) dio.create());
        }
        representativeWritable.set(entry.getKey(), msg);
      } catch (IOException e) {
        throw new IllegalStateException("next: readFields got IOException", e);
      }
      return representativeWritable;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove: Not supported");
    }
  }
}
