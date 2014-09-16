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
import org.apache.hadoop.io.Writable;

import java.util.Iterator;
import java.io.IOException;

/**
 * YH: Special iterable that recycles the message.
 *
 * @param <M> Message data
 */
public class MessagesIterable<M extends Writable>
    implements Iterable<M> {
  /** Iterator over message store's messages */
  private final Iterator<? extends Factory<? extends ExtendedDataInput>> itr;
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;

  /**
   * Constructor
   *
   * @param itr Iterator over collection of data input factories
   * @param messageValueFactory factory for creating message values
   */
  public MessagesIterable(
      Iterator<? extends Factory<? extends ExtendedDataInput>> itr,
      MessageValueFactory<M> messageValueFactory) {
    this.itr = itr;
    this.messageValueFactory = messageValueFactory;
  }

  @Override
  public Iterator<M> iterator() {
    return new MessagesIterator();
  }

  /**
   * Iterator that returns messages, as objects, whose lifetimes are
   * only until next() is called.
   */
  private class MessagesIterator implements Iterator<M> {
    /** Representative writable */
    private final M representativeWritable =
      messageValueFactory.newInstance();

    @Override
    public boolean hasNext() {
      return itr.hasNext();
    }

    @Override
    public M next() {
      try {
        Factory<? extends ExtendedDataInput> factory = itr.next();
        // YH: possible race condition (message store is concurrently modified)
        synchronized (factory) {
          representativeWritable.readFields(factory.create());
        }
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
