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

package org.apache.giraph.comm.messages.with_source.primitives;

import org.apache.hadoop.io.DoubleWritable;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.Iterator;
import java.util.Map;

/**
 * YH: Special iterable that recycles a DoubleWritable message.
 */
public class DoubleMessagesIterable implements Iterable<DoubleWritable> {
  /** Source map from message store */
  private final Map<?, Double> srcMap;
  /** Iterator over message store's messages */
  private final DoubleIterator itr;

  /**
   * Constructor
   *
   * @param srcMap Source map from message store
   */
  public DoubleMessagesIterable(Long2DoubleOpenHashMap srcMap) {
    this.srcMap = srcMap;
    this.itr = srcMap.values().iterator();
  }

  @Override
  public Iterator<DoubleWritable> iterator() {
    return new Iterator<DoubleWritable>() {
      /** Representative writable */
      private final DoubleWritable representativeWritable =
        new DoubleWritable();

      @Override
      public boolean hasNext() {
        synchronized (srcMap) {
          return itr.hasNext();
        }
      }

      @Override
      public DoubleWritable next() {
        synchronized (srcMap) {
          representativeWritable.set(itr.nextDouble());
        }
        return representativeWritable;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("remove: Not supported");
      }
    };
  }
}
