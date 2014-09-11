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

/**
 * YH: Special iterator that reuses source vertex ids so that the lifetime
 * of the object is only until next() is called.
 *
 * @param <I> Vertex id
 */
public interface SourceVertexIdIterator<I extends WritableComparable> {
  /**
   * Get the current source vertex id.  Ihis object's contents are only
   * guaranteed until next() is called.  To take ownership of this object
   * call releaseCurrentSourceVertexId() after getting a reference to
   * this object.
   *
   * @return Current source vertex id
   */
  I getCurrentSourceVertexId();

  /**
   * The backing store of the current source vertex id is now released.
   * Further calls to getCurrentSourceVertexId() without calling next()
   * will return null.
   *
   * @return Current source vertex id that was released
   */
  I releaseCurrentSourceVertexId();
}
