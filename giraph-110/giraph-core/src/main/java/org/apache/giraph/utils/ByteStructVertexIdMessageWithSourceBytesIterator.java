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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * YH: Special iterator that reuses vertex ids and messages bytes so that the
 * lifetime of the object is only until next() is called. This can return
 * the source vertex id without deserializing the actual message.
 *
 * Vertex id ownership can be released if desired through
 * releaseCurrentVertexId().  This optimization allows us to cut down
 * on the number of objects instantiated and garbage collected.  Messages
 * can only be copied to an ExtendedDataOutput object
 *
 * @param <I> vertexId type parameter
 * @param <M> message type parameter
 */
@NotThreadSafe
public abstract class ByteStructVertexIdMessageWithSourceBytesIterator<
  I extends WritableComparable, M extends Writable>
  extends ByteStructVertexIdDataIterator<I, MessageWithSource<I, M>>
  implements VertexIdMessageWithSourceBytesIterator<I, M>,
             SourceVertexIdIterator<I> {
  /** Last message offset */
  protected int messageOffset = -1;
  /** Number of bytes in the last message */
  protected int messageBytes = -1;
  /** Source vertex id */
  protected I srcVertexId;

  /**
   * Constructor with vertexIdData
   *
   * @param vertexIdData vertexIdData
   */
  public ByteStructVertexIdMessageWithSourceBytesIterator(
    AbstractVertexIdData<I, MessageWithSource<I, M>> vertexIdData) {
    super(vertexIdData);
  }

  /**
   * Moves to the next element in the iteration.
   */
  @Override
  public void next() {
    if (vertexId == null) {
      vertexId = vertexIdData.getConf().createVertexId();
    }

    if (srcVertexId == null) {
      srcVertexId = vertexIdData.getConf().createVertexId();
    }

    try {
      vertexId.readFields(extendedDataInput);
      messageBytes = extendedDataInput.readInt();
      int tmpMessageOffset = extendedDataInput.getPos();
      srcVertexId.readFields(extendedDataInput);

      // YH: a slight hack to deal with the fact that actual message
      // itself doesn't have an explicit int indicating its size
      messageOffset = extendedDataInput.getPos();
      messageBytes = messageBytes - (messageOffset - tmpMessageOffset);

      if (extendedDataInput.skipBytes(messageBytes) != messageBytes) {
        throw new IllegalStateException("next: Failed to skip " +
            messageBytes);
      }
    } catch (IOException e) {
      throw new IllegalStateException("next: IOException", e);
    }
  }

  // TODO-YH: *currentData() functions in superclass are okay to leave as-is?
  // Seems like ByteStructVertexIdMessageBytesIterator just leaves them...

  @Override
  public I getCurrentSourceVertexId() {
    return srcVertexId;
  }

  @Override
  public I releaseCurrentSourceVertexId() {
    I releasedSrcVertexId = srcVertexId;
    srcVertexId = null;
    return releasedSrcVertexId;
  }
}
