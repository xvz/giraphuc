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

package org.apache.giraph.comm.messages;

import org.apache.giraph.factories.MessageValueFactory;
import org.apache.giraph.utils.ExtendedDataInput;
import org.apache.giraph.utils.Factory;
import org.apache.giraph.utils.ByteStructIterable;
import org.apache.giraph.utils.ByteStructIterator;
import org.apache.giraph.utils.io.DataInputOutput;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Iterator;
import java.io.IOException;

/**
 * YH: Special iterable that recycles messages with phases.
 * Messages not for the current phase are placed back on to the store.
 *
 * @param <I> Vertex id
 * @param <M> Message data
 */
public class MessagesWithPhaseIterable<I extends WritableComparable,
    M extends Writable> extends ByteStructIterable<M> {
  /** Message data */
  private final DataInputOutput dataInputOutput;
  /** Message class */
  private final MessageValueFactory<M> messageValueFactory;
  /** Message store */
  private final MessageStore<I, M> msgStore;
  /** Vertex id that messages are for */
  private final I dstVertexId;
  /** Partiton id that vertex belongs to */
  private final int partitionId;
  /** Current value of phase counter */
  private final int currentPhase;

  /** True if compute() iterated through all messages */
  private boolean doneAllMsgs;

  /**
   * Constructor
   *
   * @param msgStore Message store creating this iterable
   * @param dstVertexId Id of vertex that these messages are for
   * @param partitionId Id of partition that vertex belongs to
   * @param currentPhase Current value of phase counter
   * @param dataInputOutput Message data/factory for data inputs
   * @param messageValueFactory factory for creating message values
   */
  public MessagesWithPhaseIterable(
      MessageStore msgStore, I dstVertexId,
      int partitionId, int currentPhase,
      DataInputOutput dataInputOutput,
      MessageValueFactory<M> messageValueFactory) {
    super(dataInputOutput);
    this.dataInputOutput = dataInputOutput;
    this.messageValueFactory = messageValueFactory;

    this.msgStore = msgStore;
    this.dstVertexId = dstVertexId;
    this.partitionId = partitionId;
    this.currentPhase = currentPhase;

    this.doneAllMsgs = false;
  }

  @Override
  protected M createWritable() {
    return messageValueFactory.newInstance();
  }

  @Override
  public Iterator<M> iterator() {
    return new MessagesWithPhaseIterator(
        ((Factory<? extends ExtendedDataInput>) dataInputOutput).create());
  }

  /**
   * Adds messages, which were not meant to be processed in this phase,
   * back on to the message store.
   */
  public void restore() {
    // if compute() didn't look at messages, restore everything
    // back on to message store
    if (!doneAllMsgs) {
      try {
        msgStore.restore(partitionId, dstVertexId, dataInputOutput);
      } catch (IOException e) {
        throw new IllegalStateException("restore: got IOException", e);
      }

      doneAllMsgs = true;
    }
  }

  /**
   * Iterator that returns messages, as objects, whose lifetimes are
   * only until next() is called.
   *
   * This is similar to RepresentativeByteStructIterator, but we add the
   * the functionality of putting messages back on to the store.
   */
  private class MessagesWithPhaseIterator extends ByteStructIterator<M> {
    /** Representative writable */
    private final M representativeWritable = createWritable();
    /** True if representativeWritable should be returned on next() */
    private boolean alreadyFound;

    /**
     * Wrap ExtendedDataInput in ByteArrayIterator
     *
     * @param extendedDataInput ExtendedDataInput
     */
    public MessagesWithPhaseIterator(ExtendedDataInput extendedDataInput) {
      super(extendedDataInput);
      alreadyFound = false;
    }

    @Override
    public boolean hasNext() {
      return super.hasNext();
      /**
      // this ensures continuous calls to hasNext() are idempotent
      if (alreadyFound) {
        return true;
      }

      // iterate through messages to check if there are valid ones
      // that we can actually return
      while (super.hasNext()) {
        try {
          representativeWritable.readFields(extendedDataInput);
        } catch (IOException e) {
          throw new IllegalStateException(
              "hasNext: readFields got IOException", e);
        }

        MessageWithPhase msg = (MessageWithPhase) representativeWritable;

        // 4 scenarios (denote current phase as "k"):
        //
        // 1. message sent in phase k-1 AND to be processed in same phase
        //    -> DROP; BSP would have dropped this message
        // 2. message sent in phase k-1 AND to be processed in another phase
        //    -> SHOW; this message is for phase k
        // 3. message sent in phase k AND to be processed in same phase
        //    -> SHOW; this message is for phase k
        // 4. message sent in phase k AND to be processed in another phase
        //    -> SAVE; this message is for phase k+1
        //
        // Must also handle wrap around.
        if ((msg.getPhase() == (currentPhase - 1)) ||
            (msg.getPhase() == Integer.MAX_VALUE && currentPhase == 0)) {
          if (msg.processInSamePhase()) {
            continue;     // drop message
          } else {
            alreadyFound = true;
            return true;
          }
        } else if (msg.getPhase() == currentPhase) {
          if (msg.processInSamePhase()) {
            alreadyFound = true;
            return true;
          } else {
            try {
              // this is safe b/c message will be re-serialized,
              // allowing us to continue using representativeWritable
              msgStore.addPartitionMessage(partitionId, dstVertexId,
                                           representativeWritable);
            } catch (IOException e) {
              throw new RuntimeException("hasNext: Got IOException ", e);
            }
          }
        } else {
          // Due to restoring all messages when compute() doesn't look at them,
          // there can be messages from several phases ago. These messages must
          // be dropped, as they would have also been dropped in BSP.
          continue;
        }
      }

      // compute() will always iterate through ALL messages
      // (see ComputeCallable for how we ensure only compute() calls hasNext())
      MessagesWithPhaseIterable.this.doneAllMsgs = true;
      return false;
      **/
    }

    @Override
    public M next() {
      return null;
      /**
      // result is set from hasNext()'s search
      alreadyFound = false;
      return representativeWritable;
      **/
    }

    @Override
    protected M createWritable() {
      return MessagesWithPhaseIterable.this.createWritable();
    }
  }
}
