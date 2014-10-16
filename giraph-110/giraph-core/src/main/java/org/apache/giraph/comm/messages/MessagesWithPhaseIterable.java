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

  /** True if all data was iterated through at least once */
  private boolean allChecked;
  /** True if there are messages that must be saved/restored */
  private boolean needToRestore;

  /**
   * Constructor
   *
   * @param msgStore Message store creating this iterable
   * @param dstVertexId Id of vertex that these messages are for
   * @param partitionId Id of partition that vertex belongs to
   * @param currentPhase Current value of phase counter
   * @param dataInputFactory Factory for data inputs
   * @param messageValueFactory factory for creating message values
   */
  public MessagesWithPhaseIterable(
      MessageStore msgStore, I dstVertexId,
      int partitionId, int currentPhase,
      Factory<? extends ExtendedDataInput> dataInputFactory,
      MessageValueFactory<M> messageValueFactory) {
    super(dataInputFactory);
    this.messageValueFactory = messageValueFactory;

    this.msgStore = msgStore;
    this.dstVertexId = dstVertexId;
    this.partitionId = partitionId;
    this.currentPhase = currentPhase;

    this.allChecked = false;
    this.needToRestore = false;
  }

  @Override
  protected M createWritable() {
    return messageValueFactory.newInstance();
  }

  @Override
  public Iterator<M> iterator() {
    return new MessagesWithPhaseIterator(dataInputFactory.create());
  }

  /**
   * Adds messages, which were not meant to be processed in this phase,
   * back on to the message store.
   */
  public void restore() {
    // YH: we have to do it this way because iterator() can and will
    // be called multiple times, meaning we would add the same messages
    // back on to the message store MULTIPLE times if we did restore
    // in the iterator()'s hasNext(). Doing it this way ensures we only
    // restore exactly once.

    // TODO-YH: can we make restores more efficient? E.g., put everything
    // back on to message store instead of iterating through it all again?

    // If we have at least one msg to restore, restore it. If not,
    // AND we haven't gone through all messages, double check now.
    if (needToRestore || !allChecked) {
      RestoreMessagesIterator itr =
        new RestoreMessagesIterator(dataInputFactory.create());
      itr.hasNext();   // this will do restore

      allChecked = true;
      needToRestore = false;
    }
  }

  /**
   * Iterator that returns messages, as objects, whose lifetimes are
   * only until next() is called.
   *
   * This is similar to RepresentativeByteStructIterator, but we add the
   * functionality of skipping messages not intended for current phase.
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

        // messages should always be from currentPhase or currentPhase-1
        // exception is if phase is 0 (due to wrap around)
        if (msg.getPhase() != 0 && msg.getPhase() < currentPhase - 1) {
          throw new RuntimeException(
              "hasNext: got message that was sent more than 1 phase ago! " +
              representativeWritable);
        }

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
        if (msg.getPhase() != currentPhase) {
          if (msg.processInSamePhase()) {
            continue;     // drop message
          } else {
            alreadyFound = true;
            return true;
          }
        } else {
          if (msg.processInSamePhase()) {
            alreadyFound = true;
            return true;
          } else {
            // for next phase, so ignore (this will be put back on
            // message store later)
            MessagesWithPhaseIterable.this.needToRestore = true;
            continue;
          }
        }
      }

      MessagesWithPhaseIterable.this.allChecked = true;
      return false;
    }

    @Override
    public M next() {
      // result is set from hasNext()'s search
      alreadyFound = false;
      return representativeWritable;
    }

    @Override
    protected M createWritable() {
      return MessagesWithPhaseIterable.this.createWritable();
    }
  }

  /**
   * Iterator that restores unprocessed messages back on to message store.
   */
  private class RestoreMessagesIterator extends ByteStructIterator<M> {
    /** Representative writable */
    private final M representativeWritable = createWritable();

    /**
     * Wrap ExtendedDataInput in ByteArrayIterator
     *
     * @param extendedDataInput ExtendedDataInput
     */
    public RestoreMessagesIterator(ExtendedDataInput extendedDataInput) {
      super(extendedDataInput);
    }

    @Override
    public boolean hasNext() {
      // YH: this function is hacked to allow us to iterate over
      // all messages and restore ones that were not processed
      while (super.hasNext()) {
        try {
          representativeWritable.readFields(extendedDataInput);

          MessageWithPhase msg = (MessageWithPhase) representativeWritable;
          // only care about scenario 4:
          // 4. message sent in phase k AND to be processed in another phase
          //    -> SAVE; this message is for phase k+1
          if (msg.getPhase() == currentPhase && !msg.processInSamePhase()) {
            // this is safe b/c message will be re-serialized,
            // allowing us to continue using representativeWritable
            msgStore.addPartitionMessage(partitionId, dstVertexId,
                                         representativeWritable);
          }
        } catch (IOException e) {
          throw new RuntimeException("hasNext: Got IOException ", e);
        }
      }
      return false;
    }

    @Override
    public M next() {
      throw new UnsupportedOperationException("next: Not supported");
    }

    @Override
    protected M createWritable() {
      return MessagesWithPhaseIterable.this.createWritable();
    }
  }
}
