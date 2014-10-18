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

import org.apache.hadoop.io.Writable;

/**
 * Abstract class for messages, which tracks whether or not
 * a message should be processed in the same phase it was sent.
 * Must be overridden by users for algorithms sending different
 * messages for different phases.
 */
public abstract class MessageWithPhase implements Writable {
  /** Mask to encode boolean "for next phase = true" into an int */
  private static final int NEXT_PHASE_MASK = 1 << 31;

  /**
   * True if message is to be processed in the next phase.
   */
  private boolean forNextPhase;

  /**
   * Constructor that sets whether this message is for the next phase.
   *
   * @param forNextPhase True if message is to be processed in next phase.
   */
  public MessageWithPhase(boolean forNextPhase) {
    this.forNextPhase = forNextPhase;
  }

  /**
   * Encode a positive integer with a boolean.
   *
   * @param val A positive integer
   * @param forNextPhase True if message is to be processed in next phase
   * @return Encoded int
   */
  public static int encode(int val, boolean forNextPhase) {
    if (val < 0) {
      throw new RuntimeException("encode: value cannot be negative!");
    }

    if (forNextPhase) {
      return val | NEXT_PHASE_MASK;
    }
    return val;
  }

  /**
   * Decode an integer containing a boolean.
   *
   * @param val An integer encoded with a boolean
   * @return Boolean encoded in the int
   */
  public static boolean forNextPhase(int val) {
    // works b/c we assume all values are original positive
    return val < 0;
  }

  /**
   * Remove any encoded boolean from an integer.
   *
   * @param val An integer encoded with a boolean
   * @return Clean unencoded int
   */
  public static int decode(int val) {
    return val < 0 ? val & ~NEXT_PHASE_MASK : val;
  }

  /**
   * Return whether or not this message is to be processed in the
   * phase following the one in which it was sent.
   *
   * @return True if the message is to be processed in the next phase
   */
  public final boolean forNextPhase() {
    return forNextPhase;
  }

  // YH: we don't need to send this boolean---it's used internally
  // by the system to place the message on the correct store.
  // Hence, user's message class must implement readFields() and write().
}
