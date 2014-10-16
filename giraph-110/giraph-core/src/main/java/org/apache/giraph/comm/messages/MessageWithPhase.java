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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 * Abstract class for messages, which tracks computation phases.
 * Can be overridden by users for algorithms that have different
 * messages for different phases.
 */
public abstract class MessageWithPhase implements Writable  {
  /** Mask to get boolean encoded in int */
  private static final int BOOL_MASK = 1 << 31;
  /**
   * Computation phase that this message was sent in.
   * Most significant bit indicates whether or not this message
   * should be processed in the same phase as it was sent.
   */
  private int phase;

  /**
   * Constructor that sets the computation phase of this message.
   * NOTE: setPhase() must be called to correctly set this msg's phase.
   *
   * @param forCurrPhase True if message should be processed in this phase.
   */
  public MessageWithPhase(boolean forCurrPhase) {
    phase = 0;      // dummy value, must be set via setPhase()
    if (forCurrPhase) {
      this.phase |= BOOL_MASK;
    }
  }

  /**
   * Set computation phase that this message was sent in.
   *
   * @param phase Computation phase for this message
   */
  public final void setPhase(int phase) {
    if (phase < 0) {
      throw new RuntimeException("Computation phases cannot be negative!");
    }
    this.phase &= BOOL_MASK;   // clear out previous phase
    this.phase |= phase;       // set to new phase
  }

  /**
   * @return Computation phase that this message was sent in.
   */
  public final int getPhase() {
    return phase & ~BOOL_MASK;
  }

  /**
   * Return whether this message is to be processed
   * in the same phase it was sent.
   *
   * @return Whether to process message in the phase it was sent
   */
  public final boolean processInSamePhase() {
    return (phase & BOOL_MASK) >>> 31 == 1;
  }

  /**
   * Deserialize the fields of this message from in.
   *
   * @param in DataInput to deserialize this message from.
   */
  protected abstract void readFieldsMsg(DataInput in) throws IOException;

  /**
   * Serialize the fields of this message to out.
   *
   * @param out DataOutput to serialize this message to.
   */
  protected abstract void writeMsg(DataOutput out) throws IOException;


  // YH: we do this to ensure phase is always read/written
  @Override
  public final void readFields(DataInput in) throws IOException {
    phase = in.readInt();
    readFieldsMsg(in);
  }

  @Override
  public final void write(DataOutput out) throws IOException {
    out.writeInt(phase);
    writeMsg(out);
  }

  @Override
  public String toString() {
    return "phase=" + getPhase() + ", processInSamePhase=" +
      processInSamePhase();
  }
}
