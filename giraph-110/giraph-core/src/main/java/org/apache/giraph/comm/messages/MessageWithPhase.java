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
  /** Computation phase of this message. */
  private int phase;

  /**
   * Constructor that sets the computation phase of this message.
   *
   * @param phase Computation phase of this messag.
   */
  public MessageWithPhase(int phase) {
    this.phase = phase;
  }

  ///**
  // * Sets the computation phase of this message.
  // *
  // * @param phase Computation phase of this message.
  // */
  //public final void setPhase(int phase) {
  //  this.phase = phase;
  //}

  /**
   * Returns the computation phase of this message.
   *
   * @return Computation phase of this message.
   */
  public final int getPhase() {
    return phase;
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
    return "phase=" + phase;
  }
}
