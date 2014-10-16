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

package org.apache.giraph.conf;

import org.apache.giraph.comm.messages.MessageWithPhase;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

/**
 * YH: Tracks configuration specific to async mode.
 */
public class AsyncConfiguration {
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(AsyncConfiguration.class);

  /** Whether or not to read most recently available local values */
  private boolean doLocalRead;
  /** Whether or not to read most recently available remote values */
  private boolean doRemoteRead;
  /**
   * Whether algorithm (or phase) needs every vertex to have all messages
   * from all its neighbours for every superstep (aka, "stationary")
   */
  private boolean needAllMsgs;

  // TODO-YH: phases w/ PR-like execution are not fully completed yet
  /** Is the next superstep a new computation phase? */
  private boolean isNewPhase;
  /** Counter for current phase; wraps around under overflow */
  private int currentPhase;
  /** Does the computation have multiple phases? */
  private boolean isMultiPhase;

  /** Whether or not to disable BSP barriers for async execution */
  private boolean disableBarriers;
  /** Is a global barrier needed? */
  private boolean needBarrier;
  /** Local in-flight message bytes */
  private AtomicLong inFlightBytes;

  // YH: inFlightBytes tracks the number of bytes this worker has sent
  // to remote workers MINUS the bytes this worker has received from
  // remote workers. By itself, this value is meaningless. However, when
  // summed across ALL workers, the master can learn if any bytes are
  // still in-flight (=> whether to terminate or not).

  /**
   * Initialization constructor.
   *
   * @param conf GiraphConfiguration
   */
  public AsyncConfiguration(ImmutableClassesGiraphConfiguration conf) {
    disableBarriers = GiraphConstants.ASYNC_DISABLE_BARRIERS.get(conf);

    if (disableBarriers) {
      doLocalRead = true;
      doRemoteRead = true;
    } else {
      doLocalRead = GiraphConstants.ASYNC_LOCAL_READ.get(conf);
      doRemoteRead = GiraphConstants.ASYNC_REMOTE_READ.get(conf);
    }

    if (doLocalRead || doRemoteRead) {
      needAllMsgs = GiraphConstants.ASYNC_NEED_ALL_MSGS.get(conf);
    } else {
      needAllMsgs = false;
    }

    // This only sets isNewPhase for SS -1 (INPUT_SUPERSTEP).
    isNewPhase = true;
    // All computations have at least one phase.
    currentPhase = 0;

    // if M implements MessageWithPhase, we have multiphase computation
    // NOTE: we assume incoming and outgoing types are same
    //
    // (doing it here exactly once probably gives better performance;
    // reflection can be expensive)
    isMultiPhase = MessageWithPhase.class.
      isAssignableFrom(conf.getIncomingMessageValueClass());

    // special case: first superstep always needs barrier after
    needBarrier = true;
    inFlightBytes = new AtomicLong();
  }

  /**
   * Return whether or not to read most recently available local values.
   *
   * @return True if reading most recent local values
   */
  public boolean doLocalRead() {
    return doLocalRead;
  }

  /**
   * Return whether or not to read most recently available remote values.
   *
   * @return True if reading most recent remote values
   */
  public boolean doRemoteRead() {
    return doRemoteRead;
  }

  /**
   * Return whether or not BSP barriers should be disabled.
   *
   * @return True if BSP barriers should be disabled
   */
  public boolean disableBarriers() {
    return disableBarriers;
  }

  /**
   * Return whether or not vertices need messages from all neigbours.
   *
   * @return Whether every vertex needs messages from all its neighbours.
   */
  public boolean needAllMsgs() {
    return needAllMsgs;
  }


  /**
   * Return whether or not the current superstep is a new
   * computation phase, relative to the previous superstep.
   *
   * @return True if this superstep is a new phase
   */
  public boolean isNewPhase() {
    return isNewPhase;
  }

  /**
   * Set whether or not the next global superstep is a new phase.
   *
   * @param isNewPhase True if next global superstep is a new phase.
   */
  public void setNewPhase(boolean isNewPhase) {
    this.isNewPhase = isNewPhase;
    if (isNewPhase) {
      // in case of overflow, just wrap around (also, shut up checkstyle)
      // CHECKSTYLE: stop UnnecessaryParenthesesCheck
      currentPhase = (currentPhase + 1 == Integer.MIN_VALUE ?
                      0 : currentPhase + 1);
      // CHECKSTYLE: resume UnnecessaryParenthesesCheck
    }
  }

  /**
   * Get the phase counter for the current global superstep.
   * This phase counter increases monotonically, but wraps under overflow.
   *
   * Note that this does NOT relate at all to the actual computation phase
   * used by the algorithm's logic---this is only a system counter.
   *
   * @return Current phase
   */
  public int getCurrentPhase() {
    return currentPhase;
  }

  /**
   * Return whether or not the computation has multiple phases.
   *
   * @return True if computation has multiple phases
   */
  public boolean isMultiPhase() {
    return isMultiPhase;
  }


  /**
   * Return whether or not a global barrier is needed between
   * the current and the next supersteps.
   *
   * @return True if a global barrier is needed
   */
  public boolean needBarrier() {
    return needBarrier;
  }

  /**
   * Set whether or not a global barrier is needed between
   * the current and the next supersteps.
   *
   * @param needBarrier True if a global barrier is needed
   */
  public void setNeedBarrier(boolean needBarrier) {
    this.needBarrier = needBarrier;
  }


  /**
   * Return the local number of in-flight message bytes for
   * the previous global superstep.
   *
   * This value is meaningful ONLY if summed up with values from
   * ALL workers. This value by itself has NO meaning!!
   *
   * @return Local in-flight bytes
   */
  public long getInFlightBytes() {
    return inFlightBytes.get();
  }

  /**
   * Reset the local in-flight message bytes
   */
  public void resetInFlightBytes() {
    inFlightBytes.set(0);
  }

  /**
   * Add the number of message bytes received. Only used when
   * running with asynchronous execution and barriers disabled.
   *
   * This is cumulative over multiple logical supersteps,
   * but for exactly a single global superstep.
   *
   * @param recvBytes Received message bytes
   */
  public void addRecvBytes(long recvBytes) {
    // note that this is subtracting
    inFlightBytes.addAndGet(-recvBytes);
  }

  /**
   * Add the number of message bytes sent. Only used when
   * running with asynchronous execution and barriers disabled.
   *
   * This is cumulative over multiple logical supersteps,
   * but for exactly a single global superstep.
   *
   * @param sentBytes Sent message bytes
   */
  public void addSentBytes(long sentBytes) {
    inFlightBytes.addAndGet(sentBytes);
  }
}
