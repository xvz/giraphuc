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

/**
 * YH: Tracks configuration specific to async mode.
 */
public class AsyncConfiguration {
  /** Whether or not to read most recently available local values */
  private boolean doLocalRead;
  /** Whether or not to read most recently available remote values */
  private boolean doRemoteRead;
  /** Whether or not to disable BSP barriers for async execution */
  private boolean disableBarriers;
  /** Maximum number of messages before flushing cached messages */
  private int maxNumMsgs;
  /**
   * Whether algorithm (or phase) needs every vertex to have all messages
   * from all its neighbours for every superstep (aka, "stationary")
   */
  private boolean needAllMsgs;

  /** Is the next superstep a new computation phase? */
  // TODO-YH: phases are not completed yet
  private boolean isNewPhase;

  /** Is a global barrier needed? */
  private boolean needBarrier;

  /**
   * Initialization constructor.
   *
   * @param conf GiraphConfiguration
   */
  public AsyncConfiguration(GiraphConfiguration conf) {
    doLocalRead = GiraphConfiguration.ASYNC_LOCAL_READ.get(conf);
    doRemoteRead = GiraphConfiguration.ASYNC_REMOTE_READ.get(conf);
    disableBarriers = GiraphConfiguration.ASYNC_DISABLE_BARRIERS.get(conf);
    maxNumMsgs = GiraphConfiguration.ASYNC_MAX_NUM_MSGS.get(conf);
    needAllMsgs = GiraphConfiguration.ASYNC_NEED_ALL_MSGS.get(conf);

    // special case: first superstep is always new "phase"
    isNewPhase = true;
    // special case: first superstep always needs barrier after
    needBarrier = true;
  }

  /**
   * Returns whether or not to read most recently available local values.
   *
   * @return True if reading most recent local values
   */
  public boolean doLocalRead() {
    return doLocalRead;
  }

  /**
   * Returns whether or not to read most recently available remote values.
   *
   * @return True if reading most recent remote values
   */
  public boolean doRemoteRead() {
    return doRemoteRead;
  }

  /**
   * Returns whether or not BSP barriers should be disabled.
   *
   * @return True if BSP barriers should be disabled
   */
  public boolean disableBarriers() {
    return disableBarriers;
  }

  /**
   * @return Number of messages before flushing.
   */
  public int maxNumMsgs() {
    return maxNumMsgs;
  }

  /**
   * @return Whether every vertex needs messages from all its neighbours.
   */
  public boolean needAllMsgs() {
    return needAllMsgs;
  }


  /**
   * Returns whether or not the current superstep is a new
   * computation phase, relative to the previous superstep.
   *
   * @return True if this superstep is a new phase
   */
  public boolean isNewPhase() {
    return isNewPhase;
  }

  /**
   * Sets whether current superstep is new phase.
   *
   * NOTE: Set by GraphTaskManager.setup() and execute().
   *
   * @param isNewPhase True if this superstep is a new computation phase
   */
  public void setNewPhase(boolean isNewPhase) {
    this.isNewPhase = isNewPhase;
  }


  /**
   * Returns whether or not a global barrier is needed between
   * the current and the next supersteps.
   *
   * @return True if a global barrier is needed
   */
  public boolean needBarrier() {
    return needBarrier;
  }

  /**
   * Sets whether or not a global barrier is needed between
   * the current and the next supersteps.
   *
   * @param needBarrier True if a global barrier is needed
   */
  public void setNeedBarrier(boolean needBarrier) {
    this.needBarrier = needBarrier;
  }
}
