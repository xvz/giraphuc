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
  /** Whether or not to read most recently available local values **/
  private boolean doLocalRead;
  /** Whether or not this is a new computation phase **/
  private boolean isNewPhase;
  /** Number of vertices to delay by, when caching sent messages **/
  private int numVertexDelay;

  /**
   * Default constructor.
   */
  public AsyncConfiguration() {
    doLocalRead = false;
    isNewPhase = false;
    numVertexDelay = 10;
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
   * Returns whether or not the current superstep is a new
   * computation phase, relative to the previous superstep.
   *
   * @return True if this superstep is a new phase
   */
  public boolean isNewPhase() {
    return isNewPhase;
  }

  /**
   * Setter for doLocalRead.
   *
   * @param doLocalRead True to read most recently available local values
   */
  protected void setLocalRead(boolean doLocalRead) {
    this.doLocalRead = doLocalRead;
  }

  /**
   * Setter for isNewPhase.
   *
   * NOTE: Set by GraphTaskManager.setup() and execute().
   *
   * @param isNewPhase True if this superstep is a new computation phase
   */
  public void setNewPhase(boolean isNewPhase) {
    this.isNewPhase = isNewPhase;
  }
}
