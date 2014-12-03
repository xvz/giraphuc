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
 * Interface for messages of multi-phase algorithms, used to track
 * whether or not a message should be processed in the same phase
 * it was sent. Must be overridden by users for algorithms sending
 * different messages for different phases.
 */
public interface MessageWithPhase extends Writable {
  /**
   * Return whether or not this message is to be processed in the
   * phase following the one in which it was sent.
   *
   * @return True if the message is to be processed in the next phase
   */
  boolean forNextPhase();

  // YH: we don't need to send this boolean---it's used internally
  // by the system to place the message on the correct store.
  // Hence, user's message class must implement readFields() and write().
}
