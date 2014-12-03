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

/**
 * Static utils class for messages with phases. Functions here
 * can be used to encode/decode a boolean into an int.
 */
public class MessageWithPhaseUtils {
  /** Mask to encode boolean "for next phase = true" into an int */
  private static final int NEXT_PHASE_MASK = 1 << 31;

  /** Do not instantiate. */
  private MessageWithPhaseUtils() {
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

    return forNextPhase ? val | NEXT_PHASE_MASK : val;
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
}
