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

package org.apache.giraph.utils;

import org.apache.giraph.conf.ImmutableClassesGiraphConfigurable;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static org.apache.giraph.utils.ByteUtils.SIZE_OF_BYTE;
import static org.apache.giraph.utils.ByteUtils.SIZE_OF_INT;

/**
 * YH: Stores pairs of int and int data in a single byte array.
 *
 * Similar to, but distinct from, VertexIdData classes as the
 * first element is not a vertex id and all data are primitives.
 */
public class ByteArrayIntInt
  implements ImmutableClassesGiraphConfigurable, Writable {
  /** Extended data output */
  protected ExtendedDataOutput extendedDataOutput;
  /** Configuration */
  private ImmutableClassesGiraphConfiguration configuration;

  /**
   * Initialize the inner state. Must be called before {@code add()} is
   * called.
   */
  public void initialize() {
    extendedDataOutput = getConf().createExtendedDataOutput();
  }

  /**
   * Initialize the inner state, with a known size. Must be called before
   * {@code add()} is called.
   *
   * @param expectedSize Number of bytes to be expected
   */
  public void initialize(int expectedSize) {
    extendedDataOutput = getConf().createExtendedDataOutput(expectedSize);
  }

  /**
   * Add an int, int pair to the collection.
   *
   * @param val1 First int
   * @param val2 Second int
   */
  public void add(int val1, int val2) {
    try {
      extendedDataOutput.writeInt(val1);
      extendedDataOutput.writeInt(val2);
    } catch (IOException e) {
      throw new IllegalStateException("add: IOException", e);
    }
  }

  /**
   * Get the number of bytes used.
   *
   * @return Bytes used
   */
  public int getSize() {
    return extendedDataOutput.getPos();
  }

  /**
   * Get the size of this object in serialized form.
   *
   * @return The size (in bytes) of the serialized object
   */
  public int getSerializedSize() {
    return SIZE_OF_BYTE + SIZE_OF_INT + getSize();
  }

  /**
   * Check if the list is empty.
   *
   * @return Whether the list is empty
   */
  public boolean isEmpty() {
    return extendedDataOutput.getPos() == 0;
  }

  /**
   * Clear the list.
   */
  public void clear() {
    extendedDataOutput.reset();
  }

  /**
   * Returns an iterator over the int, int pairs.
   *
   * @return Iterator
   */
  public ByteArrayIntIntIterator getIterator() {
    return new ByteArrayIntIntIterator(this);
  }

  @Override
  public void setConf(ImmutableClassesGiraphConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public ImmutableClassesGiraphConfiguration getConf() {
    return configuration;
  }

  /**
   * Get the underlying byte-array.
   *
   * @return The underlying byte-array
   */
  public byte[] getByteArray() {
    return extendedDataOutput.getByteArray();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    WritableUtils.writeExtendedDataOutput(extendedDataOutput, dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    extendedDataOutput =
        WritableUtils.readExtendedDataOutput(dataInput, getConf());
  }

  /**
   * Special iterator for ByteArrayIntInt that does not
   * allocate any objects (returns primitives).
   */
  public static class ByteArrayIntIntIterator {
    /** Serialized input data to read from */
    protected final ExtendedDataInput extendedDataInput;

    /** Current first value */
    private int val1;
    /** Current second value */
    private int val2;

    /**
     * Constructor.
     *
     * @param inputData ByteArrayIntInt data
     */
    public ByteArrayIntIntIterator(ByteArrayIntInt inputData) {
      extendedDataInput = inputData.getConf().
        createExtendedDataInput(inputData.extendedDataOutput);
    }

    /**
     * Returns true if the iteration has more elements.
     *
     * @return True if the iteration has more elements.
     */
    public boolean hasNext() {
      return extendedDataInput.available() > 0;
    }

    /**
     * Moves to the next element in the iteration.
     */
    public void next() {
      try {
        val1 = extendedDataInput.readInt();
        val2 = extendedDataInput.readInt();
      } catch (IOException e) {
        throw new IllegalStateException("next: IOException", e);
      }
    }

    public int getFirst() {
      return val1;
    }

    public int getSecond() {
      return val2;
    }
  }
}
