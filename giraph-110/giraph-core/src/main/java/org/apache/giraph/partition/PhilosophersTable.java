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

package org.apache.giraph.partition;

import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.requests.SendDistributedLockingForkRequest;
import org.apache.giraph.comm.requests.SendDistributedLockingTokenRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * YH: Implements the hygienic dining philosophers solution.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class PhilosophersTable<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Mask for have-token bit */
  private static final byte MASK_HAVE_TOKEN = 0x1;
  /** Mask for have-fork bit */
  private static final byte MASK_HAVE_FORK = 0x2;
  /** Mask for is-dirty bit */
  private static final byte MASK_IS_DIRTY = 0x4;

  /** Provided configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  /** Lock for condition var */
  private final Lock cvLock = new ReentrantLock();
  /** Condition var indicating arrival of fork */
  private final Condition getFork = cvLock.newCondition();

  /**
   * Map of vertex id (philosopher) to vertex ids (vertex's neighbours)
   * to single byte (indicates status, fork, token, dirty/clean).
   *
   * Basically, this tracks the state of philosophers (boundary vertices),
   * who are sitting at a distributed table, that are local to this worker.
   */
  private Long2ObjectOpenHashMap<Long2ByteOpenHashMap> pMap;

  /**
   * Constructor
   *
   * @param conf Configuration used.
   * @param serviceWorker Service worker
   */
  public PhilosophersTable(
     ImmutableClassesGiraphConfiguration<I, V, E> conf,
     CentralizedServiceWorker<I, V, E> serviceWorker) {
    Class<I> vertexIdClass = conf.getVertexIdClass();
    if (!vertexIdClass.equals(LongWritable.class)) {
      // TODO-YH: implement general support (easy to do, but tedious)
      throw new RuntimeException(
        "Non-long vertex id classes not yet supported!");
    }

    this.conf = conf;
    this.serviceWorker = serviceWorker;
    this.pMap = new Long2ObjectOpenHashMap<Long2ByteOpenHashMap>();
  }

  /**
   * Add and initialize a boundary vertex (philosopher).
   * Must not be called/used when compute threads are executing.
   *
   * @param vertex Vertex to be added
   */
  public void addBoundaryVertex(Vertex<I, V, E> vertex) {
    // TODO-YH: assumes undirected graph
    Long2ByteOpenHashMap neighbours =
      new Long2ByteOpenHashMap(vertex.getNumEdges());
    long pId = ((LongWritable) vertex.getId()).get();

    for (Edge<I, E> e : vertex.getEdges()) {
      long neighbourId = ((LongWritable) e.getTargetVertexId()).get();
      byte forkInfo = 0;

      // for acyclic precedence graph, always initialize
      // tokens at smaller id and dirty fork at larger id
      if (neighbourId < pId) {
        forkInfo |= MASK_HAVE_TOKEN;
      } else {
        forkInfo |= MASK_HAVE_FORK;
        forkInfo |= MASK_IS_DIRTY;
      }

      neighbours.put(neighbourId, forkInfo);
    }

    synchronized (pMap) {
      Long2ByteOpenHashMap ret = pMap.put(pId, neighbours);
      if (ret != null) {
        throw new RuntimeException("Duplicate neighbours!");
      }
    }
  }

  /**
   * Whether vertex is a boundary vertex (philosopher). Thread-safe
   * for all concurrent calls EXCEPT addBoundaryVertex() calls.
   *
   * @param vertexId Vertex id to check
   * @return True if vertex is boundary vertex
   */
  public boolean isBoundaryVertex(I vertexId) {
    return pMap.containsKey(((LongWritable) vertexId).get());
  }


  /**
   * Blocking call that returns when all forks are acquired and
   * the philosopher is ready to eat. Equivalent to "start eating".
   *
   * @param vertexId Vertex id (philosopher) to acquire forks for
   */
  public void acquireForks(I vertexId) {
    boolean needRemoteFork = false;
    boolean needForks = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);

    synchronized (neighbours) {
      ObjectIterator<Long2ByteMap.Entry> itr =
        neighbours.long2ByteEntrySet().fastIterator();
      while (itr.hasNext()) {
        Long2ByteMap.Entry e = itr.next();
        long neighbourId = e.getLongKey();
        byte forkInfo = e.getByteValue();

        if (haveToken(forkInfo) && !haveFork(forkInfo)) {
          dstId.set(neighbourId);
          needRemoteFork |= sendToken(vertexId, (I) dstId);
          needForks = true;
        } else if (!haveToken(forkInfo) &&
                   haveFork(forkInfo) && isDirty(forkInfo)) {
          // TODO-YH: is this really safe?? will this cause deadlock?
          // this would be unfair, but useful b/c of stragglers
          forkInfo &= ~MASK_IS_DIRTY;
        }

        neighbours.put(neighbourId, forkInfo);
      }
    }

    if (needRemoteFork) {
      serviceWorker.getWorkerClient().waitAllRequests();
    }

    while (needForks) {
      cvLock.lock();
      try {
        // wait for signal
        getFork.await();
      } catch (InterruptedException e) {
        // TODO-YH: blow up?
        throw new RuntimeException("Got interrupted!");
      } finally {
        cvLock.unlock();
      }

      // signalled; recheck forks
      needForks = false;
      synchronized (neighbours) {
        ObjectIterator<Long2ByteMap.Entry> itr =
          neighbours.long2ByteEntrySet().fastIterator();
        while (itr.hasNext()) {
          byte forkInfo = itr.next().getByteValue();
          if (!haveFork(forkInfo)) {
            needForks = true;
            break;
          }
        }
      }
    }
  }

  /**
   * Dirties used forks and satisfies any pending requests
   * for such forks. Equivalent to "stop eating".
   *
   * @param vertexId Vertex id (philosopher) that finished eating
   */
  public void releaseForks(I vertexId) {
    boolean needFlush = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);

    // all held forks are (implicitly) dirty
    synchronized (neighbours) {
      ObjectIterator<Long2ByteMap.Entry> itr =
        neighbours.long2ByteEntrySet().fastIterator();
      while (itr.hasNext()) {
        Long2ByteMap.Entry e = itr.next();
        long neighbourId = e.getLongKey();
        byte forkInfo = e.getByteValue();

        if (haveToken(forkInfo)) {
          // send fork if our philosopher pair has requested it
          dstId.set(neighbourId);
          needFlush |= sendFork(vertexId, (I) dstId);
          forkInfo &= ~MASK_HAVE_FORK;
        } else {
          // otherwise, explicitly dirty the fork
          // (so that fork is released immediately on token receipt)
          forkInfo |= MASK_IS_DIRTY;
        }

        neighbours.put(neighbourId, forkInfo);
      }
    }

    if (needFlush) {
      serviceWorker.getWorkerClient().waitAllRequests();
    }
  }

  /**
   * Send token/request for fork.
   *
   * @param senderId Sender of token
   * @param receiverId Receiver of token
   * @return True if receiver is remote, false if local
   */
  public boolean sendToken(I senderId, I receiverId) {
    int dstTaskId = serviceWorker.getVertexPartitionOwner(receiverId).
      getWorkerInfo().getTaskId();

    if (serviceWorker.getWorkerInfo().getTaskId() == dstTaskId) {
      // handle request locally
      receiveToken(senderId, receiverId);
      return false;
    } else {
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingTokenRequest(senderId, receiverId));
      return true;
    }
  }

  /**
   * Send fork.
   *
   * @param senderId Sender of fork
   * @param receiverId Receiver of fork
   * @return True if receiver is remote, false if local
   */
  public boolean sendFork(I senderId, I receiverId) {
    int dstTaskId = serviceWorker.getVertexPartitionOwner(receiverId).
      getWorkerInfo().getTaskId();

    if (serviceWorker.getWorkerInfo().getTaskId() == dstTaskId) {
      // handle request locally
      receiveFork(senderId, receiverId);
      return false;
    } else {
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingForkRequest(senderId, receiverId));
      return true;
    }
  }

  /**
   * Process a received token/request for fork.
   *
   * @param senderId Sender of token
   * @param receiverId New holder of token
   */
  public void receiveToken(I senderId, I receiverId) {
    boolean needFlush = false;

    long pId = ((LongWritable) receiverId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);

    synchronized (neighbours) {
      long neighbourId = ((LongWritable) senderId).get();
      byte forkInfo = neighbours.get(neighbourId);

      // fork requests are always sent with a token
      forkInfo |= MASK_HAVE_TOKEN;

      // If fork is dirty, we can immediately send it.
      // Otherwise, return---fork holder will eventually see
      // token when it dirties the fork.
      if (isDirty(forkInfo)) {
        needFlush |= sendFork(receiverId, senderId);
        forkInfo &= ~MASK_HAVE_FORK;  // fork sent
        forkInfo &= ~MASK_IS_DIRTY;   // no longer dirty
      }

      neighbours.put(neighbourId, forkInfo);
    }

    if (needFlush) {
      serviceWorker.getWorkerClient().waitAllRequests();
    }
  }

  /**
   * Process a received fork.
   *
   * @param senderId Sender of fork
   * @param receiverId New holder of fork
   */
  public void receiveFork(I senderId, I receiverId) {
    long pId = ((LongWritable) receiverId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);

    synchronized (neighbours) {
      long neighbourId = ((LongWritable) senderId).get();
      byte forkInfo = neighbours.get(neighbourId);
      forkInfo |= MASK_HAVE_FORK;
      neighbours.put(neighbourId, forkInfo);
    }

    // signal fork arrival
    cvLock.lock();
    getFork.signal();
    cvLock.unlock();
  }

  /**
   * @param forkInfo Philosopher's state
   * @return True if have token
   */
  private boolean haveToken(byte forkInfo) {
    return (forkInfo & MASK_HAVE_TOKEN) != 0;
  }

  /**
   * @param forkInfo Philosopher's state
   * @return True if have fork
   */
  private boolean haveFork(byte forkInfo) {
    return (forkInfo & MASK_HAVE_FORK) != 0;
  }

  /**
   * @param forkInfo Philosopher's state
   * @return True if fork is dirty
   */
  private boolean isDirty(byte forkInfo) {
    return (forkInfo & MASK_IS_DIRTY) != 0;
  }
}
