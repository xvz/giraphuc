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
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

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
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(PhilosophersTable.class);

  /** Mask for have-token bit */
  private static final byte MASK_HAVE_TOKEN = 0x1;
  /** Mask for have-fork bit */
  private static final byte MASK_HAVE_FORK = 0x2;
  /** Mask for is-dirty bit */
  private static final byte MASK_IS_DIRTY = 0x4;
  /** Mask for is-hungry bit */
  private static final byte MASK_IS_HUNGRY = 0x8;

  /** Provided configuration */
  private final ImmutableClassesGiraphConfiguration<I, V, E> conf;
  /** Service worker */
  private final CentralizedServiceWorker<I, V, E> serviceWorker;

  /** Runnable for waiting on requests asynchronously */
  private final Runnable waitAllRunnable;
  /** Lock for accessing wait thread */
  private final Lock waitLock = new ReentrantLock();

  /**
   * Map of vertex id (philosopher) to vertex ids (vertex's neighbours)
   * to single byte (indicates status, fork, token, dirty/clean).
   *
   * Basically, this tracks the state of philosophers (boundary vertices),
   * who are sitting at a distributed table, that are local to this worker.
   */
  private Long2ObjectOpenHashMap<Long2ByteOpenHashMap> pMap;
  /** Set of vertices (philosophers) that are hungry (acquiring forks) */
  private LongOpenHashSet pHungry;

  /** Thread for waiting on requests asynchronously */
  private Thread waitThread;

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
    this.pHungry = new LongOpenHashSet(
      Math.min(conf.getNumComputeThreads(),
               serviceWorker.getPartitionStore().getNumPartitions()));

    this.waitAllRunnable = new Runnable() {
        @Override
        public void run() {
          PhilosophersTable.this.serviceWorker.
            getWorkerClient().waitAllRequests();
        }
      };

    LOG.info("[[TESTING]] ========================================");
    LOG.info("[[TESTING]] I am worker: " +
             serviceWorker.getWorkerInfo().getTaskId());
  }

  /**
   * Add and initialize a vertex as a philosopher if it's a boundary vertex.
   * Must not be called/used when compute threads are executing.
   *
   * @param vertex Vertex to be added
   */
  public void addVertexIfBoundary(Vertex<I, V, E> vertex) {
    Long2ByteOpenHashMap neighbours = null;

    int partitionId = serviceWorker.
      getVertexPartitionOwner(vertex.getId()).getPartitionId();
    long pId = ((LongWritable) vertex.getId()).get();  // "philosopher" id

    // TODO-YH: assumes undirected graph... for directed graph,
    // need do broadcast to all neighbours
    for (Edge<I, E> e : vertex.getEdges()) {
      int dstPartitionId = serviceWorker.
        getVertexPartitionOwner(e.getTargetVertexId()).getPartitionId();
      long neighbourId = ((LongWritable) e.getTargetVertexId()).get();
      byte forkInfo = 0;

      // Determine if neighbour is on different partition.
      // This does two things:
      // - if vertex is internal, skips creating "neighbours" altogether
      // - if vertex is boundary, skips tracking neighbours on same partition
      //   (same partition = executed by single thread = no forks needed)
      if (dstPartitionId != partitionId) {
        if (neighbours == null) {
          neighbours = new Long2ByteOpenHashMap(vertex.getNumEdges());
        }

        // For acyclic precedence graph, always initialize
        // tokens at smaller id and dirty fork at larger id.
        // Skip self-loops (saves a bit of space).
        if (neighbourId == pId) {
          continue;
        } else if (neighbourId < pId) {
          // I am larger id, so I hold dirty fork
          forkInfo |= MASK_HAVE_FORK;
          forkInfo |= MASK_IS_DIRTY;
        } else {
          forkInfo |= MASK_HAVE_TOKEN;
        }
        neighbours.put(neighbourId, forkInfo);
      }
    }

    if (neighbours != null) {
      synchronized (pMap) {
        Long2ByteOpenHashMap ret = pMap.put(pId, neighbours);
        if (ret != null) {
          throw new RuntimeException("Duplicate neighbours!");
        }
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
   * @return True if forks acquired, false otherwise
   */
  public boolean acquireForks(I vertexId) {
    LOG.info("[[TESTING]] " + vertexId + ": acquiring forks");
    boolean needRemoteFork = false;
    boolean needForks = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    // acquiring fork = hungry philosopher
    synchronized (pHungry) {
      pHungry.add(pId);
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    for (long neighbourId : neighbours.keySet()) {
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        if (haveToken(forkInfo) && !haveFork(forkInfo)) {
          forkInfo &= ~MASK_HAVE_TOKEN;
          needForks = true;
          LOG.info("[[TESTING]] " + vertexId + ":   missing fork " +
                   neighbourId + " " + toString(forkInfo));
        } else if (haveFork(forkInfo) && isDirty(forkInfo)) {
          // This scenario isn't detailed in original Chandy-Misra alg.
          //
          // For our usage, dirty fork will remain dirty UNTIL partner vertex
          // tries to acquire it by executing. However, if worker is done,
          // such vertices will never execute! We can either wake worker up
          // and force it to do dummy round of fork acquisition + release,
          // or we can send our dirty for to our partner and let them decide
          // whether they want to give a clean one back to us.
          //
          // We use latter sln, b/c former sln uses a lot more messages.
          // This is what pHungry is used for: if philosopher isn't hungry,
          // it will send back clean fork; else it will hold on to fork,
          // equivalent to if we sent a token to try and request fork.
          //
          // Note that we CANNOT clean our own fork directly (i.e., w/o
          // consulting our partner) b/c it can result in cyclic precedence
          // graph (=> deadlock).
          forkInfo &= ~MASK_IS_DIRTY;
          forkInfo &= ~MASK_HAVE_FORK;
          needForks = true;
          LOG.info("[[TESTING]] " + vertexId + ":   dirty fork " +
                   neighbourId + " " + toString(forkInfo));
        } else {
          LOG.info("[[TESTING]] " + vertexId + ":   have clean fork " +
                   neighbourId + " " + toString(forkInfo));
        }

        neighbours.put(neighbourId, forkInfo);
      }

      // We're working with stale forkInfo here, so that request can
      // be sent outside of synchronization block. This has to be
      // done to avoid deadlocks due to:
      // - remote sendToken getting blocked due to message flushing
      // - local sendToken getting blocked due to local conflicts
      //
      // Also note that we've applied forkInfo update before sending.
      // Important b/c local requests will immediately modify forkInfo.
      if (haveToken(oldForkInfo) && !haveFork(oldForkInfo)) {
        dstId.set(neighbourId);
        needRemoteFork |= sendToken(vertexId, (I) dstId);
      } else if (haveFork(oldForkInfo) && isDirty(oldForkInfo)) {
        // send dirty fork and see if we get it back
        dstId.set(neighbourId);
        needRemoteFork |= sendFork(vertexId, (I) dstId);
      }
    }

    if (needRemoteFork) {
      LOG.info("[[TESTING]] " + vertexId + ":   flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.info("[[TESTING]] " + vertexId + ":   done flush");
    }

    if (!needForks) {
      LOG.info("[[TESTING]] " + vertexId + ": got all forks");
      return true;
    } else {
      // If our requests have been delivered but our forks still
      // haven't all arrived, then they are in use (forks have
      // guaranteed delivery b/c we flush all messages).
      //
      // For efficiency, don't block---just give up and execute
      // other vertices. Revisit on next (logical) superstep.
      for (long neighbourId : neighbours.keySet()) {
        synchronized (neighbours) {
          byte forkInfo = neighbours.get(neighbourId);
          if (!haveFork(forkInfo)) {
            LOG.info("[[TESTING]] " + vertexId + ": no go, missing fork " +
                     neighbourId + " " + toString(forkInfo));
            synchronized (pHungry) {
              pHungry.remove(pId);   // no longer hungry
            }
            return false;
          }
        }
      }
    }
    LOG.info("[[TESTING]] " + vertexId + ": got all forks");
    return true;
  }

  /**
   * Dirties used forks and satisfies any pending requests
   * for such forks. Equivalent to "stop eating".
   *
   * @param vertexId Vertex id (philosopher) that finished eating
   */
  public void releaseForks(I vertexId) {
    LOG.info("[[TESTING]] " + vertexId + ": releasing forks");
    boolean needFlush = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    // done eating, no longer hungry
    synchronized (pHungry) {
      pHungry.remove(pId);
    }

    // all held forks are (implicitly) dirty
    for (long neighbourId : neighbours.keySet()) {
      // must synchronize since we may receive token
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        if (haveToken(forkInfo)) {
          // fork will be sent outside of sync block
          forkInfo &= ~MASK_HAVE_FORK;
          LOG.info("[[TESTING]] " + vertexId + ": sending clean fork to " +
                   neighbourId + " " + toString(forkInfo));
        } else {
          // otherwise, explicitly dirty the fork
          // (so that fork is released immediately on token receipt)
          forkInfo |= MASK_IS_DIRTY;
          LOG.info("[[TESTING]] " + vertexId + ": dirty fork " +
                   neighbourId + " " + toString(forkInfo));
        }

        neighbours.put(neighbourId, forkInfo);
      }

      if (haveToken(oldForkInfo)) {
        dstId.set(neighbourId);
        needFlush |= sendFork(vertexId, (I) dstId);
      }
    }

    if (needFlush) {
      LOG.info("[[TESTING]] " + vertexId + ": flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.info("[[TESTING]] " + vertexId + ": done flush");
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
      LOG.info("[[TESTING]] " + senderId +
               ": send local token to " + receiverId);
      // handle request locally
      receiveToken(senderId, receiverId);
      LOG.info("[[TESTING]] " + senderId +
               ": SENT local token to " + receiverId);
      return false;
    } else {
      LOG.info("[[TESTING]] " + senderId +
               ": send remote token to " + receiverId);
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingTokenRequest(senderId, receiverId, conf),
        true);
      LOG.info("[[TESTING]] " + senderId +
               ": SENT remote token to " + receiverId);
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
      LOG.info("[[TESTING]] " + senderId +
               ": send local fork to " + receiverId);
      // handle request locally
      receiveFork(senderId, receiverId);
      LOG.info("[[TESTING]] " + senderId +
               ": SENT local fork to " + receiverId);
      return false;
    } else {
      LOG.info("[[TESTING]] " + senderId +
               ": send remote fork to " + receiverId);
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingForkRequest(senderId, receiverId, conf),
        true);
      LOG.info("[[TESTING]] " + senderId +
               ": SENT remote fork to " + receiverId);
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
    long neighbourId = ((LongWritable) senderId).get();

    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    synchronized (neighbours) {
      byte forkInfo = neighbours.get(neighbourId);
      oldForkInfo = forkInfo;

      LOG.info("[[TESTING]] " + receiverId + ": got token from " +
               senderId  + " " + toString(forkInfo));

      // fork requests are always sent with a token
      forkInfo |= MASK_HAVE_TOKEN;

      // If fork is dirty, send outside of sync block.
      // Else, return---fork holder will see token when it dirties fork.
      if (isDirty(forkInfo)) {
        forkInfo &= ~MASK_IS_DIRTY;
        forkInfo &= ~MASK_HAVE_FORK;
      }

      neighbours.put(neighbourId, forkInfo);
    }

    if (isDirty(oldForkInfo)) {
      needFlush |= sendFork(receiverId, senderId);
    }

    // TODO-YH: still need async thread... why??
    if (needFlush) {
      // can't use synchronized block b/c ref is changing
      waitLock.lock();
      if (waitThread == null || !waitThread.isAlive()) {
        waitThread = new Thread(waitAllRunnable);
        waitThread.start();
      }
      waitLock.unlock();
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
    long neighbourId = ((LongWritable) senderId).get();

    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    boolean isReceiverHungry;
    synchronized (pHungry) {
      isReceiverHungry = pHungry.contains(pId);
    }

    synchronized (neighbours) {
      byte forkInfo = neighbours.get(neighbourId);
      oldForkInfo = forkInfo;

      // If fork is received and we didn't send token for it,
      // then sender is asking us if it's okay to reuse fork
      // without waiting for us (receiver) to eat. If we're
      // not hungry, let them have clean fork.
      // (this is not part of original Chandy-Misra alg)
      if (isReceiverHungry || !haveToken(forkInfo)) {
        forkInfo |= MASK_HAVE_FORK;
        neighbours.put(neighbourId, forkInfo);
      }
      LOG.info("[[TESTING]] " + receiverId + ": got fork from " +
               senderId + " " + toString(forkInfo));
    }

    if (!isReceiverHungry && haveToken(oldForkInfo)) {
      LOG.info("[[TESTING]] " + receiverId + ": returning clean fork to " +
               senderId + " " + toString(oldForkInfo));
      sendFork(receiverId, senderId);
    }
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

  /**
   * @param forkInfo Philosopher's state
   * @return String
   */
  private String toString(byte forkInfo) {
    return "(" + ((forkInfo & 0x4) >>> 2) +
      "," + ((forkInfo & 0x2) >>> 1) +
      "," + (forkInfo & 0x1) + ")";
  }
}
