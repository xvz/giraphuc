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
 * YH: Implements the hygienic dining philosophers solution,
 * with vertices as philosophers.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class VertexPhilosophersTable<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(VertexPhilosophersTable.class);

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

  /** Runnable for waiting on requests asynchronously */
  private final Runnable waitAllRunnable;
  /** Lock for accessing wait thread */
  private final Lock waitLock = new ReentrantLock();
  /** Thread for waiting on requests asynchronously */
  private Thread waitThread;

  /**
   * Map of vertex id (philosopher) to vertex ids (vertex's neighbours)
   * to single byte (indicates dirty/clean, have fork, have token).
   *
   * Basically, this tracks the state of philosophers (boundary vertices),
   * sitting at a distributed "table", that are local to this worker.
   */
  private Long2ObjectOpenHashMap<Long2ByteOpenHashMap> pMap;
  /** Set of vertices (philosophers) that are hungry (acquiring forks) */
  private LongOpenHashSet pHungry;
  /** Set of vertices (philosophers) that are eating (acquired forks) */
  private LongOpenHashSet pEating;

  /**
   * Constructor
   *
   * @param conf Configuration used.
   * @param serviceWorker Service worker
   */
  public VertexPhilosophersTable(
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
    this.pEating = new LongOpenHashSet(
      Math.min(conf.getNumComputeThreads(),
               serviceWorker.getPartitionStore().getNumPartitions()));

    this.waitAllRunnable = new Runnable() {
        @Override
        public void run() {
          VertexPhilosophersTable.this.serviceWorker.
            getWorkerClient().waitAllRequests();
        }
      };

    LOG.debug("[[PTABLE]] ========================================");
    LOG.debug("[[PTABLE]] I am worker: " +
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
    // need to broadcast to all neighbours + check in-edges
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
   * Blocking call that returns true when all forks are acquired and
   * the philosopher starts to eat, or false if acquisition failed.
   *
   * @param vertexId Vertex id (philosopher) to acquire forks for
   * @return True if forks acquired, false otherwise
   */
  public boolean acquireForks(I vertexId) {
    LOG.debug("[[PTABLE]] " + vertexId + ": acquiring forks");
    boolean needRemoteFork = false;
    boolean needForks = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    synchronized (pHungry) {
      pHungry.add(pId);
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    for (long neighbourId : neighbours.keySet()) {
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        if (!haveFork(forkInfo)) {
          needForks = true;
          LOG.debug("[[PTABLE]] " + vertexId + ":   missing fork " +
                    neighbourId + " " + toString(forkInfo));

          // missing fork does NOT imply holding token, b/c we
          // may have already sent the token for our missing fork
          if (haveToken(forkInfo)) {
            forkInfo &= ~MASK_HAVE_TOKEN;
          }
        }

        neighbours.put(neighbourId, forkInfo);
      }

      // We're working with stale forkInfo here, so that request can
      // be sent outside of synchronization block. This has to be
      // done to avoid deadlocks due to local sendToken getting blocked
      // due to local conflicts.
      //
      // Also note that we've applied forkInfo update before sending.
      // Important b/c local requests will immediately modify forkInfo.
      if (haveToken(oldForkInfo) && !haveFork(oldForkInfo)) {
        dstId.set(neighbourId);
        needRemoteFork |= sendToken(vertexId, (I) dstId);
      }
    }

    if (needRemoteFork) {
      LOG.debug("[[PTABLE]] " + vertexId + ":   flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.debug("[[PTABLE]] " + vertexId + ":   done flush");
    }

    // Do we have all our forks? This must be done in a single
    // block to get fully consistent view of ALL forks.
    synchronized (neighbours) {
      if (needForks) {
        for (long neighbourId : neighbours.keySet()) {
          byte forkInfo = neighbours.get(neighbourId);
          if (!haveFork(forkInfo)) {
            // For efficiency, don't block---just give up and execute
            // other vertices. Revisit on next (logical) superstep.
            LOG.debug("[[PTABLE]] " + vertexId + ": no go, missing fork " +
                     neighbourId + " " + toString(forkInfo));
            synchronized (pHungry) {
              pHungry.remove(pId);
            }
            return false;
          }
        }
      }

      // have all forks, now eating
      synchronized (pEating) {
        pEating.add(pId);
      }
      synchronized (pHungry) {
        pHungry.remove(pId);
      }
    }
    LOG.debug("[[PTABLE]] " + vertexId + ": got all forks");
    return true;
  }

  /**
   * Dirties used forks and satisfies any pending requests
   * for such forks. Equivalent to "stop eating".
   *
   * @param vertexId Vertex id (philosopher) that finished eating
   */
  public void releaseForks(I vertexId) {
    LOG.debug("[[PTABLE]] " + vertexId + ": releasing forks");
    boolean needFlush = false;
    LongWritable dstId = new LongWritable();  // reused

    long pId = ((LongWritable) vertexId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    synchronized (pEating) {
      pEating.remove(pId);
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
          LOG.debug("[[PTABLE]] " + vertexId + ": sending clean fork to " +
                   neighbourId + " " + toString(forkInfo));
        } else {
          // otherwise, explicitly dirty the fork
          // (so that fork is released immediately on token receipt)
          forkInfo |= MASK_IS_DIRTY;
          LOG.debug("[[PTABLE]] " + vertexId + ": dirty fork " +
                   neighbourId + " " + toString(forkInfo));
        }

        neighbours.put(neighbourId, forkInfo);
      }

      if (haveToken(oldForkInfo)) {
        dstId.set(neighbourId);
        needFlush |= sendFork(vertexId, (I) dstId);
      }
    }

    // NOTE: this is also what flushes pending messages to the network,
    // so that other philosophers will see up-to-date messages (required
    // for serializability). If nobody needs a fork, messages will
    // get flushed only when that fork is requested.
    if (needFlush) {
      LOG.debug("[[PTABLE]] " + vertexId + ": flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.debug("[[PTABLE]] " + vertexId + ": done flush");
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
      LOG.debug("[[PTABLE]] " + senderId +
               ": send local token to " + receiverId);
      // handle request locally
      receiveToken(senderId, receiverId);
      LOG.debug("[[PTABLE]] " + senderId +
               ": SENT local token to " + receiverId);
      return false;
    } else {
      LOG.debug("[[PTABLE]] " + senderId +
               ": send remote token to " + receiverId);
      // call must be non-blocking to avoid deadlocks
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingTokenRequest(senderId, receiverId, conf),
        true);
      LOG.debug("[[PTABLE]] " + senderId +
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
      LOG.debug("[[PTABLE]] " + senderId +
               ": send local fork to " + receiverId);
      // handle request locally
      receiveFork(senderId, receiverId);
      LOG.debug("[[PTABLE]] " + senderId +
               ": SENT local fork to " + receiverId);
      return false;
    } else {
      LOG.debug("[[PTABLE]] " + senderId +
               ": send remote fork to " + receiverId);
      // call must be non-blocking to avoid deadlocks
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendDistributedLockingForkRequest(senderId, receiverId, conf),
        true);
      LOG.debug("[[PTABLE]] " + senderId +
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

    boolean isHungry;
    boolean isEating;

    // locking order matters: lock neighbours before pEating/pHungry;
    // this is same order as in acquireForks()'s recheck-all-forks
    synchronized (neighbours) {
      synchronized (pHungry) {
        isHungry = pHungry.contains(pId);
      }
      synchronized (pEating) {
        isEating = pEating.contains(pId);
      }

      byte forkInfo = neighbours.get(neighbourId);
      oldForkInfo = forkInfo;

      LOG.debug("[[PTABLE]] " + receiverId + ": got token from " +
               senderId  + " " + toString(forkInfo));

      if (isEating || !isDirty(forkInfo)) {
        // Do not give up fork, but record token so that receiver
        // will see it when it finishes eating.
        //
        // This works b/c a philosopher has a clean fork iff it wants
        // to eat, hence the fork is guaranteed to become dirty.
        //
        // Philosopher quits if it can't get all its forks, so it
        // CAN be "thinking" while holding a clean fork (i.e., it
        // failed to eat). But this is okay, since sender will also
        // quit if it can't get fork. Note that we cannot give up
        // clean fork, as it creates cycle in precedence graph.
        forkInfo |= MASK_HAVE_TOKEN;
        LOG.debug("[[PTABLE]] " + receiverId + ": not giving up fork " +
                 senderId  + " " + toString(forkInfo));
      } else if (!isHungry && !isEating && isDirty(forkInfo)) {
        // Give up dirty fork and record token receipt.
        forkInfo &= ~MASK_IS_DIRTY;
        forkInfo &= ~MASK_HAVE_FORK;
        forkInfo |= MASK_HAVE_TOKEN;
        LOG.debug("[[PTABLE]] " + receiverId + ": give up fork " +
                  senderId  + " " + toString(forkInfo));
      } else if (isHungry && isDirty(forkInfo)) {
        // Give up fork (sender has priority), but tell sender that
        // we want fork back by sending back the newly receive token.
        forkInfo &= ~MASK_IS_DIRTY;
        forkInfo &= ~MASK_HAVE_FORK;
        LOG.debug("[[PTABLE]] " + receiverId + ": reluctantly give up fork " +
                 senderId  + " " + toString(forkInfo));
      }

      neighbours.put(neighbourId, forkInfo);
    }

    if (!isHungry && !isEating && isDirty(oldForkInfo)) {
      needFlush |= sendFork(receiverId, senderId);
    } else if (isHungry && isDirty(oldForkInfo)) {
      // TODO-YH: consolidate??
      needFlush |= sendFork(receiverId, senderId);
      needFlush |= sendToken(receiverId, senderId);
    }

    // NOTE: If fork is requested, we must flush our messages
    // in case it hasn't been flushed since releaseFork().
    // This ensures fork is delivered AND serializability
    // is maintained (newest messages show up).
    //
    // TODO-YH: still need async thread... why??
    if (needFlush) {
      // can't use synchronized block b/c waitThread ref is changing
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

    synchronized (neighbours) {
      byte forkInfo = neighbours.get(neighbourId);
      forkInfo |= MASK_HAVE_FORK;
      neighbours.put(neighbourId, forkInfo);
      LOG.debug("[[PTABLE]] " + receiverId + ": got fork from " +
               senderId + " " + toString(forkInfo));
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
