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
import org.apache.giraph.comm.requests.SendPartitionDLForkRequest;
import org.apache.giraph.comm.requests.SendPartitionDLTokenRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Iterables;

/**
 * YH: Implements the hygienic dining philosophers solution that
 * treats partitions (rather than vertices) as philosophers.
 *
 * @param <I> Vertex id
 * @param <V> Vertex value
 * @param <E> Edge value
 */
public class PartitionPhilosophersTable<I extends WritableComparable,
    V extends Writable, E extends Writable> {
  /** Class logger */
  private static final Logger LOG =
    Logger.getLogger(PartitionPhilosophersTable.class);

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
   * Map of partition id (philosopher) to partition ids (dependencies)
   * to single byte (indicates dirty/clean, have fork, have token).
   *
   * Basically, this tracks the state of philosophers (partitions),
   * sitting at a distributed "table", that are local to this worker.
   */
  private Int2ObjectOpenHashMap<Int2ByteOpenHashMap> pMap;
  /** Set of vertices (philosophers) that are hungry (acquiring forks) */
  private IntOpenHashSet pHungry;
  /** Set of vertices (philosophers) that are eating (acquired forks) */
  private IntOpenHashSet pEating;

  /** Total number of partitions across all workers */
  private int numTotalPartitions;
  /** Map of partition ids to worker/task ids */
  private Int2IntOpenHashMap taskIdMap;

  /**
   * Constructor
   *
   * @param conf Configuration used.
   * @param serviceWorker Service worker
   */
  public PartitionPhilosophersTable(
     ImmutableClassesGiraphConfiguration<I, V, E> conf,
     CentralizedServiceWorker<I, V, E> serviceWorker) {
    this.conf = conf;
    this.serviceWorker = serviceWorker;

    int numLocalPartitions =
      serviceWorker.getPartitionStore().getNumPartitions();

    // need one entry for every local partition
    this.pMap =
      new Int2ObjectOpenHashMap<Int2ByteOpenHashMap>(numLocalPartitions);

    this.pHungry = new IntOpenHashSet(
      Math.min(conf.getNumComputeThreads(), numLocalPartitions));
    this.pEating = new IntOpenHashSet(
      Math.min(conf.getNumComputeThreads(), numLocalPartitions));

    // this gives total number of partitions, b/c WorkerGraphPartitioner
    // must store location of ALL partitions in system
    // (PartitionOwners are init and assigned by master)
    this.numTotalPartitions =
      Iterables.size(serviceWorker.getPartitionOwners());

    this.taskIdMap = new Int2IntOpenHashMap(numTotalPartitions);

    this.waitAllRunnable = new Runnable() {
        @Override
        public void run() {
          PartitionPhilosophersTable.this.serviceWorker.
            getWorkerClient().waitAllRequests();
        }
      };

    LOG.debug("[[PTABLE]] ========================================");
    LOG.debug("[[PTABLE]] I am worker: " +
             serviceWorker.getWorkerInfo().getTaskId());
  }

  /**
   * Add a vertex's dependencies to the partition to which it belongs.
   * Must not be called/used when compute threads are executing.
   *
   * @param vertex Vertex to be processed
   */
  public void addVertexDependencies(Vertex<I, V, E> vertex) {
    // partition and "philosopher" id
    int partitionId = serviceWorker.
      getVertexPartitionOwner(vertex.getId()).getPartitionId();

    Int2ByteOpenHashMap neighbours;
    synchronized (pMap) {
      neighbours = pMap.get(partitionId);
      if (neighbours == null) {
        // for hash partitioning, one partition will almost always
        // depend uniformly on ALL other partitions
        neighbours = new Int2ByteOpenHashMap(numTotalPartitions);
      }
    }

    // TODO-YH: assumes undirected graph... for directed graph,
    // need to broadcast to all neighbours + check in-edges
    for (Edge<I, E> e : vertex.getEdges()) {
      int dstPartitionId = serviceWorker.
        getVertexPartitionOwner(e.getTargetVertexId()).getPartitionId();

      // neighbours are not deleted, so no need to sync on pMap
      synchronized (neighbours) {
        // partition dependency may have already been
        // initialized by another vertex.. if so, skip
        if (neighbours.get(dstPartitionId) != 0) {
          continue;
        } else {
          byte forkInfo = 0;

          // For acyclic precedence graph, always initialize
          // tokens at smaller id and dirty fork at larger id.
          // Skip self-loops/dependencies.
          if (dstPartitionId == partitionId) {
            continue;
          } else if (dstPartitionId < partitionId) {
            // I have larger id, so I hold dirty fork
            forkInfo |= MASK_HAVE_FORK;
            forkInfo |= MASK_IS_DIRTY;
          } else {
            forkInfo |= MASK_HAVE_TOKEN;
          }
          neighbours.put(dstPartitionId, forkInfo);

          // also store reverse index of partition to task ids
          // (by default, we only have task to partition ids)
          taskIdMap.put(dstPartitionId,
                        serviceWorker.getVertexPartitionOwner(vertex.getId()).
                        getWorkerInfo().getTaskId());
        }
      }
    }
  }

  /**
   * Blocking call that returns true when all forks are acquired and
   * the philosopher starts to eat, or false if acquisition failed.
   *
   * @param pId Partition id (philosopher) to acquire forks for
   * @return True if forks acquired, false otherwise
   */
  public boolean acquireForks(int pId) {
    LOG.debug("[[PTABLE]] " + pId + ": acquiring forks");
    boolean needRemoteFork = false;
    boolean needForks = false;

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    synchronized (pHungry) {
      pHungry.add(pId);
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    for (int neighbourId : neighbours.keySet()) {
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        if (haveToken(forkInfo) && !haveFork(forkInfo)) {
          forkInfo &= ~MASK_HAVE_TOKEN;
          needForks = true;
          LOG.debug("[[PTABLE]] " + pId + ":   missing fork " +
                   neighbourId + " " + toString(forkInfo));
        }
        // otherwise, we either have fork (could be clean or dirty)
        // or dirty fork was taken and we already sent a token for it

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
        needRemoteFork |= sendToken(pId, neighbourId);
      }
    }

    if (needRemoteFork) {
      LOG.debug("[[PTABLE]] " + pId + ":   flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.debug("[[PTABLE]] " + pId + ":   done flush");
    }

    // Do we have all our forks? This must be done in a single
    // block to get fully consistent view of ALL forks.
    synchronized (neighbours) {
      if (needForks) {
        for (int neighbourId : neighbours.keySet()) {
          byte forkInfo = neighbours.get(neighbourId);
          if (!haveFork(forkInfo)) {
            // For efficiency, don't block---just give up and execute
            // other vertices. Revisit on next (logical) superstep.
            LOG.debug("[[PTABLE]] " + pId + ": no go, missing fork " +
                     neighbourId + " " + toString(forkInfo));
            synchronized (pHungry) {
              pHungry.remove(pId);
            }
            return false;
          }
        }
      }

      // have all forks, now eating
      synchronized (pHungry) {
        pHungry.remove(pId);
      }
      synchronized (pEating) {
        pEating.add(pId);
      }
    }
    LOG.debug("[[PTABLE]] " + pId + ": got all forks");
    return true;
  }

  /**
   * Dirties used forks and satisfies any pending requests
   * for such forks. Equivalent to "stop eating".
   *
   * @param pId Partition id (philosopher) that finished eating
   */
  public void releaseForks(int pId) {
    LOG.debug("[[PTABLE]] " + pId + ": releasing forks");
    boolean needFlush = false;

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;

    synchronized (pEating) {
      pEating.remove(pId);
    }

    // all held forks are (implicitly) dirty
    for (int neighbourId : neighbours.keySet()) {
      // must synchronize since we may receive token
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        if (haveToken(forkInfo)) {
          // fork will be sent outside of sync block
          forkInfo &= ~MASK_HAVE_FORK;
          LOG.debug("[[PTABLE]] " + pId + ": sending clean fork to " +
                   neighbourId + " " + toString(forkInfo));
        } else {
          // otherwise, explicitly dirty the fork
          // (so that fork is released immediately on token receipt)
          forkInfo |= MASK_IS_DIRTY;
          LOG.debug("[[PTABLE]] " + pId + ": dirty fork " +
                   neighbourId + " " + toString(forkInfo));
        }

        neighbours.put(neighbourId, forkInfo);
      }

      if (haveToken(oldForkInfo)) {
        needFlush |= sendFork(pId, neighbourId);
      }
    }

    if (needFlush) {
      LOG.debug("[[PTABLE]] " + pId + ": flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      LOG.debug("[[PTABLE]] " + pId + ": done flush");
    }
  }

  /**
   * Send token/request for fork.
   *
   * @param senderId Sender of token
   * @param receiverId Receiver of token
   * @return True if receiver is remote, false if local
   */
  public boolean sendToken(int senderId, int receiverId) {
    int dstTaskId = taskIdMap.get(receiverId);

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
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendPartitionDLTokenRequest(senderId, receiverId));
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
  public boolean sendFork(int senderId, int receiverId) {
    int dstTaskId = taskIdMap.get(receiverId);

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
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendPartitionDLForkRequest(senderId, receiverId));
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
  public void receiveToken(int senderId, int receiverId) {
    boolean needFlush = false;
    int pId = senderId;
    int neighbourId = receiverId;

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
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
  public void receiveFork(int senderId, int receiverId) {
    int pId = senderId;
    int neighbourId = receiverId;
    Int2ByteOpenHashMap neighbours = pMap.get(pId);

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
