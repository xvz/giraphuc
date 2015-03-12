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
import org.apache.giraph.comm.requests.SendPartitionDLDepRequest;
import org.apache.giraph.comm.requests.SendPartitionDLForkRequest;
import org.apache.giraph.comm.requests.SendPartitionDLTokenRequest;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ByteMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.concurrent.locks.Condition;
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

  /** Lock for condition var */
  private final Lock cvLock = new ReentrantLock();
  /** Condition var for arrival of fork */
  private final Condition newFork = cvLock.newCondition();

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
  /** Set of partitions (philosophers) that are hungry (acquiring forks) */
  private IntOpenHashSet pHungry;
  /** Set of partitions (philosophers) that are eating (acquired forks) */
  private IntOpenHashSet pEating;
  // "thinking" philosophers are absent from both sets

  /** Total number of partitions across all workers */
  private int numTotalPartitions;
  /** Map of partition ids to worker/task ids */
  private Int2IntOpenHashMap taskIdMap;
  /** Map of partition ids to "all halted?" boolean */
  private Int2BooleanOpenHashMap allHaltedMap;

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
    this.allHaltedMap = new Int2BooleanOpenHashMap(numLocalPartitions);

    this.waitAllRunnable = new Runnable() {
        @Override
        public void run() {
          PartitionPhilosophersTable.this.serviceWorker.
            getWorkerClient().waitAllRequests();
        }
      };

    //LOG.info("[[PTABLE]] ========================================");
    //LOG.info("[[PTABLE]] I am worker: " +
    //         serviceWorker.getWorkerInfo().getTaskId());
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

    //LOG.info("[[PTABLE]] processing vertex " + vertex.getId() +
    //          ", partition " + partitionId);

    Int2ByteOpenHashMap neighbours;
    synchronized (pMap) {
      neighbours = pMap.get(partitionId);
      if (neighbours == null) {
        // for hash partitioning, one partition will almost always
        // depend uniformly on ALL other partitions
        neighbours = new Int2ByteOpenHashMap(numTotalPartitions);
        pMap.put(partitionId, neighbours);
      }
    }

    for (Edge<I, E> e : vertex.getEdges()) {
      int dstPartitionId = serviceWorker.
        getVertexPartitionOwner(e.getTargetVertexId()).getPartitionId();

      // neighbours are not deleted, so no need to sync on pMap
      synchronized (neighbours) {
        // partition dependency may have already been
        // initialized by another vertex.. if so, skip
        if (!neighbours.containsKey(dstPartitionId)) {
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
          //
          // need synchronize b/c not everyone synchronizes
          // on same "neighbours"
          synchronized (taskIdMap) {
            taskIdMap.put(dstPartitionId, serviceWorker.
                          getVertexPartitionOwner(e.getTargetVertexId()).
                          getWorkerInfo().getTaskId());
          }
        }
      }
    }
  }

  /**
   * Send our philosopher's dependencies to their partners.
   * Required in the case of directed graphs (neighbouring
   * philosopher will otherwise fail to see dependencies).
   */
  public void sendDependencies() {
    // Get cached copy of keyset to ignore future modifications
    // (future modifications are always *received* dependencies).
    // keySet() is backed by collection, so need to get "safe" copy.
    int[] pKeySet;
    synchronized (pMap) {
      pKeySet = pMap.keySet().toIntArray();
    }

    int[] neighboursKeySet;
    for (int pId : pKeySet) {
      synchronized (pMap) {
        neighboursKeySet = pMap.get(pId).keySet().toIntArray();
      }

      // we store pId to neighbourId mapping, so we want every
      // neighbourId to know that they need a "to pId" mapping
      for (int neighbourId : neighboursKeySet) {
        int dstTaskId = taskIdMap.get(neighbourId);
        //LOG.info("[[PTABLE]] send dependency to taskId " + dstTaskId);

        // skip sending message for local request
        if (serviceWorker.getWorkerInfo().getTaskId() == dstTaskId) {
          receiveDependency(neighbourId, pId);
        } else {
          SendPartitionDLDepRequest req =
            new SendPartitionDLDepRequest(neighbourId, pId);
          serviceWorker.getWorkerClient().
            sendWritableRequest(dstTaskId, req, true);
        }
      }

      // flush network
      serviceWorker.getWorkerClient().waitAllRequests();
    }
  }

  /**
   * Process a received dependency.
   *
   * @param pId Partition id of philosopher
   * @param depId Partition id of new dependency
   */
  public void receiveDependency(int pId, int depId) {
    Int2ByteOpenHashMap neighbours;
    synchronized (pMap) {
      neighbours = pMap.get(pId);

      // can happen when not-yet-created vertex
      // falls under an otherwise empty partition
      if (neighbours == null) {
        neighbours = new Int2ByteOpenHashMap(numTotalPartitions);
        pMap.put(pId, neighbours);
      }
    }

    synchronized (neighbours) {
      if (!neighbours.containsKey(depId)) {
        byte forkInfo = 0;
        if (depId < pId) {
          // I have larger id, so I hold dirty fork
          forkInfo |= MASK_HAVE_FORK;
          forkInfo |= MASK_IS_DIRTY;
        } else if (depId > pId) {
          forkInfo |= MASK_HAVE_TOKEN;
        } else {
          throw new IllegalStateException("Ids must not be same!");
        }
        neighbours.put(depId, forkInfo);
      }
    }

    synchronized (taskIdMap) {
      if (!taskIdMap.containsKey(depId)) {
        // have to do this the hard way---no vertex IDs available
        for (PartitionOwner po : serviceWorker.getPartitionOwners()) {
          if (po.getPartitionId() == depId) {
            taskIdMap.put(depId, po.getWorkerInfo().getTaskId());
            break;
          }
        }
      }
    }
  }


  /**
   * Blocking call that returns when all forks are acquired and
   * the philosopher starts to eat.
   *
   * @param pId Partition id (philosopher) to acquire forks for
   */
  public void acquireForks(int pId) {
    //LOG.info("[[PTABLE]] " + pId + ": acquiring forks");

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;
    boolean needFlush = false;

    // there CAN be partitions with 0 vertices---there will be
    // no dependencies on them, but we do need to handle them
    if (neighbours == null) {
      //LOG.info("[[PTABLE]] " + pId + ": empty partition");
      return;
    }

    // ALWAYS lock neighbours before pHungry/pEating
    synchronized (neighbours) {
      synchronized (pHungry) {
        pHungry.add(pId);
      }
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    // (if NEW items were added, this will fail catastrophically!)
    for (int neighbourId : neighbours.keySet()) {
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        // Note that missing fork does NOT imply holding token,
        // b/c we may have already sent off our token
        if (!haveFork(forkInfo) && haveToken(forkInfo)) {
          forkInfo &= ~MASK_HAVE_TOKEN;
          neighbours.put(neighbourId, forkInfo);

          //LOG.info("[[PTABLE]] " + pId + ":   request fork " +
          //         neighbourId + " " + toString(forkInfo));
        }
        // otherwise, we already sent our token or already hold
        // our fork (can be clean or dirty)
      }

      // We're working with stale forkInfo here, so that request can
      // be sent outside of synchronization block. This has to be
      // done to avoid deadlocks due to local sendToken getting blocked
      // due to local conflicts.
      //
      // Also note that we've applied forkInfo update before sending.
      // Important b/c local requests will immediately modify forkInfo.
      if (!haveFork(oldForkInfo) && haveToken(oldForkInfo)) {
        needFlush |= sendToken(pId, neighbourId);
      }
    }

    if (needFlush) {
      //LOG.info("[[PTABLE]] " + pId + ":   flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      //LOG.info("[[PTABLE]] " + pId + ":   done flush");
    }

    boolean done;
    cvLock.lock();     // lock early to avoid missing signals
    try {
      while (true) {
        done = true;

        // Recheck forks. This must be done in a single block
        // to get fully consistent view of ALL forks.
        synchronized (neighbours) {
          ObjectIterator<Int2ByteMap.Entry> itr =
            neighbours.int2ByteEntrySet().fastIterator();
          while (itr.hasNext()) {
            byte forkInfo = itr.next().getByteValue();
            if (!haveFork(forkInfo)) {
              //LOG.info("[[PTABLE]] " + pId + ": still missing fork");
              done = false;
              break;
            }
          }

          if (done) {
            // have all forks, now eating
            // must do this while synchronized on neighbours
            synchronized (pHungry) {
              pHungry.remove(pId);
            }
            synchronized (pEating) {
              pEating.add(pId);
            }
            //LOG.info("[[PTABLE]] " + pId + ": got all forks");
            return;
          }
        }

        //LOG.info("[[PTABLE]] " + pId + ": wait for forks");
        newFork.await();
        //LOG.info("[[PTABLE]] " + pId + ": woke up!");
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException("acquireForks: got interrupted!");
    } finally {
      cvLock.unlock();
    }
  }

  /**
   * Dirties used forks and satisfies any pending requests
   * for such forks. Equivalent to "stop eating".
   *
   * @param pId Partition id (philosopher) that finished eating
   */
  public void releaseForks(int pId) {
    //LOG.info("[[PTABLE]] " + pId + ": releasing forks");

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;
    boolean needFlush = false;

    // there CAN be partitions with 0 vertices---there will be
    // no dependencies on them, but we do need to handle them
    if (neighbours == null) {
      //LOG.info("[[PTABLE]] " + pId + ": empty partition");
      return;
    }

    // ALWAYS lock neighbours before pHungry/pEating
    synchronized (neighbours) {
      synchronized (pEating) {
        pEating.remove(pId);
      }
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    // (if NEW items were added, this will fail catastrophically!)
    //
    // all held forks are (possibly implicitly) dirty
    for (int neighbourId : neighbours.keySet()) {
      // must synchronize since we may receive token
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        // we stop eating right away, so a previously dirtied fork
        // (dirty forks can be reused) CAN be taken from under us
        if (!haveFork(forkInfo)) {
          //LOG.info("[[PTABLE]] " + pId + ": already lost fork " +
          //         neighbourId + " " + toString(forkInfo));
          continue;

        } else {
          if (haveToken(forkInfo)) {
            // fork will be sent outside of sync block
            forkInfo &= ~MASK_IS_DIRTY;
            forkInfo &= ~MASK_HAVE_FORK;

            //LOG.info("[[PTABLE]] " + pId + ": sending clean fork to " +
            //         neighbourId + " " + toString(forkInfo));
          } else {
            // otherwise, explicitly dirty the fork
            // (so that fork is released immediately on token receipt)
            forkInfo |= MASK_IS_DIRTY;

            //LOG.info("[[PTABLE]] " + pId + ": dirty fork " +
            //         neighbourId + " " + toString(forkInfo));
          }
          neighbours.put(neighbourId, forkInfo);
        }
      }

      if (haveFork(oldForkInfo) && haveToken(oldForkInfo)) {
        needFlush |= sendFork(pId, neighbourId);
      }
    }

    // NOTE: this is also what flushes pending messages to the network,
    // so that other philosophers will see up-to-date messages (required
    // for serializability). If nobody needs a fork, messages will
    // get flushed only when that fork is requested.
    if (needFlush) {
      //LOG.info("[[PTABLE]] " + pId + ": flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      //LOG.info("[[PTABLE]] " + pId + ": done flush");
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
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": send local token to " + receiverId);
      // handle request locally
      receiveToken(senderId, receiverId);
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": SENT local token to " + receiverId);
      return false;
    } else {
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": send remote token to " + receiverId);
      // call must be non-blocking to avoid deadlocks
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendPartitionDLTokenRequest(senderId, receiverId),
        true);
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": SENT remote token to " + receiverId);
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
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": send local fork to " + receiverId);
      // handle request locally
      receiveFork(senderId, receiverId);
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": SENT local fork to " + receiverId);
      return false;
    } else {
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": send remote fork to " + receiverId);
      // call must be non-blocking to avoid deadlocks
      serviceWorker.getWorkerClient().sendWritableRequest(
        dstTaskId,
        new SendPartitionDLForkRequest(senderId, receiverId),
        true);
      //LOG.info("[[PTABLE]] " + senderId +
      //         ": SENT remote fork to " + receiverId);
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
    int pId = receiverId;
    int neighbourId = senderId;

    Int2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;
    boolean needFlush = false;

    boolean isHungry;
    boolean isEating;

    // locking order matters: lock neighbours before pEating/pHungry;
    // this is same order as in acquireForks & releasForks
    synchronized (neighbours) {
      synchronized (pHungry) {
        isHungry = pHungry.contains(pId);
      }
      synchronized (pEating) {
        isEating = pEating.contains(pId);
      }

      byte forkInfo = neighbours.get(neighbourId);
      oldForkInfo = forkInfo;

      // invariant: must not already have token
      //if (haveToken(forkInfo)) {
      //  throw new IllegalStateException("Should not already have token!");
      //}

      //LOG.info("[[PTABLE]] " + receiverId + ": got token from " +
      //         senderId  + " " + toString(forkInfo));

      if (isEating || !isDirty(forkInfo)) {
        // Do not give up fork, but record token so that receiver
        // will see it when it finishes eating.
        //
        // This works b/c a philosopher has a clean fork iff it wants
        // to eat, hence the fork is guaranteed to become dirty. Note
        // that we cannot give up clean fork, as it creates a cycle
        // in the precedence graph.
        forkInfo |= MASK_HAVE_TOKEN;

        //LOG.info("[[PTABLE]] " + receiverId + ": not giving up fork " +
        //         senderId  + " " + toString(forkInfo));
      } else {
        if (isHungry) {      // hungry + not eating + dirty fork
          // Give up fork (sender has priority), but tell sender that
          // we want fork back by returning the newly received token.
          forkInfo &= ~MASK_IS_DIRTY;
          forkInfo &= ~MASK_HAVE_FORK;
        } else {     // thinking + dirty fork
          // Give up dirty fork and record token receipt.
          forkInfo &= ~MASK_IS_DIRTY;
          forkInfo &= ~MASK_HAVE_FORK;
          forkInfo |= MASK_HAVE_TOKEN;
        }
        //LOG.info("[[PTABLE]] " + receiverId + ": give up fork " +
        //          senderId  + " " + toString(forkInfo));
      }

      neighbours.put(neighbourId, forkInfo);
    }

    if (!isEating && isDirty(oldForkInfo)) {
      if (isHungry) {
        needFlush |= sendFork(receiverId, senderId);
        needFlush |= sendToken(receiverId, senderId);
      } else {
        needFlush |= sendFork(receiverId, senderId);
      }
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
  public void receiveFork(int senderId, int receiverId) {
    int pId = receiverId;
    int neighbourId = senderId;
    Int2ByteOpenHashMap neighbours = pMap.get(pId);

    synchronized (neighbours) {
      byte forkInfo = neighbours.get(neighbourId);
      forkInfo |= MASK_HAVE_FORK;
      neighbours.put(neighbourId, forkInfo);

      // invariant: fork must not be dirty
      //if (isDirty(forkInfo)) {
      //  throw new IllegalStateException("Fork should not be dirty!");
      //}

      //LOG.info("[[PTABLE]] " + receiverId + ": got fork from " +
      //         senderId + " " + toString(forkInfo));
    }

    // signal fork arrival
    cvLock.lock();
    newFork.signalAll();
    cvLock.unlock();
  }


  /**
   * Get whether a partition has all halted vertices.
   *
   * @param partitionId Partition id
   * @return True if partition has vertices that are all halted
   */
  public boolean allVerticesHalted(int partitionId) {
    synchronized (allHaltedMap) {
      // if partition id is missing, get() returns false,
      // which is exactly what we want
      return allHaltedMap.get(partitionId);
    }
  }

  /**
   * Store whether a partition has all halted vertices.
   *
   * @param partitionId Partition id
   * @param allHalted True if all vertices in partition have halted
   */
  public void setAllVerticesHalted(int partitionId, boolean allHalted) {
    synchronized (allHaltedMap) {
      allHaltedMap.put(partitionId, allHalted);
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
