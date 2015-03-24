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
import org.apache.giraph.comm.requests.SendVertexDLDepRequest;
import org.apache.giraph.comm.requests.SendVertexDLForkRequest;
import org.apache.giraph.comm.requests.SendVertexDLTokenRequest;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ByteArrayVertexIdVertexId;
import org.apache.giraph.utils.VertexIdData;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.log4j.Logger;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ByteMap;
import it.unimi.dsi.fastutil.longs.Long2ByteOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.concurrent.locks.Condition;
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
  // "thinking" philosophers are absent from both sets

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

    //LOG.info("[[PTABLE]] ========================================");
    //LOG.info("[[PTABLE]] I am worker: " +
    //         serviceWorker.getWorkerInfo().getTaskId());
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
   * Send our philosopher's dependencies to their partners.
   * Required in the case of directed graphs (neighbouring
   * philosopher will otherwise fail to see dependencies).
   */
  public void sendDependencies() {
    LongWritable vertexId = new LongWritable();     // reused
    LongWritable depVertexId = new LongWritable();  // reused

    // when to flush cache/send message to particular worker
    int maxMessagesSizePerWorker =
      GiraphConfiguration.MAX_MSG_REQUEST_SIZE.get(conf);

    // cache for messages that will be sent to neighbours
    // (much more efficient than using SendDataCache b/c
    //  we don't need to store/know dst partition ids)
    Int2ObjectOpenHashMap<VertexIdData<I, I>> msgCache =
      new Int2ObjectOpenHashMap<VertexIdData<I, I>>();


    // Get cached copy of keyset to ignore future modifications
    // (future modifications are always *received* dependencies).
    // keySet() is backed by collection, so need to get "safe" copy.
    long[] pKeySet;
    synchronized (pMap) {
      pKeySet = pMap.keySet().toLongArray();
    }

    long[] neighboursKeySet;
    for (long pId : pKeySet) {
      synchronized (pMap) {
        neighboursKeySet = pMap.get(pId).keySet().toLongArray();
      }

      // we store pId to neighbourId mapping, so we want every
      // neighbourId to know that they need a "to pId" mapping
      for (long neighbourId : neighboursKeySet) {
        vertexId.set(neighbourId);
        depVertexId.set(pId);

        // this will work even if vertex doesn't exist yet
        int dstTaskId = serviceWorker.getVertexPartitionOwner((I) vertexId).
          getWorkerInfo().getTaskId();
        //LOG.info("[[PTABLE]] send dependency to taskId " + dstTaskId);

        // skip sending message for local request
        if (serviceWorker.getWorkerInfo().getTaskId() == dstTaskId) {
          receiveDependency((I) vertexId, (I) depVertexId);
        } else {
          // no need to sync, this is executed by single thread
          VertexIdData<I, I> messages = msgCache.get(dstTaskId);
          if (messages == null) {
            messages = new ByteArrayVertexIdVertexId<I>();
            messages.setConf(conf);
            messages.initialize();
            msgCache.put(dstTaskId, messages);
          }

          // note that, unlike regular data messages, first vertex id
          // is also data---i.e., this msg is not being sent to that id
          messages.add((I) vertexId, (I) depVertexId);

          if (messages.getSize() > maxMessagesSizePerWorker) {
            msgCache.remove(dstTaskId);
            serviceWorker.getWorkerClient().sendWritableRequest(
                dstTaskId, new SendVertexDLDepRequest(messages));
          }
        }
      }
    }

    // send remaining messages
    for (int dstTaskId : msgCache.keySet()) {
      // no need to remove, map will be trashed entirely
      VertexIdData<I, I> messages = msgCache.get(dstTaskId);
      serviceWorker.getWorkerClient().sendWritableRequest(
          dstTaskId, new SendVertexDLDepRequest(messages));
    }

    // flush network
    serviceWorker.getWorkerClient().waitAllRequests();
  }

  /**
   * Process a received dependency.
   *
   * @param vertexId Vertex id of philosopher
   * @param depVertexId Vertex id of new dependency
   */
  public void receiveDependency(I vertexId, I depVertexId) {
    long pId = ((LongWritable) vertexId).get();
    long depId = ((LongWritable) depVertexId).get();

    // NOTE: this works even when there are vertices that have yet
    // to be added via vertex mutation, because fork/philosopher
    // state is stored separately from the vertex object

    Long2ByteOpenHashMap neighbours;
    synchronized (pMap) {
      neighbours = pMap.get(pId);
      if (neighbours == null) {
        neighbours = new Long2ByteOpenHashMap();
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
  }


  /**
   * Blocking call that returns when all forks are acquired and
   * the philosopher starts to eat.
   *
   * @param vertexId Vertex id (philosopher) to acquire forks for
   */
  public void acquireForks(I vertexId) {
    //LOG.info("[[PTABLE]] " + vertexId + ": acquiring forks");
    LongWritable dstId = new LongWritable();  // reused
    long pId = ((LongWritable) vertexId).get();

    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;
    boolean needFlush = false;

    // ALWAYS lock neighbours before pHungry/pEating
    synchronized (neighbours) {
      synchronized (pHungry) {
        pHungry.add(pId);
      }
    }

    // for loop NOT synchronized b/c mutations must never occur
    // while compute threads are running, so keyset is fixed
    // (if NEW items were added, this will fail catastrophically!)
    for (long neighbourId : neighbours.keySet()) {
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
        dstId.set(neighbourId);
        needFlush |= sendToken(vertexId, (I) dstId);
      }
    }

    if (needFlush) {
      //LOG.info("[[PTABLE]] " + vertexId + ":   flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      //LOG.info("[[PTABLE]] " + vertexId + ":   done flush");
    }

    boolean done;
    cvLock.lock();     // lock early to avoid missing signals
    try {
      while (true) {
        done = true;

        // Recheck forks. This must be done in a single block
        // to get fully consistent view of ALL forks.
        synchronized (neighbours) {
          ObjectIterator<Long2ByteMap.Entry> itr =
            neighbours.long2ByteEntrySet().fastIterator();
          while (itr.hasNext()) {
            byte forkInfo = itr.next().getByteValue();
            if (!haveFork(forkInfo)) {
              //LOG.info("[[PTABLE]] " + vertexId + ": still missing fork");
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
            //LOG.info("[[PTABLE]] " + vertexId + ": got all forks");
            return;
          }
        }

        //LOG.info("[[PTABLE]] " + vertexId + ": wait for forks");
        newFork.await();
        //LOG.info("[[PTABLE]] " + vertexId + ": woke up!");
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
   * @param vertexId Vertex id (philosopher) that finished eating
   */
  public void releaseForks(I vertexId) {
    //LOG.info("[[PTABLE]] " + vertexId + ": releasing forks");
    LongWritable dstId = new LongWritable();  // reused
    long pId = ((LongWritable) vertexId).get();

    Long2ByteOpenHashMap neighbours = pMap.get(pId);
    byte oldForkInfo;
    boolean needFlush = false;

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
    for (long neighbourId : neighbours.keySet()) {
      // must synchronize since we may receive token
      synchronized (neighbours) {
        byte forkInfo = neighbours.get(neighbourId);
        oldForkInfo = forkInfo;

        // we stop eating right away, so a previously dirtied fork
        // (dirty forks can be reused) CAN be taken from under us
        if (!haveFork(forkInfo)) {
          //LOG.info("[[PTABLE]] " + vertexId + ": already lost fork " +
          //         neighbourId + " " + toString(forkInfo));
          continue;

        } else {
          if (haveToken(forkInfo)) {
            // fork will be sent outside of sync block
            forkInfo &= ~MASK_IS_DIRTY;
            forkInfo &= ~MASK_HAVE_FORK;

            //LOG.info("[[PTABLE]] " + vertexId + ": sending clean fork to " +
            //         neighbourId + " " + toString(forkInfo));
          } else {
            // otherwise, explicitly dirty the fork
            // (so that fork is released immediately on token receipt)
            forkInfo |= MASK_IS_DIRTY;

            //LOG.info("[[PTABLE]] " + vertexId + ": dirty fork " +
            //         neighbourId + " " + toString(forkInfo));
          }
          neighbours.put(neighbourId, forkInfo);
        }
      }

      if (haveFork(oldForkInfo) && haveToken(oldForkInfo)) {
        dstId.set(neighbourId);
        needFlush |= sendFork(vertexId, (I) dstId);
      }
    }

    // NOTE: this is also what flushes pending messages to the network,
    // so that other philosophers will see up-to-date messages (required
    // for serializability). If nobody needs a fork, messages will
    // get flushed only when that fork is requested.
    if (needFlush) {
      //LOG.info("[[PTABLE]] " + vertexId + ": flushing");
      serviceWorker.getWorkerClient().waitAllRequests();
      //LOG.info("[[PTABLE]] " + vertexId + ": done flush");
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
        new SendVertexDLTokenRequest(senderId, receiverId, conf),
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
  public boolean sendFork(I senderId, I receiverId) {
    int dstTaskId = serviceWorker.getVertexPartitionOwner(receiverId).
      getWorkerInfo().getTaskId();

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
        new SendVertexDLForkRequest(senderId, receiverId, conf),
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
  public void receiveToken(I senderId, I receiverId) {
    long pId = ((LongWritable) receiverId).get();
    long neighbourId = ((LongWritable) senderId).get();

    Long2ByteOpenHashMap neighbours = pMap.get(pId);
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
  public void receiveFork(I senderId, I receiverId) {
    long pId = ((LongWritable) receiverId).get();
    long neighbourId = ((LongWritable) senderId).get();
    Long2ByteOpenHashMap neighbours = pMap.get(pId);

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
