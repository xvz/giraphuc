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

package org.apache.giraph.worker;

import org.apache.giraph.bsp.ApplicationState;
import org.apache.giraph.bsp.BspService;
import org.apache.giraph.bsp.CentralizedServiceWorker;
import org.apache.giraph.comm.ServerData;
import org.apache.giraph.comm.WorkerClient;
import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.comm.WorkerServer;
import org.apache.giraph.comm.aggregators.WorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerAggregatorRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerClient;
import org.apache.giraph.comm.netty.NettyWorkerClientRequestProcessor;
import org.apache.giraph.comm.netty.NettyWorkerServer;
import org.apache.giraph.comm.requests.SendGlobalTokenRequest;
import org.apache.giraph.conf.AsyncConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AddressesAndPartitionsWritable;
import org.apache.giraph.graph.FinishedSuperstepStats;
import org.apache.giraph.graph.GlobalStats;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.InputSplitEvents;
import org.apache.giraph.graph.InputSplitPaths;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.graph.VertexEdgeCount;
import org.apache.giraph.io.EdgeOutputFormat;
import org.apache.giraph.io.EdgeWriter;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.superstep_output.SuperstepOutput;
import org.apache.giraph.mapping.translate.TranslateEdge;
import org.apache.giraph.master.MasterInfo;
import org.apache.giraph.master.SuperstepClasses;
import org.apache.giraph.metrics.GiraphMetrics;
import org.apache.giraph.metrics.GiraphTimer;
import org.apache.giraph.metrics.GiraphTimerContext;
import org.apache.giraph.metrics.ResetSuperstepMetricsObserver;
import org.apache.giraph.metrics.SuperstepMetricsRegistry;
import org.apache.giraph.metrics.WorkerSuperstepMetrics;
import org.apache.giraph.partition.Partition;
import org.apache.giraph.partition.PartitionExchange;
import org.apache.giraph.partition.PartitionOwner;
import org.apache.giraph.partition.PartitionStats;
import org.apache.giraph.partition.PartitionStore;
import org.apache.giraph.partition.PartitionPhilosophersTable;
import org.apache.giraph.partition.VertexPhilosophersTable;
import org.apache.giraph.partition.WorkerGraphPartitioner;
import org.apache.giraph.partition.VertexTypeStore;
import org.apache.giraph.utils.CallableFactory;
import org.apache.giraph.utils.JMapHistoDumper;
import org.apache.giraph.utils.LoggerUtils;
import org.apache.giraph.utils.MemoryUtils;
import org.apache.giraph.utils.ProgressableUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.giraph.zk.BspEvent;
import org.apache.giraph.zk.PredicateLock;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import net.iharder.Base64;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper-based implementation of {@link CentralizedServiceWorker}.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
@SuppressWarnings("rawtypes")
public class BspServiceWorker<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends BspService<I, V, E>
    implements CentralizedServiceWorker<I, V, E>,
    ResetSuperstepMetricsObserver {
  /** Name of gauge for time spent waiting on other workers */
  public static final String TIMER_WAIT_REQUESTS = "wait-requests-us";
  /** Class logger */
  private static final Logger LOG = Logger.getLogger(BspServiceWorker.class);

  /** YH: Starting time of superstep 0, used for offset calculations */
  private static long START_TIME;

  /** My process health znode */
  private String myHealthZnode;
  /** Worker info */
  private final WorkerInfo workerInfo;
  /** Worker graph partitioner */
  private final WorkerGraphPartitioner<I, V, E> workerGraphPartitioner;
  /** Local Data for each worker */
  private final LocalData<I, V, E, ? extends Writable> localData;
  /** Used to translate Edges during vertex input phase based on localData */
  private final TranslateEdge<I, E> translateEdge;
  /** IPC Client */
  private final WorkerClient<I, V, E> workerClient;
  /** IPC Server */
  private final WorkerServer<I, V, E> workerServer;
  /** Request processor for aggregator requests */
  private final WorkerAggregatorRequestProcessor
  workerAggregatorRequestProcessor;
  /** Master info */
  private MasterInfo masterInfo = new MasterInfo();
  /** List of workers */
  private List<WorkerInfo> workerInfoList = Lists.newArrayList();
  /** Have the partition exchange children (workers) changed? */
  private final BspEvent partitionExchangeChildrenChanged;

  /** Worker Context */
  private final WorkerContext workerContext;

  /** Handler for aggregators */
  private final WorkerAggregatorHandler aggregatorHandler;

  /** Superstep output */
  private SuperstepOutput<I, V, E> superstepOutput;

  /** array of observers to call back to */
  private final WorkerObserver[] observers;
  /** Writer for worker progress */
  private final WorkerProgressWriter workerProgressWriter;

  /** YH: Async configuration. */
  private final AsyncConfiguration asyncConf;
  /**
   * YH: Number of supersteps to hold global token for.
   * Global token is revoked when this is 0.
   */
  private int superstepsUntilTokenRevoke;

  /** YH: Vertex type store/tracker for token serializability */
  private VertexTypeStore<I, V, E> vertexTypeStore;
  /** YH: Philosophers table for vertex-based dist locking serializability */
  private VertexPhilosophersTable<I, V, E> vertexPTable;
  /** YH: Philosophers table for partition-base dist locking */
  private PartitionPhilosophersTable<I, V, E> partitionPTable;

  // Per-Superstep Metrics
  /** Timer for WorkerContext#postSuperstep */
  private GiraphTimer wcPostSuperstepTimer;
  /** Time spent waiting on requests to finish */
  private GiraphTimer waitRequestsTimer;

  /**
   * Constructor for setting up the worker.
   *
   * @param context Mapper context
   * @param graphTaskManager GraphTaskManager for this compute node
   * @throws IOException
   * @throws InterruptedException
   */
  public BspServiceWorker(
    Mapper<?, ?, ?, ?>.Context context,
    GraphTaskManager<I, V, E> graphTaskManager)
    throws IOException, InterruptedException {
    super(context, graphTaskManager);
    ImmutableClassesGiraphConfiguration<I, V, E> conf = getConfiguration();
    localData = new LocalData<>(conf);
    translateEdge = getConfiguration().edgeTranslationInstance();
    if (translateEdge != null) {
      translateEdge.initialize(this);
    }
    partitionExchangeChildrenChanged = new PredicateLock(context);
    registerBspEvent(partitionExchangeChildrenChanged);
    workerGraphPartitioner =
        getGraphPartitionerFactory().createWorkerGraphPartitioner();
    workerInfo = new WorkerInfo();
    workerServer = new NettyWorkerServer<I, V, E>(conf, this, context);
    workerInfo.setInetSocketAddress(workerServer.getMyAddress());
    workerInfo.setTaskId(getTaskPartition());
    workerClient = new NettyWorkerClient<I, V, E>(context, conf, this);

    workerAggregatorRequestProcessor =
        new NettyWorkerAggregatorRequestProcessor(getContext(), conf, this);

    aggregatorHandler = new WorkerAggregatorHandler(this, conf, context);

    workerContext = conf.createWorkerContext();
    workerContext.setWorkerAggregatorUsage(aggregatorHandler);

    superstepOutput = conf.createSuperstepOutput(context);

    if (conf.isJMapHistogramDumpEnabled()) {
      conf.addWorkerObserverClass(JMapHistoDumper.class);
    }
    observers = conf.createWorkerObservers();

    WorkerProgress.get().setTaskId(getTaskPartition());
    workerProgressWriter = conf.trackJobProgressOnClient() ?
        new WorkerProgressWriter(myProgressPath, getZkExt()) : null;

    GiraphMetrics.get().addSuperstepResetObserver(this);

    asyncConf = conf.getAsyncConf();

    if (asyncConf.tokenSerialized()) {
      vertexTypeStore = new VertexTypeStore<I, V, E>(conf, this);
    } else if (asyncConf.vertexLockSerialized()) {
      vertexPTable = new VertexPhilosophersTable<I, V, E>(conf, this);
    } else if (asyncConf.partitionLockSerialized()) {
      partitionPTable = new PartitionPhilosophersTable<I, V, E>(conf, this);
    }
  }

  @Override
  public void newSuperstep(SuperstepMetricsRegistry superstepMetrics) {
    waitRequestsTimer = new GiraphTimer(superstepMetrics,
        TIMER_WAIT_REQUESTS, TimeUnit.MICROSECONDS);
    wcPostSuperstepTimer = new GiraphTimer(superstepMetrics,
        "worker-context-post-superstep", TimeUnit.MICROSECONDS);
  }

  @Override
  public WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public WorkerObserver[] getWorkerObservers() {
    return observers;
  }

  @Override
  public WorkerClient<I, V, E> getWorkerClient() {
    return workerClient;
  }

  public LocalData<I, V, E, ? extends Writable> getLocalData() {
    return localData;
  }

  public TranslateEdge<I, E> getTranslateEdge() {
    return translateEdge;
  }

  /**
   * Intended to check the health of the node.  For instance, can it ssh,
   * dmesg, etc. For now, does nothing.
   * TODO: Make this check configurable by the user (i.e. search dmesg for
   * problems).
   *
   * @return True if healthy (always in this case).
   */
  public boolean isHealthy() {
    return true;
  }

  /**
   * Load the vertices/edges from input slits. Do this until all the
   * InputSplits have been processed.
   * All workers will try to do as many InputSplits as they can.  The master
   * will monitor progress and stop this once all the InputSplits have been
   * loaded and check-pointed.  Keep track of the last input split path to
   * ensure the input split cache is flushed prior to marking the last input
   * split complete.
   *
   * Use one or more threads to do the loading.
   *
   * @param inputSplitPathList List of input split paths
   * @param inputSplitsCallableFactory Factory for {@link InputSplitsCallable}s
   * @return Statistics of the vertices and edges loaded
   * @throws InterruptedException
   * @throws KeeperException
   */
  private VertexEdgeCount loadInputSplits(
      List<String> inputSplitPathList,
      CallableFactory<VertexEdgeCount> inputSplitsCallableFactory)
    throws KeeperException, InterruptedException {
    VertexEdgeCount vertexEdgeCount = new VertexEdgeCount();
    // Determine how many threads to use based on the number of input splits
    int maxInputSplitThreads = (inputSplitPathList.size() - 1) /
        getConfiguration().getMaxWorkers() + 1;
    int numThreads = Math.min(getConfiguration().getNumInputSplitsThreads(),
        maxInputSplitThreads);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadInputSplits: Using " + numThreads + " thread(s), " +
          "originally " + getConfiguration().getNumInputSplitsThreads() +
          " threads(s) for " + inputSplitPathList.size() + " total splits.");
    }

    List<VertexEdgeCount> results =
        ProgressableUtils.getResultsWithNCallables(inputSplitsCallableFactory,
            numThreads, "load-%d", getContext());
    for (VertexEdgeCount result : results) {
      vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(result);
    }

    workerClient.waitAllRequests();
    return vertexEdgeCount;
  }

  /**
   * Load the mapping entries from the user-defined
   * {@link org.apache.giraph.io.MappingReader}
   *
   * @return Count of mapping entries loaded
   */
  private Integer loadMapping() throws KeeperException,
    InterruptedException {
    List<String> inputSplitPathList =
        getZkExt().getChildrenExt(mappingInputSplitsPaths.getPath(),
        false, false, true);

    InputSplitPathOrganizer splitOrganizer =
        new InputSplitPathOrganizer(getZkExt(),
            inputSplitPathList, getWorkerInfo().getHostname(),
            getConfiguration().useInputSplitLocality());

    MappingInputSplitsCallableFactory<I, V, E, ? extends Writable>
        mappingInputSplitsCallableFactory =
        new MappingInputSplitsCallableFactory<>(
            getConfiguration().createWrappedMappingInputFormat(),
            splitOrganizer,
            getContext(),
            getConfiguration(),
            this,
            getZkExt());

    int entriesLoaded = 0;
    // Determine how many threads to use based on the number of input splits
    int maxInputSplitThreads = inputSplitPathList.size();
    int numThreads = Math.min(getConfiguration().getNumInputSplitsThreads(),
        maxInputSplitThreads);
    if (LOG.isInfoEnabled()) {
      LOG.info("loadInputSplits: Using " + numThreads + " thread(s), " +
          "originally " + getConfiguration().getNumInputSplitsThreads() +
          " threads(s) for " + inputSplitPathList.size() + " total splits.");
    }

    List<Integer> results =
        ProgressableUtils.getResultsWithNCallables(
            mappingInputSplitsCallableFactory,
            numThreads, "load-mapping-%d", getContext());
    for (Integer result : results) {
      entriesLoaded += result;
    }
    // after all threads finish loading - call postFilling
    localData.getMappingStore().postFilling();
    return entriesLoaded;
  }

  /**
   * Load the vertices from the user-defined
   * {@link org.apache.giraph.io.VertexReader}
   *
   * @return Count of vertices and edges loaded
   */
  private VertexEdgeCount loadVertices() throws KeeperException,
      InterruptedException {
    List<String> inputSplitPathList =
        getZkExt().getChildrenExt(vertexInputSplitsPaths.getPath(),
            false, false, true);

    InputSplitPathOrganizer splitOrganizer =
        new InputSplitPathOrganizer(getZkExt(),
            inputSplitPathList, getWorkerInfo().getHostname(),
            getConfiguration().useInputSplitLocality());
    InputSplitsHandler splitsHandler = new InputSplitsHandler(
        splitOrganizer,
        getZkExt(),
        getContext(),
        BspService.VERTEX_INPUT_SPLIT_RESERVED_NODE,
        BspService.VERTEX_INPUT_SPLIT_FINISHED_NODE);

    VertexInputSplitsCallableFactory<I, V, E> inputSplitsCallableFactory =
        new VertexInputSplitsCallableFactory<I, V, E>(
            getConfiguration().createWrappedVertexInputFormat(),
            getContext(),
            getConfiguration(),
            this,
            splitsHandler,
            getZkExt());

    return loadInputSplits(inputSplitPathList, inputSplitsCallableFactory);
  }

  /**
   * Load the edges from the user-defined
   * {@link org.apache.giraph.io.EdgeReader}.
   *
   * @return Number of edges loaded
   */
  private long loadEdges() throws KeeperException, InterruptedException {
    List<String> inputSplitPathList =
        getZkExt().getChildrenExt(edgeInputSplitsPaths.getPath(),
            false, false, true);

    InputSplitPathOrganizer splitOrganizer =
        new InputSplitPathOrganizer(getZkExt(),
            inputSplitPathList, getWorkerInfo().getHostname(),
            getConfiguration().useInputSplitLocality());
    InputSplitsHandler splitsHandler = new InputSplitsHandler(
        splitOrganizer,
        getZkExt(),
        getContext(),
        BspService.EDGE_INPUT_SPLIT_RESERVED_NODE,
        BspService.EDGE_INPUT_SPLIT_FINISHED_NODE);

    EdgeInputSplitsCallableFactory<I, V, E> inputSplitsCallableFactory =
        new EdgeInputSplitsCallableFactory<I, V, E>(
            getConfiguration().createWrappedEdgeInputFormat(),
            getContext(),
            getConfiguration(),
            this,
            splitsHandler,
            getZkExt());

    return loadInputSplits(inputSplitPathList, inputSplitsCallableFactory).
        getEdgeCount();
  }

  @Override
  public MasterInfo getMasterInfo() {
    return masterInfo;
  }

  @Override
  public List<WorkerInfo> getWorkerInfoList() {
    return workerInfoList;
  }

  /**
   * Ensure the input splits are ready for processing
   *
   * @param inputSplitPaths Input split paths
   * @param inputSplitEvents Input split events
   */
  private void ensureInputSplitsReady(InputSplitPaths inputSplitPaths,
                                      InputSplitEvents inputSplitEvents) {
    while (true) {
      Stat inputSplitsReadyStat;
      try {
        inputSplitsReadyStat = getZkExt().exists(
            inputSplitPaths.getAllReadyPath(), true);
      } catch (KeeperException e) {
        throw new IllegalStateException("ensureInputSplitsReady: " +
            "KeeperException waiting on input splits", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException("ensureInputSplitsReady: " +
            "InterruptedException waiting on input splits", e);
      }
      if (inputSplitsReadyStat != null) {
        break;
      }
      inputSplitEvents.getAllReadyChanged().waitForever();
      inputSplitEvents.getAllReadyChanged().reset();
    }
  }

  /**
   * Mark current worker as done and then wait for all workers
   * to finish processing input splits.
   *
   * @param inputSplitPaths Input split paths
   * @param inputSplitEvents Input split events
   */
  private void markCurrentWorkerDoneThenWaitForOthers(
    InputSplitPaths inputSplitPaths,
    InputSplitEvents inputSplitEvents) {
    String workerInputSplitsDonePath =
        inputSplitPaths.getDonePath() + "/" +
            getWorkerInfo().getHostnameId();
    try {
      getZkExt().createExt(workerInputSplitsDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "markCurrentWorkerDoneThenWaitForOthers: " +
          "KeeperException creating worker done splits", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "markCurrentWorkerDoneThenWaitForOthers: " +
          "InterruptedException creating worker done splits", e);
    }
    while (true) {
      Stat inputSplitsDoneStat;
      try {
        inputSplitsDoneStat =
            getZkExt().exists(inputSplitPaths.getAllDonePath(),
                true);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "markCurrentWorkerDoneThenWaitForOthers: " +
            "KeeperException waiting on worker done splits", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "markCurrentWorkerDoneThenWaitForOthers: " +
            "InterruptedException waiting on worker done splits", e);
      }
      if (inputSplitsDoneStat != null) {
        break;
      }
      inputSplitEvents.getAllDoneChanged().waitForever();
      inputSplitEvents.getAllDoneChanged().reset();
    }
  }

  @Override
  public FinishedSuperstepStats setup() {
    // Unless doing a restart, prepare for computation:
    // 1. Start superstep INPUT_SUPERSTEP (no computation)
    // 2. Wait until the INPUT_SPLIT_ALL_READY_PATH node has been created
    // 3. Process input splits until there are no more.
    // 4. Wait until the INPUT_SPLIT_ALL_DONE_PATH node has been created
    // 5. Process any mutations deriving from add edge requests
    // 6. Wait for superstep INPUT_SUPERSTEP to complete.
    if (getRestartedSuperstep() != UNSET_SUPERSTEP) {
      setCachedSuperstep(getRestartedSuperstep());
      return new FinishedSuperstepStats(0, false, 0, 0, true);
    }

    JSONObject jobState = getJobState();
    if (jobState != null) {
      try {
        if ((ApplicationState.valueOf(jobState.getString(JSONOBJ_STATE_KEY)) ==
            ApplicationState.START_SUPERSTEP) &&
            jobState.getLong(JSONOBJ_SUPERSTEP_KEY) ==
            getSuperstep()) {
          if (LOG.isInfoEnabled()) {
            LOG.info("setup: Restarting from an automated " +
                "checkpointed superstep " +
                getSuperstep() + ", attempt " +
                getApplicationAttempt());
          }
          setRestartedSuperstep(getSuperstep());
          return new FinishedSuperstepStats(0, false, 0, 0, true);
        }
      } catch (JSONException e) {
        throw new RuntimeException(
            "setup: Failed to get key-values from " +
                jobState.toString(), e);
      }
    }

    // Add the partitions that this worker owns
    Collection<? extends PartitionOwner> masterSetPartitionOwners =
        startSuperstep();
    workerGraphPartitioner.updatePartitionOwners(
        getWorkerInfo(), masterSetPartitionOwners, getPartitionStore());

    /*if[HADOOP_NON_SECURE]
      workerClient.setup();
    else[HADOOP_NON_SECURE]*/
    workerClient.setup(getConfiguration().authenticate());
    /*end[HADOOP_NON_SECURE]*/

    // Initialize aggregator at worker side during setup.
    // Do this just before vertex and edge loading.
    aggregatorHandler.prepareSuperstep(workerAggregatorRequestProcessor);

    VertexEdgeCount vertexEdgeCount;
    int entriesLoaded = 0;

    if (getConfiguration().hasMappingInputFormat()) {
      // Ensure the mapping InputSplits are ready for processing
      ensureInputSplitsReady(mappingInputSplitsPaths, mappingInputSplitsEvents);
      getContext().progress();
      try {
        entriesLoaded = loadMapping();
        // successfully loaded mapping
        // now initialize graphPartitionerFactory with this data
        getGraphPartitionerFactory().initialize(localData);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadMapping failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadMapping failed with KeeperException", e);
      }
      getContext().progress();
      if (LOG.isInfoEnabled()) {
        LOG.info("setup: Finally loaded a total of " +
            entriesLoaded + " entries from inputSplits");
      }

      // Workers wait for each other to finish, coordinated by master
      markCurrentWorkerDoneThenWaitForOthers(mappingInputSplitsPaths,
          mappingInputSplitsEvents);
      // Print stats for data stored in localData once mapping is fully
      // loaded on all the workers
      localData.printStats();
    }

    // YH: we only use vertex input formats, so rest are irrelevant
    if (getConfiguration().hasVertexInputFormat()) {
      // Ensure the vertex InputSplits are ready for processing
      ensureInputSplitsReady(vertexInputSplitsPaths, vertexInputSplitsEvents);
      getContext().progress();
      try {
        vertexEdgeCount = loadVertices();
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadVertices failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadVertices failed with KeeperException", e);
      }
      getContext().progress();
    } else {
      vertexEdgeCount = new VertexEdgeCount();
    }
    WorkerProgress.get().finishLoadingVertices();

    if (getConfiguration().hasEdgeInputFormat()) {
      // Ensure the edge InputSplits are ready for processing
      ensureInputSplitsReady(edgeInputSplitsPaths, edgeInputSplitsEvents);
      getContext().progress();
      try {
        vertexEdgeCount = vertexEdgeCount.incrVertexEdgeCount(0, loadEdges());
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "setup: loadEdges failed with InterruptedException", e);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "setup: loadEdges failed with KeeperException", e);
      }
      getContext().progress();
    }
    WorkerProgress.get().finishLoadingEdges();

    if (LOG.isInfoEnabled()) {
      LOG.info("setup: Finally loaded a total of " + vertexEdgeCount);
    }

    if (getConfiguration().hasVertexInputFormat()) {
      // Workers wait for each other to finish, coordinated by master
      markCurrentWorkerDoneThenWaitForOthers(vertexInputSplitsPaths,
          vertexInputSplitsEvents);
    }

    if (getConfiguration().hasEdgeInputFormat()) {
      // Workers wait for each other to finish, coordinated by master
      markCurrentWorkerDoneThenWaitForOthers(edgeInputSplitsPaths,
          edgeInputSplitsEvents);
    }

    // Create remaining partitions owned by this worker.
    for (PartitionOwner partitionOwner : masterSetPartitionOwners) {
      if (partitionOwner.getWorkerInfo().equals(getWorkerInfo()) &&
          !getPartitionStore().hasPartition(
              partitionOwner.getPartitionId())) {
        Partition<I, V, E> partition =
            getConfiguration().createPartition(
                partitionOwner.getPartitionId(), getContext());
        getPartitionStore().addPartition(partition);
      }
    }

    if (getConfiguration().hasEdgeInputFormat()) {
      // Move edges from temporary storage to their source vertices.
      getServerData().getEdgeStore().moveEdgesToVertices();
    }

    localData.removeMappingStoreIfPossible();

    // Generate the partition stats for the input superstep and process
    // if necessary
    List<PartitionStats> partitionStatsList =
        new ArrayList<PartitionStats>();
    for (Integer partitionId : getPartitionStore().getPartitionIds()) {
      Partition<I, V, E> partition =
          getPartitionStore().getOrCreatePartition(partitionId);
      PartitionStats partitionStats =
          new PartitionStats(partition.getId(),
              partition.getVertexCount(),
              0,
              partition.getEdgeCount(),
              0, 0);
      partitionStatsList.add(partitionStats);
      getPartitionStore().putPartition(partition);
    }
    workerGraphPartitioner.finalizePartitionStats(
        partitionStatsList, getPartitionStore());

    // YH: At this point, all input splits are loaded and all workers
    // have their correct partitions/vertices.
    //
    // If doing serializability, need to send in-edge dependencies
    // for directed graphs. Must be performed after all workers have
    // their correct partitions/vertices. Only needs to be done once.
    if (asyncConf.tokenSerialized()) {
      vertexTypeStore.sendDependencies();
    } else if (asyncConf.vertexLockSerialized()) {
      vertexPTable.sendDependencies();
    } else if (asyncConf.partitionLockSerialized()) {
      partitionPTable.sendDependencies();
    }

    return finishSuperstep(partitionStatsList, null);
  }

  /**
   * Register the health of this worker for a given superstep
   *
   * @param superstep Superstep to register health on
   */
  private void registerHealth(long superstep) {
    JSONArray hostnamePort = new JSONArray();
    hostnamePort.put(getHostname());

    hostnamePort.put(workerInfo.getPort());

    String myHealthPath = null;
    if (isHealthy()) {
      myHealthPath = getWorkerInfoHealthyPath(getApplicationAttempt(),
          getSuperstep());
    } else {
      myHealthPath = getWorkerInfoUnhealthyPath(getApplicationAttempt(),
          getSuperstep());
    }
    myHealthPath = myHealthPath + "/" + workerInfo.getHostnameId();
    try {
      myHealthZnode = getZkExt().createExt(
          myHealthPath,
          WritableUtils.writeToByteArray(workerInfo),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("registerHealth: myHealthPath already exists (likely " +
          "from previous failure): " + myHealthPath +
          ".  Waiting for change in attempts " +
          "to re-join the application");
      getApplicationAttemptChangedEvent().waitForever();
      if (LOG.isInfoEnabled()) {
        LOG.info("registerHealth: Got application " +
            "attempt changed event, killing self");
      }
      throw new IllegalStateException(
          "registerHealth: Trying " +
              "to get the new application attempt by killing self", e);
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + myHealthPath +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " + myHealthPath +
          " failed with InterruptedException", e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("registerHealth: Created my health node for attempt=" +
          getApplicationAttempt() + ", superstep=" +
          getSuperstep() + " with " + myHealthZnode +
          " and workerInfo= " + workerInfo);
    }
  }

  /**
   * Do this to help notify the master quicker that this worker has failed.
   */
  private void unregisterHealth() {
    LOG.error("unregisterHealth: Got failure, unregistering health on " +
        myHealthZnode + " on superstep " + getSuperstep());
    try {
      getZkExt().deleteExt(myHealthZnode, -1, false);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "unregisterHealth: InterruptedException - Couldn't delete " +
              myHealthZnode, e);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "unregisterHealth: KeeperException - Couldn't delete " +
              myHealthZnode, e);
    }
  }

  @Override
  public void failureCleanup() {
    unregisterHealth();
  }

  @Override
  public Collection<? extends PartitionOwner> startSuperstep() {
    // Algorithm:
    // 1. Communication service will combine message from previous
    //    superstep
    // 2. Register my health for the next superstep.
    // 3. Wait until the partition assignment is complete and get it
    // 4. Get the aggregator values from the previous superstep

    // initialize timer for visualization
    if (asyncConf.printTiming() && getLogicalSuperstep() == 0) {
      LOG.info("[[__TIMING]] " + workerInfo.getTaskId() + " id [who_am_i]");
      START_TIME = System.nanoTime();
    }

    // debugging stats
    //if (getLogicalSuperstep() == 0) {
    //  if (asyncConf.tokenSerialized()) {
    //    //int numVertices = 0;
    //    //for (int i : getServerData().
    //    //       getPartitionStore().getPartitionIds()) {
    //    //  numVertices += getServerData().getPartitionStore().
    //    //    getOrCreatePartition(i).getVertexCount();
    //    //}
    //    vertexTypeStore.printAll();
    //  } else if (asyncConf.vertexLockSerialized()) {
    //    vertexPTable.printAll();
    //  } else if (asyncConf.partitionLockSerialized()) {
    //    partitionPTable.printAll();
    //  }
    //}

    //LOG.info("[[MST-internal]] ##START##: ss=" + getSuperstep() +
    //         ", lss=" + getLogicalSuperstep() +
    //         ", nb=" + asyncConf.needBarrier() +
    //         ", multi-phase=" + asyncConf.isMultiPhase() +
    //         ", currPhase=" + asyncConf.getCurrentPhase() +
    //         ", isNewPhase=" + asyncConf.isNewPhase());

    // YH: decrement revoke counter if needed
    if (asyncConf.tokenSerialized() && asyncConf.haveGlobalToken()) {
      superstepsUntilTokenRevoke--;
    }

    // YH: reset counter on new global superstep
    if (asyncConf.needBarrier()) {
      asyncConf.resetInFlightBytes();
    }

    if (getSuperstep() != INPUT_SUPERSTEP) {
      workerServer.prepareSuperstep();
    }

    // YH: SKIP getting assignments/rebalancing---master is unaware
    // of workers' logical supersteps. Also skip registering health,
    // as global SS has not changed (re-registering for same SS will
    // cause exception).
    //
    // Note that needBarrier() is set by finishSuperstep(). That is,
    // if barrier was deemed necessary in previous pseudo or non-pseudo
    // superstep, then it will be executed now. needBarrier() is also
    // "reset" in finishSuperstep() as needed.
    if (asyncConf.needBarrier()) {
      registerHealth(getSuperstep());

      String addressesAndPartitionsPath =
          getAddressesAndPartitionsPath(getApplicationAttempt(),
              getSuperstep());
      AddressesAndPartitionsWritable addressesAndPartitions =
          new AddressesAndPartitionsWritable(
              workerGraphPartitioner.createPartitionOwner().getClass());
      try {
        while (getZkExt().exists(addressesAndPartitionsPath, true) ==
            null) {
          getAddressesAndPartitionsReadyChangedEvent().waitForever();
          getAddressesAndPartitionsReadyChangedEvent().reset();
        }
        WritableUtils.readFieldsFromZnode(
            getZkExt(),
            addressesAndPartitionsPath,
            false,
            null,
            addressesAndPartitions);
      } catch (KeeperException e) {
        throw new IllegalStateException(
            "startSuperstep: KeeperException getting assignments", e);
      } catch (InterruptedException e) {
        throw new IllegalStateException(
            "startSuperstep: InterruptedException getting assignments", e);
      }

      workerInfoList.clear();
      workerInfoList = addressesAndPartitions.getWorkerInfos();
      masterInfo = addressesAndPartitions.getMasterInfo();

      if (LOG.isInfoEnabled()) {
        LOG.info("startSuperstep: " + masterInfo);
        LOG.info("startSuperstep: Ready for computation on superstep " +
            getSuperstep() + " (logical superstep " +
            getLogicalSuperstep() + ") since worker " +
            "selection and vertex range assignments are done in " +
            addressesAndPartitionsPath);
      }

      getContext().setStatus("startSuperstep: " +
          getGraphTaskManager().getGraphFunctions().toString() +
          " - Attempt=" + getApplicationAttempt() +
          ", Superstep=" + getSuperstep() +
          " (logicalSuperstep=" + getLogicalSuperstep() + ")");

      if (LOG.isDebugEnabled()) {
        LOG.debug("startSuperstep: addressesAndPartitions" +
            addressesAndPartitions.getWorkerInfos());
        for (PartitionOwner partitionOwner : addressesAndPartitions
            .getPartitionOwners()) {
          LOG.debug(partitionOwner.getPartitionId() + " " +
              partitionOwner.getWorkerInfo());
        }
      }

      return addressesAndPartitions.getPartitionOwners();

    } else {
      // YH: some duplicate status-update code
      if (LOG.isInfoEnabled()) {
        LOG.info("startSuperstep: " + masterInfo);
        LOG.info("startSuperstep: Ready for computation on superstep " +
            getSuperstep() + " (logical superstep " +
            getLogicalSuperstep() + ") since we skipped assignments.");
      }

      getContext().setStatus("startSuperstep: " +
          getGraphTaskManager().getGraphFunctions().toString() +
          " - Attempt=" + getApplicationAttempt() +
          ", Superstep=" + getSuperstep() +
          " (logicalSuperstep=" + getLogicalSuperstep() + ")");

      // Return junk---GraphTaskManager ignores it.
      return null;
    }
  }

  // CHECKSTYLE: stop MethodLengthCheck
  @Override
  public FinishedSuperstepStats finishSuperstep(
      List<PartitionStats> partitionStatsList,
      GiraphTimerContext superstepTimerContext) {

    // This barrier blocks until success (or the master signals it to
    // restart).
    //
    // Master will coordinate the barriers and aggregate "doneness" of all
    // the vertices.  Each worker will:
    // 1. Ensure that the requests are complete
    // 2. Execute user postSuperstep() if necessary.
    // 3. Save aggregator values that are in use.
    // 4. Report the statistics (vertices, edges, messages, etc.)
    //    of this worker
    // 5. Let the master know it is finished.
    // 6. Wait for the master's superstep info, and check if done

    if (asyncConf.printTiming() &&
        getLogicalSuperstep() > INPUT_SUPERSTEP) {
      long elapsedTime = (System.nanoTime() - START_TIME) / 1000;

      // corresponding *_start is implied, so we leave it out
      // (*_start has same timestamp as the last *_end)
      LOG.info("[[__TIMING]] " + elapsedTime + " us [ss_end]");
    }

    long workerSentMessages = 0;
    long workerSentMessageBytes = 0;
    long localVertices = 0;
    long doneVertices = 0;
    for (PartitionStats partitionStats : partitionStatsList) {
      workerSentMessages += partitionStats.getMessagesSentCount();
      workerSentMessageBytes += partitionStats.getMessageBytesSentCount();
      localVertices += partitionStats.getVertexCount();
      doneVertices += partitionStats.getFinishedVertexCount();
    }

    // YH: only use in-flight bytes when we're beyond SS -1 (INPUT_SUPERSTEP)
    //
    // More generally, when barriers are disabled after SS X (X >= -1), we
    // can run into case where remote messages ARE received, but we can't
    // spend a logical SS to process them b/c another global barrier is needed
    // => master erroneously terminates.
    if (asyncConf.disableBarriers() &&
        getLogicalSuperstep() > INPUT_SUPERSTEP) {
      // YH: tracking sent bytes is well-supported, so batch increment
      // it here (avoids contention between compute threads)
      asyncConf.addSentBytes(workerSentMessageBytes);

      // Subtle race condition: a remote message can arrive at ANY time,
      // so we must treat it either as received but unprocessed OR
      // still in-flight. To do so, we must get the in-flight bytes
      // count (possibly stale) BEFORE checking remote message store.
      //
      // If message was *actually* received and we just missed seeing
      // that it was, then we treat it as in-flight to handle in the
      // next global superstep. If message was actually received and
      // we do see it, then we ignore in-flight bytes and spend another
      // logical superstep processing the message. Either way, it gets
      // processed (i.e., we don't erroneously terminate).
      workerSentMessageBytes = asyncConf.getInFlightBytes();
    }

    // YH: determine if global barriers are needed
    asyncConf.setNeedBarrier(
      needBarrier(workerSentMessages, localVertices, doneVertices));

    //LOG.info("[[MST-internal]] ##FINISH##: ss=" + getSuperstep() +
    //         ", lss=" + getLogicalSuperstep() +
    //         ", nb=" + asyncConf.needBarrier() +
    //         ", multi-phase=" + asyncConf.isMultiPhase() +
    //         ", currPhase=" + asyncConf.getCurrentPhase() +
    //         ", isNewPhase=" + asyncConf.isNewPhase());

    if (asyncConf.tokenSerialized()) {
      roundRobinTokens();
    }

    if (asyncConf.needBarrier()) {
      // YH: we count this as part of the global barrier synchronization
      // cost, since BAP avoids this code-path on logical supersteps.
      waitForRequestsToFinish();

      // YH: everything between [ss_end] and waitForRequestsToFinish() call
      // is negligible (few hundred us at worst), so just roll it into
      // "blocked-on-communication" time instead
      //
      // If BAP, this code path is skipped on a local barrier, so everything
      // after [ss_end] is properly accounted as local barrier cost.
      if (asyncConf.printTiming() &&
          getLogicalSuperstep() > INPUT_SUPERSTEP) {
        long elapsedTime = (System.nanoTime() - START_TIME) / 1000;
        LOG.info("[[__TIMING]] " + elapsedTime + " us [comm_block_end]");
      }

      // YH: if barriers are disabled and we are going to wait for
      // global superstep, approach "ready to sync" barrier first.
      // Skip this barrier if worker MUST have global barrier.
      //
      // Must place this here b/c:
      // (1) barrier must be AFTER all requests are ACKed by their destinations
      //     => this must be after waitForRequestsToFinish()
      // (2) post global-SS tasks must be done AFTER committing to wait
      //     at global barrier without leaving (here, workers can leave)
      //     => this must be before post-superstep/aggregator finish, etc.
      if (asyncConf.disableBarriers() &&
          getLogicalSuperstep() > INPUT_SUPERSTEP) {
        // if global tokens are needed, block on lightweight
        // barrier WITHOUT doing ZK stuff
        if (asyncConf.tokenSerialized() &&
            needGlobalToken(localVertices, doneVertices)) {
          waitForGlobalToken();
          asyncConf.setNeedBarrier(false);
        } else if (!waitForWorkersOrMessages(workerSentMessageBytes)) {
          asyncConf.setNeedBarrier(false);
        }

        // this captures how long worker is blocked without work to do
        // (BAP only), after resolving all open requests
        if (asyncConf.printTiming()) {
          long elapsedTime = (System.nanoTime() - START_TIME) / 1000;
          LOG.info("[[__TIMING]] " + elapsedTime +
                   " us [lightweight_barrier_end]");
        }
      }
    }

    getGraphTaskManager().notifyFinishedCommunication();

    // TODO-YH: post-superstep callbacks may not be safe??
    postSuperstepCallbacks();

    // YH: process aggregators globally or locally
    if (asyncConf.needBarrier()) {
      // YH: this is actually very sneaky! This function call will cause
      // the worker to BLOCK and wait for partial aggregators to be received
      // from all other workers---i.e., this is THE cause of the global
      // synchronization cost.
      //
      // By the time this function call returns, all the workers will
      // have arrived at the global barrier, making the ZK read and writes
      // (further down below) complete pretty quickly.
      aggregatorHandler.finishSuperstep(workerAggregatorRequestProcessor);
    } else {
      aggregatorHandler.finishLogicalSuperstep();
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Superstep " + getSuperstep() +
          " (logicalSuperstep " + getLogicalSuperstep() + ")" +
          ", messages = " + workerSentMessages + " " +
          ", message bytes = " + workerSentMessageBytes + " , " +
          MemoryUtils.getRuntimeMemoryStats());
    }

    if (superstepTimerContext != null) {
      superstepTimerContext.stop();
    }

    if (asyncConf.needBarrier()) {
      writeFinishedSuperstepInfoToZK(partitionStatsList,
        workerSentMessages, workerSentMessageBytes);

      LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
          "finishSuperstep: (waiting for rest " +
              "of workers) " +
              getGraphTaskManager().getGraphFunctions().toString() +
              " - Attempt=" + getApplicationAttempt() +
              ", Superstep=" + getSuperstep() +
              " (logicalSuperstep=" + getLogicalSuperstep() + ")");

      String superstepFinishedNode =
          getSuperstepFinishedPath(getApplicationAttempt(), getSuperstep());

      waitForOtherWorkers(superstepFinishedNode);

      GlobalStats globalStats = new GlobalStats();
      SuperstepClasses superstepClasses = new SuperstepClasses();
      WritableUtils.readFieldsFromZnode(
          getZkExt(), superstepFinishedNode, false, null, globalStats,
          superstepClasses);
      if (LOG.isInfoEnabled()) {
        LOG.info("finishSuperstep: Completed superstep " + getSuperstep() +
            " with global stats " + globalStats + " and classes " +
            superstepClasses);
      }
      getContext().setStatus("finishSuperstep: (all workers done) " +
          getGraphTaskManager().getGraphFunctions().toString() +
          " - Attempt=" + getApplicationAttempt() +
          ", Superstep=" + getSuperstep() +
          " (logicalSuperstep=" + getLogicalSuperstep() + ")");

      incrCachedSuperstep();
      getConfiguration().updateSuperstepClasses(superstepClasses);

      if (asyncConf.isMultiPhase()) {
        if (asyncConf.disableBarriers()) {
          // for BAP, global supersteps are equivalent to new phase,
          // since each phase is executed entirely in 1 global SS
          asyncConf.setNewPhase(true);
        } else {
          // for AP, have to look at global stats from master
          asyncConf.setNewPhase(globalStats.isNewPhase());
        }
      }

      // YH: edge-case for mutations in BAP. If algorithm terminates,
      // there may be pending mutations that will fail to get executed,
      // since startSuperstep() will never be called. This resolves that.
      if (asyncConf.disableBarriers() &&
          globalStats.getHaltComputation()) {
        workerServer.finishComputation();
      }

      // must keep this here, as end-of-computation is an edge case
      // that never touches startSuperstep() afterwards
      // (needs > 0 b/c superstep is incremented above)
      if (asyncConf.printTiming() && getLogicalSuperstep() > 0) {
        long elapsedTime = (System.nanoTime() - START_TIME) / 1000;
        LOG.info("[[__TIMING]] " + elapsedTime + " us [global_barrier_end]");
      }

      return new FinishedSuperstepStats(
          localVertices,
          globalStats.getHaltComputation(),
          globalStats.getVertexCount(),
          globalStats.getEdgeCount(),
          false);

    } else {
      // YH: if in logical superstep only, increment only "logicalSuperstep".
      // This enables "superstep" to be used for coordination (master
      // and ZK will just see it as a very long superstep), with minimal
      // code modifications.
      incrLogicalSuperstep();

      // no longer a new phase (AP never executes this code path)
      asyncConf.setNewPhase(false);

      // this will only occur when > INPUT_SUPERSTEP
      if (asyncConf.printTiming()) {
        long elapsedTime = (System.nanoTime() - START_TIME) / 1000;
        LOG.info("[[__TIMING]] " + elapsedTime + " us [local_barrier_end]");
      }

      // Return junk---GraphTaskManager ignores it.
      // Note that we CANNOT fake "halt" as that is a GLOBAL condition.
      return null;
    }
  }
  // CHECKSTYLE: resume MethodLengthCheck

  /**
   * YH: Returns whether global barrier is required.
   *
   * @param workerSentMessages Number of sent messages (worker)
   * @param localVertices Number of vertices (worker)
   * @param doneVertices Number of finished vertices (worker)
   * @return True if global barrier is required.
   */
  private boolean needBarrier(long workerSentMessages,
                              long localVertices, long doneVertices) {
    // Cannot disable global barrier if SS <= INPUT_SUPERSTEP,
    // as that is when message store is still uninitialized.
    if (asyncConf.disableBarriers() &&
        getLogicalSuperstep() > INPUT_SUPERSTEP) {

      // local and remote conditions that determine whether there is
      // more work to do (i.e., global barrier NOT needed)
      boolean haveLocalWork;
      boolean haveRemoteWork;

      if (getLogicalSuperstep() == 0 &&
          (asyncConf.tokenSerialized() ||
           asyncConf.vertexLockSerialized() ||
           asyncConf.partitionLockSerialized())) {
        // For token passing and dist locking, LSS/SS 0 is *reserved* for
        // initialization. This is purely for backwards compatibility: BSP algs
        // are coded under assumption that no messages are visible in SS 0.
        // By having this support, we can execute BSP algs with serializability
        // (although such algs do not need it for correctness).
        //
        // Consequently, there is always more local work to do.
        haveLocalWork = true;
      } else if (asyncConf.isMultiPhase() ||
                 (asyncConf.tokenSerialized() && !asyncConf.needAllMsgs())) {
        // For multi-phase computation, we must check only the message stores
        // for the current phase. Hence, we can't use workerSentMessages, since
        // that also captures messages sent for the next phase.
        //
        // For token serialized computations, local messages for local boundary
        // vertices do not get processed right away (it needs local token),
        // so workerSentMessages will miss these messages. There's more local
        // work to do b/c more LSSes will pass local token around.
        // (Only exception is when needAllMsgs is true.)
        haveLocalWork = getServerData().getLocalMessageStore().hasMessages();
      } else {
        // For single-phase algs, workerSentMessages is sufficient.
        // This is faster than checking local message store.
        //
        // Also works for serialized algs via dist locking b/c all
        // vertices execute once in each LSS, so its behaviour is
        // identical to non-serializable algs.
        //
        // NOTE: we do NOT need to check if all vertices are halted,
        // b/c if we have no messages, the vertices will have nothing
        // to work with---we're better off waiting in that case
        haveLocalWork = workerSentMessages > 0;
      }

      if (asyncConf.tokenSerialized() && !asyncConf.haveGlobalToken()) {
        // If need serializable execution, then remote messages are
        // ignored unless we are holding token. Remote messages can
        // arrive, b/c there is always one worker with token.
        //
        // Unlike local tokens, doing more LSSes will not cause global
        // token to appear.
        haveRemoteWork = false;
      } else if (asyncConf.needAllMsgs()) {
        // If alg always sends messages, remoteMessageStore.hasMessages()
        // will always return true (even if it has no actual new messages).
        //
        // Expensive to explicitly track arrival of "new" messages in
        // remote store, so instead we look at whether all vertices are
        // deactivated. Trick here is that vertex is immediately reactivated
        // upon message arrival and doneVertices counter is modified too.
        //
        // TODO-YH: implement immediate waking + doneVertices tweaking...
        //
        // Right now (with the above unimplemented), there's a slim chance
        // we may miss pending messages in remote message store. However,
        // we will almost certainly catch it after unblocking from the
        // lightweight barrier. Probability is low b/c it requires:
        // 1. all vertices to have voted to halt (tolerance-based termination)
        // 2. all remote messages are received AFTER the destination process
        //   should be awoken, but BEFORE partition stats are recorded
        //
        // (2) is generally very unlikely, because it requires that all
        // remote messages to this worker arrive in very particular time frame.
        // Additionally, it is highly unlikely for sender vertex to not have
        // local neighbours (or remote neighbours in other workers) that will
        // also fail to see its messages. Since all workers arrive on
        // lightweight barrier AFTER no more remote messages are in-transit,
        // it's unlikely we'll block past lightweight barrier.
        haveRemoteWork = localVertices != doneVertices;
      } else {
        // For multi-phase alg, this will only check remote store for
        // current phase (and not next phase).
        haveRemoteWork = getServerData().getRemoteMessageStore().hasMessages();
      }

      // If there is local or remote work to do, spend another
      // logical superstep to process/complete them.
      if (haveLocalWork || haveRemoteWork) {
        return false;
      }
    }

    // barrier needed by default
    return true;
  }

  /**
   * YH: Whether global token is needed (for serializability only).
   *
   * @param localVertices Number of vertices (worker)
   * @param doneVertices Number of finished vertices (worker)
   * @return True if global token is required.
   */
  private boolean needGlobalToken(long localVertices, long doneVertices) {
    // basically, if there's stuff that needs remote messages
    // or there are remote messages, then we need global token
    if (asyncConf.needAllMsgs()) {
      return localVertices != doneVertices;
    } else {
      return getServerData().getRemoteMessageStore().hasMessages();
    }
  }

  /**
   * YH: Passes local and global tokens around in round robin fashion.
   * Local tokens are moved every LSS, while global tokens are moved
   * every "num-partitions" LSS.
   */
  private void roundRobinTokens() {
    if (getLogicalSuperstep() == INPUT_SUPERSTEP) {
      // special case for initializing tokens
      WorkerInfo firstWorker = workerGraphPartitioner.getPartitionOwners().
        iterator().next().getWorkerInfo();
      if (workerInfo.equals(firstWorker)) {
        asyncConf.getGlobalToken();
      }
      // revoke global token after num-partition supersteps
      // (+1 to avoid off-by-one, since decrment is in startSuperstep())
      superstepsUntilTokenRevoke = getServerData().getPartitionStore().
        getNumPartitions() + 1;

      int firstPartitionId = getServerData().getPartitionStore().
        getPartitionIds().iterator().next();
      asyncConf.setLocalTokenHolder(firstPartitionId);

    } else {
      // Pass global token only after holding for some number of supersteps,
      // BUT do not hold it if BAP + this worker is going to block.
      //
      // Worker must hold global token for at least ONE entire SS/LSS, b/c
      // global token can arrive at any time when using async.
      if (asyncConf.haveGlobalToken() &&
          (superstepsUntilTokenRevoke == 0 ||
           (asyncConf.disableBarriers() && asyncConf.needBarrier()))) {
        WorkerInfo nextWorker = null;
        boolean getNextWorker = false;

        // scan through all partition owners until we find ourself,
        // then next non-matching worker is who we send token to
        for (PartitionOwner po : workerGraphPartitioner.getPartitionOwners()) {
          if (workerInfo.equals(po.getWorkerInfo())) {
            getNextWorker = true;
          } else if (getNextWorker) {
            nextWorker = po.getWorkerInfo();
            break;
          }
        }

        // wrap around (to first worker) if next worker wasn't found
        if (nextWorker == null) {
          nextWorker = workerGraphPartitioner.getPartitionOwners().
            iterator().next().getWorkerInfo();
        }

        // reset counter
        superstepsUntilTokenRevoke = getServerData().getPartitionStore().
          getNumPartitions() + 1;

        asyncConf.revokeGlobalToken();
        sendGlobalToken(nextWorker);

        // wait for all messages, including token hand-over, to send
        // (this ensures serializability)
        waitForRequestsToFinish();
      }

      // pass local token
      boolean getNextPartition = false;
      int nextPartitionId = -1;

      for (int i : getServerData().getPartitionStore().getPartitionIds()) {
        if (getNextPartition) {
          nextPartitionId = i;
          break;
        }
        if (asyncConf.haveLocalToken(i)) {
          getNextPartition = true;
        }
      }

      // wrap around
      if (nextPartitionId == -1) {
        nextPartitionId = getServerData().getPartitionStore().
          getPartitionIds().iterator().next();
      }
      asyncConf.setLocalTokenHolder(nextPartitionId);
    }
  }

  /**
   * YH: Sends global token to specified worker.
   *
   * @param workerInfo Worker to send global token to
   */
  private void sendGlobalToken(WorkerInfo workerInfo) {
    workerClient.sendWritableRequest(
      workerInfo.getTaskId(), new SendGlobalTokenRequest<I, V, E>());
  }


  /**
   * YH: Returns starting time of SS0 for timing purposes.
   *
   * @return Starting time of superstep 0 (nanoseconds)
   */
  public static long getSS0StartTime() {
    return START_TIME;
  }

  /**
   * Handle post-superstep callbacks
   */
  private void postSuperstepCallbacks() {
    GiraphTimerContext timerContext = wcPostSuperstepTimer.time();
    getWorkerContext().postSuperstep();
    timerContext.stop();
    getContext().progress();

    for (WorkerObserver obs : getWorkerObservers()) {
      // YH: use logical superstep
      obs.postSuperstep(getLogicalSuperstep());
      getContext().progress();
    }
  }

  /**
   * Wait for all the requests to finish.
   */
  private void waitForRequestsToFinish() {
    if (LOG.isInfoEnabled()) {
      LOG.info("finishSuperstep: Waiting on all requests, superstep " +
          getSuperstep() + " " +
          MemoryUtils.getRuntimeMemoryStats());
    }
    GiraphTimerContext timerContext = waitRequestsTimer.time();
    workerClient.waitAllRequests();
    timerContext.stop();
  }

  /**
   * YH: Create required ZK path and wait for all other workers to finish, but
   * exit wait state (and delete ZK path) if a remote message arrives.
   *
   * @param prevInFlightBytes Number of in-flight bytes before entering
   *                          this barrier.
   * @return True on successful wait. False if interrupted by message arrival.
   */
  private boolean waitForWorkersOrMessages(long prevInFlightBytes) {
    // ZK path to wait on (created by master)
    String superstepReadyToFinishNode =
      getSuperstepReadyToFinishPath(getApplicationAttempt(), getSuperstep());

    // create ZK path
    String finishedWorkerPath =
      getWorkerReadyToFinishPath(getApplicationAttempt(), getSuperstep()) +
      "/" + getHostnamePartitionId();

    try {
      // YH: "true" for recursive not really needed but false won't make it
      // any faster, so leave it to be safe
      getZkExt().createExt(finishedWorkerPath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("finishSuperstep: ready to finish worker path " +
               finishedWorkerPath + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with InterruptedException", e);
    }

    try {
      // YH: first, we check whether or not ZK node/path exists (this path
      // is created by master when all workers are present) and also register
      // a "watch". This tells ZK to notify this client when the watched path
      // is created/deleted/changed, etc. Then, we go ahead and wait on a
      // BspEvent, basically a condition var.
      //
      // When desired WatchEvent occurs, we can process it (in another thread)
      // by signalling the condition var that this thread is waiting on. Since
      // watches are one-time only, we don't need to explicitly remove it.
      while (getZkExt().exists(superstepReadyToFinishNode, true) == null) {
        // signalled in BspService#process OR upon message arrival
        getSuperstepReadyToFinishEvent().waitForever();
        // reset immediately, so we can capture any other events
        getSuperstepReadyToFinishEvent().reset();

        // Check if signal was due to message arrival, by checking if
        // in-flight bytes has decreased.
        // (it will never increase, b/c we're not sending any messages)
        //
        // If doing multi-phase computation, return ONLY if message is
        // for current phase. Do not return if it is for the next phase.
        if (asyncConf.getInFlightBytes() < prevInFlightBytes &&
            (!asyncConf.isMultiPhase() ||
             getServerData().getRemoteMessageStore().hasMessages())) {
          try {
            // YH: "true" for recursive not really needed but false
            // won't make it any faster, so leave it to be safe
            getZkExt().deleteExt(finishedWorkerPath, -1, true);

            // We are in this function because we don't need global
            // token. BUT, we just received remote message, so we
            // DO need global token now. Immediately block for
            // global token, since there is no local work to do.
            if (asyncConf.tokenSerialized() && !asyncConf.haveGlobalToken()) {
              waitForGlobalToken();
            }

            return false;

          } catch (KeeperException e) {
            throw new IllegalStateException("Deleting " + finishedWorkerPath +
                " failed with KeeperException", e);
          } catch (InterruptedException e) {
            throw new IllegalStateException("Deleting " + finishedWorkerPath +
                " failed with InterruptedException", e);
          }
        }

        // We are here because we don't need global token, so
        // unblock only to pass global token along.
        //
        // TODO-YH: For termination, workers will keep passing the
        // token around. The current naive approach relies on race
        // condition where, eventually, a worker will block on the
        // lightweigth barrier and miss seeing the token.
        // (This is less efficient than properly tracking completed
        // workers but does not affect correctness, since workers
        // that need the token will block in a different manner.)
        if (asyncConf.tokenSerialized() && asyncConf.haveGlobalToken()) {
          roundRobinTokens();
          // do not return, b/c we have no new work to do
        }
      }

      return true;

    } catch (KeeperException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal prepare-to-finish superstep " + getSuperstep(), e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal prepare-to-finish superstep " + getSuperstep(), e);
    }
  }

  /**
   * YH: Block until global token is received.
   */
  private void waitForGlobalToken() {
    // ZK path to wait on (created by master)
    String superstepReadyToFinishNode =
      getSuperstepReadyToFinishPath(getApplicationAttempt(), getSuperstep());

    try {
      // This path is created by master when all workers are present, but
      // since we never record to ZK that we're done, this is effectively
      // an indefinite block until we get the global token.
      while (getZkExt().exists(superstepReadyToFinishNode, true) == null) {
        getSuperstepReadyToFinishEvent().waitForever();
        getSuperstepReadyToFinishEvent().reset();

        // unblock if global token is received
        if (asyncConf.haveGlobalToken()) {
          return;
        }
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal prepare-to-finish superstep " + getSuperstep(), e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal prepare-to-finish superstep " + getSuperstep(), e);
    }
  }

  /**
   * Wait for all the other Workers to finish the superstep.
   *
   * @param superstepFinishedNode ZooKeeper path to wait on.
   */
  private void waitForOtherWorkers(String superstepFinishedNode) {
    try {
      while (getZkExt().exists(superstepFinishedNode, true) == null) {
        getSuperstepFinishedEvent().waitForever();
        getSuperstepFinishedEvent().reset();
      }
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal completion of superstep " + getSuperstep(), e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "finishSuperstep: Failed while waiting for master to " +
              "signal completion of superstep " + getSuperstep(), e);
    }
  }

  /**
   * Write finished superstep info to ZooKeeper.
   *
   * @param partitionStatsList List of partition stats from superstep.
   * @param workerSentMessages Number of messages sent in superstep.
   * @param workerSentMessageBytes Number of message bytes sent
   *                               in superstep.
   */
  private void writeFinishedSuperstepInfoToZK(
      List<PartitionStats> partitionStatsList, long workerSentMessages,
      long workerSentMessageBytes) {
    Collection<PartitionStats> finalizedPartitionStats =
        workerGraphPartitioner.finalizePartitionStats(
            partitionStatsList, getPartitionStore());
    List<PartitionStats> finalizedPartitionStatsList =
        new ArrayList<PartitionStats>(finalizedPartitionStats);
    byte[] partitionStatsBytes =
        WritableUtils.writeListToByteArray(finalizedPartitionStatsList);
    WorkerSuperstepMetrics metrics = new WorkerSuperstepMetrics();
    metrics.readFromRegistry();
    byte[] metricsBytes = WritableUtils.writeToByteArray(metrics);

    JSONObject workerFinishedInfoObj = new JSONObject();
    try {
      workerFinishedInfoObj.put(JSONOBJ_PARTITION_STATS_KEY,
          Base64.encodeBytes(partitionStatsBytes));
      workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGES_KEY, workerSentMessages);
      workerFinishedInfoObj.put(JSONOBJ_NUM_MESSAGE_BYTES_KEY,
        workerSentMessageBytes);
      workerFinishedInfoObj.put(JSONOBJ_METRICS_KEY,
          Base64.encodeBytes(metricsBytes));
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    String finishedWorkerPath =
        getWorkerFinishedPath(getApplicationAttempt(), getSuperstep()) +
        "/" + getHostnamePartitionId();
    try {
      getZkExt().createExt(finishedWorkerPath,
          workerFinishedInfoObj.toString().getBytes(Charset.defaultCharset()),
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("finishSuperstep: finished worker path " +
          finishedWorkerPath + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " + finishedWorkerPath +
          " failed with InterruptedException", e);
    }
  }

  /**
   * Save the vertices using the user-defined VertexOutputFormat from our
   * vertexArray based on the split.
   *
   * @param numLocalVertices Number of local vertices
   * @throws InterruptedException
   */
  private void saveVertices(long numLocalVertices) throws IOException,
      InterruptedException {
    ImmutableClassesGiraphConfiguration<I, V, E>  conf = getConfiguration();

    if (conf.getVertexOutputFormatClass() == null) {
      LOG.warn("saveVertices: " +
          GiraphConstants.VERTEX_OUTPUT_FORMAT_CLASS +
          " not specified -- there will be no saved output");
      return;
    }
    if (conf.doOutputDuringComputation()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("saveVertices: The option for doing output during " +
            "computation is selected, so there will be no saving of the " +
            "output in the end of application");
      }
      return;
    }

    final int numPartitions = getPartitionStore().getNumPartitions();
    int numThreads = Math.min(getConfiguration().getNumOutputThreads(),
        numPartitions);
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveVertices: Starting to save " + numLocalVertices + " vertices " +
            "using " + numThreads + " threads");
    final VertexOutputFormat<I, V, E> vertexOutputFormat =
        getConfiguration().createWrappedVertexOutputFormat();

    final Queue<Integer> partitionIdQueue =
        (numPartitions == 0) ? new LinkedList<Integer>() :
            new ArrayBlockingQueue<Integer>(numPartitions);
    Iterables.addAll(partitionIdQueue, getPartitionStore().getPartitionIds());

    long verticesToStore = 0;
    PartitionStore<I, V, E> partitionStore = getPartitionStore();
    for (int partitionId : partitionStore.getPartitionIds()) {
      Partition<I, V, E> partition =
        partitionStore.getOrCreatePartition(partitionId);
      verticesToStore += partition.getVertexCount();
      partitionStore.putPartition(partition);
    }
    WorkerProgress.get().startStoring(
        verticesToStore, getPartitionStore().getNumPartitions());

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          /** How often to update WorkerProgress */
          private static final long VERTICES_TO_UPDATE_PROGRESS = 100000;

          @Override
          public Void call() throws Exception {
            VertexWriter<I, V, E> vertexWriter =
                vertexOutputFormat.createVertexWriter(getContext());
            vertexWriter.setConf(getConfiguration());
            vertexWriter.initialize(getContext());
            long nextPrintVertices = 0;
            long nextUpdateProgressVertices = 0;
            long nextPrintMsecs = System.currentTimeMillis() + 15000;
            int partitionIndex = 0;
            int numPartitions = getPartitionStore().getNumPartitions();
            while (!partitionIdQueue.isEmpty()) {
              Integer partitionId = partitionIdQueue.poll();
              if (partitionId == null) {
                break;
              }

              Partition<I, V, E> partition =
                  getPartitionStore().getOrCreatePartition(partitionId);
              long verticesWritten = 0;
              for (Vertex<I, V, E> vertex : partition) {
                vertexWriter.writeVertex(vertex);
                ++verticesWritten;

                // Update status at most every 250k vertices or 15 seconds
                if (verticesWritten > nextPrintVertices &&
                    System.currentTimeMillis() > nextPrintMsecs) {
                  LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
                      "saveVertices: Saved " + verticesWritten + " out of " +
                          partition.getVertexCount() + " partition vertices, " +
                          "on partition " + partitionIndex +
                          " out of " + numPartitions);
                  nextPrintMsecs = System.currentTimeMillis() + 15000;
                  nextPrintVertices = verticesWritten + 250000;
                }

                if (verticesWritten >= nextUpdateProgressVertices) {
                  WorkerProgress.get().addVerticesStored(
                      VERTICES_TO_UPDATE_PROGRESS);
                  nextUpdateProgressVertices += VERTICES_TO_UPDATE_PROGRESS;
                }
              }
              getPartitionStore().putPartition(partition);
              ++partitionIndex;
              WorkerProgress.get().addVerticesStored(
                  verticesWritten % VERTICES_TO_UPDATE_PROGRESS);
              WorkerProgress.get().incrementPartitionsStored();
            }
            vertexWriter.close(getContext()); // the temp results are saved now
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "save-vertices-%d", getContext());

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
      "saveVertices: Done saving vertices.");
    // YARN: must complete the commit the "task" output, Hadoop isn't there.
    if (getConfiguration().isPureYarnJob() &&
      getConfiguration().getVertexOutputFormatClass() != null) {
      try {
        OutputCommitter outputCommitter =
          vertexOutputFormat.getOutputCommitter(getContext());
        if (outputCommitter.needsTaskCommit(getContext())) {
          LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
            "OutputCommitter: committing task output.");
          // transfer from temp dirs to "task commit" dirs to prep for
          // the master's OutputCommitter#commitJob(context) call to finish.
          outputCommitter.commitTask(getContext());
        }
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while attempting to obtain " +
          "OutputCommitter.", ie);
      } catch (IOException ioe) {
        LOG.error("Master task's attempt to commit output has " +
          "FAILED.", ioe);
      }
    }
  }

  /**
   * Save the edges using the user-defined EdgeOutputFormat from our
   * vertexArray based on the split.
   *
   * @throws InterruptedException
   */
  private void saveEdges() throws IOException, InterruptedException {
    final ImmutableClassesGiraphConfiguration<I, V, E>  conf =
      getConfiguration();

    if (conf.getEdgeOutputFormatClass() == null) {
      LOG.warn("saveEdges: " +
               GiraphConstants.EDGE_OUTPUT_FORMAT_CLASS +
               "Make sure that the EdgeOutputFormat is not required.");
      return;
    }

    final int numPartitions = getPartitionStore().getNumPartitions();
    int numThreads = Math.min(conf.getNumOutputThreads(),
        numPartitions);
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "saveEdges: Starting to save the edges using " +
        numThreads + " threads");
    final EdgeOutputFormat<I, V, E> edgeOutputFormat =
        conf.createWrappedEdgeOutputFormat();

    final Queue<Integer> partitionIdQueue =
        (numPartitions == 0) ? new LinkedList<Integer>() :
            new ArrayBlockingQueue<Integer>(numPartitions);
    Iterables.addAll(partitionIdQueue, getPartitionStore().getPartitionIds());

    CallableFactory<Void> callableFactory = new CallableFactory<Void>() {
      @Override
      public Callable<Void> newCallable(int callableId) {
        return new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            EdgeWriter<I, V, E>  edgeWriter =
                edgeOutputFormat.createEdgeWriter(getContext());
            edgeWriter.setConf(conf);
            edgeWriter.initialize(getContext());

            long nextPrintVertices = 0;
            long nextPrintMsecs = System.currentTimeMillis() + 15000;
            int partitionIndex = 0;
            int numPartitions = getPartitionStore().getNumPartitions();
            while (!partitionIdQueue.isEmpty()) {
              Integer partitionId = partitionIdQueue.poll();
              if (partitionId == null) {
                break;
              }

              Partition<I, V, E> partition =
                  getPartitionStore().getOrCreatePartition(partitionId);
              long vertices = 0;
              long edges = 0;
              long partitionEdgeCount = partition.getEdgeCount();
              for (Vertex<I, V, E> vertex : partition) {
                for (Edge<I, E> edge : vertex.getEdges()) {
                  edgeWriter.writeEdge(vertex.getId(), vertex.getValue(), edge);
                  ++edges;
                }
                ++vertices;

                // Update status at most every 250k vertices or 15 seconds
                if (vertices > nextPrintVertices &&
                    System.currentTimeMillis() > nextPrintMsecs) {
                  LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
                      "saveEdges: Saved " + edges +
                      " edges out of " + partitionEdgeCount +
                      " partition edges, on partition " + partitionIndex +
                      " out of " + numPartitions);
                  nextPrintMsecs = System.currentTimeMillis() + 15000;
                  nextPrintVertices = vertices + 250000;
                }
              }
              getPartitionStore().putPartition(partition);
              ++partitionIndex;
            }
            edgeWriter.close(getContext()); // the temp results are saved now
            return null;
          }
        };
      }
    };
    ProgressableUtils.getResultsWithNCallables(callableFactory, numThreads,
        "save-vertices-%d", getContext());

    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
      "saveEdges: Done saving edges.");
    // YARN: must complete the commit the "task" output, Hadoop isn't there.
    if (conf.isPureYarnJob() &&
      conf.getVertexOutputFormatClass() != null) {
      try {
        OutputCommitter outputCommitter =
          edgeOutputFormat.getOutputCommitter(getContext());
        if (outputCommitter.needsTaskCommit(getContext())) {
          LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
            "OutputCommitter: committing task output.");
          // transfer from temp dirs to "task commit" dirs to prep for
          // the master's OutputCommitter#commitJob(context) call to finish.
          outputCommitter.commitTask(getContext());
        }
      } catch (InterruptedException ie) {
        LOG.error("Interrupted while attempting to obtain " +
          "OutputCommitter.", ie);
      } catch (IOException ioe) {
        LOG.error("Master task's attempt to commit output has " +
          "FAILED.", ioe);
      }
    }
  }

  @Override
  public void cleanup(FinishedSuperstepStats finishedSuperstepStats)
    throws IOException, InterruptedException {
    workerClient.closeConnections();
    setCachedSuperstep(getSuperstep() - 1);
    saveVertices(finishedSuperstepStats.getLocalVertexCount());
    saveEdges();
    WorkerProgress.get().finishStoring();
    if (workerProgressWriter != null) {
      workerProgressWriter.stop();
    }
    getPartitionStore().shutdown();
    // All worker processes should denote they are done by adding special
    // znode.  Once the number of znodes equals the number of partitions
    // for workers and masters, the master will clean up the ZooKeeper
    // znodes associated with this job.
    String workerCleanedUpPath = cleanedUpPath  + "/" +
        getTaskPartition() + WORKER_SUFFIX;
    try {
      String finalFinishedPath =
          getZkExt().createExt(workerCleanedUpPath,
              null,
              Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT,
              true);
      if (LOG.isInfoEnabled()) {
        LOG.info("cleanup: Notifying master its okay to cleanup with " +
            finalFinishedPath);
      }
    } catch (KeeperException.NodeExistsException e) {
      if (LOG.isInfoEnabled()) {
        LOG.info("cleanup: Couldn't create finished node '" +
            workerCleanedUpPath);
      }
    } catch (KeeperException e) {
      // Cleaning up, it's okay to fail after cleanup is successful
      LOG.error("cleanup: Got KeeperException on notification " +
          "to master about cleanup", e);
    } catch (InterruptedException e) {
      // Cleaning up, it's okay to fail after cleanup is successful
      LOG.error("cleanup: Got InterruptedException on notification " +
          "to master about cleanup", e);
    }
    try {
      getZkExt().close();
    } catch (InterruptedException e) {
      // cleanup phase -- just log the error
      LOG.error("cleanup: Zookeeper failed to close with " + e);
    }

    if (getConfiguration().metricsEnabled()) {
      GiraphMetrics.get().dumpToStream(System.err);
    }

    // Preferably would shut down the service only after
    // all clients have disconnected (or the exceptions on the
    // client side ignored).
    workerServer.close();
  }

  @Override
  public void storeCheckpoint() throws IOException {
    LoggerUtils.setStatusAndLog(getContext(), LOG, Level.INFO,
        "storeCheckpoint: Starting checkpoint " +
            getGraphTaskManager().getGraphFunctions().toString() +
            " - Attempt=" + getApplicationAttempt() +
            ", Superstep=" + getSuperstep());

    // Algorithm:
    // For each partition, dump vertices and messages
    Path metadataFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_METADATA_POSTFIX);
    Path verticesFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_VERTICES_POSTFIX);
    Path validFilePath =
        new Path(getCheckpointBasePath(getSuperstep()) + "." +
            getHostnamePartitionId() +
            CHECKPOINT_VALID_POSTFIX);

    // Remove these files if they already exist (shouldn't though, unless
    // of previous failure of this worker)
    if (getFs().delete(validFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed valid file " +
          validFilePath);
    }
    if (getFs().delete(metadataFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed metadata file " +
          metadataFilePath);
    }
    if (getFs().delete(verticesFilePath, false)) {
      LOG.warn("storeCheckpoint: Removed file " + verticesFilePath);
    }

    FSDataOutputStream verticesOutputStream =
        getFs().create(verticesFilePath);
    ByteArrayOutputStream metadataByteStream = new ByteArrayOutputStream();
    DataOutput metadataOutput = new DataOutputStream(metadataByteStream);
    for (Integer partitionId : getPartitionStore().getPartitionIds()) {
      Partition<I, V, E> partition =
          getPartitionStore().getOrCreatePartition(partitionId);
      long startPos = verticesOutputStream.getPos();
      partition.write(verticesOutputStream);
      // write messages
      // YH: write out all message stores as needed
      if (asyncConf.isAsync()) {
        getServerData().getRemoteMessageStore().
          writePartition(verticesOutputStream, partition.getId());
        getServerData().getLocalMessageStore().
          writePartition(verticesOutputStream, partition.getId());
      } else {
        getServerData().getCurrentMessageStore().
          writePartition(verticesOutputStream, partition.getId());
      }

      if (asyncConf.isMultiPhase()) {
        getServerData().getNextPhaseRemoteMessageStore().
          writePartition(verticesOutputStream, partition.getId());
        getServerData().getNextPhaseLocalMessageStore().
          writePartition(verticesOutputStream, partition.getId());
      }

      // Write the metadata for this partition
      // Format:
      // <index count>
      //   <index 0 start pos><partition id>
      //   <index 1 start pos><partition id>
      metadataOutput.writeLong(startPos);
      metadataOutput.writeInt(partition.getId());
      if (LOG.isDebugEnabled()) {
        LOG.debug("storeCheckpoint: Vertex file starting " +
            "offset = " + startPos + ", length = " +
            (verticesOutputStream.getPos() - startPos) +
            ", partition = " + partition.toString());
      }
      getPartitionStore().putPartition(partition);
      getContext().progress();
    }
    // Metadata is buffered and written at the end since it's small and
    // needs to know how many partitions this worker owns
    FSDataOutputStream metadataOutputStream =
        getFs().create(metadataFilePath);
    metadataOutputStream.writeInt(getPartitionStore().getNumPartitions());
    metadataOutputStream.write(metadataByteStream.toByteArray());
    metadataOutputStream.close();
    verticesOutputStream.close();
    if (LOG.isInfoEnabled()) {
      LOG.info("storeCheckpoint: Finished metadata (" +
          metadataFilePath + ") and vertices (" + verticesFilePath + ").");
    }

    getFs().createNewFile(validFilePath);

    // Notify master that checkpoint is stored
    String workerWroteCheckpoint =
        getWorkerWroteCheckpointPath(getApplicationAttempt(),
            getSuperstep()) + "/" + getHostnamePartitionId();
    try {
      getZkExt().createExt(workerWroteCheckpoint,
          new byte[0],
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException.NodeExistsException e) {
      LOG.warn("storeCheckpoint: wrote checkpoint worker path " +
          workerWroteCheckpoint + " already exists!");
    } catch (KeeperException e) {
      throw new IllegalStateException("Creating " + workerWroteCheckpoint +
          " failed with KeeperException", e);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Creating " +
          workerWroteCheckpoint +
          " failed with InterruptedException", e);
    }
  }

  @Override
  public VertexEdgeCount loadCheckpoint(long superstep) {
    try {
      // clear old message stores
      // YH: clear stores based on what's enabled/disabled
      if (asyncConf.isAsync()) {
        getServerData().getRemoteMessageStore().clearAll();
        getServerData().getLocalMessageStore().clearAll();
      } else {
        getServerData().getCurrentMessageStore().clearAll();
      }

      if (asyncConf.isMultiPhase()) {
        getServerData().getNextPhaseRemoteMessageStore().clearAll();
        getServerData().getNextPhaseLocalMessageStore().clearAll();
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "loadCheckpoint: Failed to clear message stores ", e);
    }

    // Algorithm:
    // Examine all the partition owners and load the ones
    // that match my hostname and id from the master designated checkpoint
    // prefixes.
    long startPos = 0;
    int loadedPartitions = 0;
    for (PartitionOwner partitionOwner :
      workerGraphPartitioner.getPartitionOwners()) {
      if (partitionOwner.getWorkerInfo().equals(getWorkerInfo())) {
        String metadataFile =
            partitionOwner.getCheckpointFilesPrefix() +
            CHECKPOINT_METADATA_POSTFIX;
        String partitionsFile =
            partitionOwner.getCheckpointFilesPrefix() +
            CHECKPOINT_VERTICES_POSTFIX;
        try {
          int partitionId = -1;
          DataInputStream metadataStream =
              getFs().open(new Path(metadataFile));
          int partitions = metadataStream.readInt();
          for (int i = 0; i < partitions; ++i) {
            startPos = metadataStream.readLong();
            partitionId = metadataStream.readInt();
            if (partitionId == partitionOwner.getPartitionId()) {
              break;
            }
          }
          if (partitionId != partitionOwner.getPartitionId()) {
            throw new IllegalStateException(
                "loadCheckpoint: " + partitionOwner +
                " not found!");
          }
          metadataStream.close();
          Partition<I, V, E> partition =
              getConfiguration().createPartition(partitionId, getContext());
          DataInputStream partitionsStream =
              getFs().open(new Path(partitionsFile));
          if (partitionsStream.skip(startPos) != startPos) {
            throw new IllegalStateException(
                "loadCheckpoint: Failed to skip " + startPos +
                " on " + partitionsFile);
          }
          partition.readFields(partitionsStream);

          // YH: restore correct message stores
          if (asyncConf.isAsync()) {
            getServerData().getRemoteMessageStore().
              readFieldsForPartition(partitionsStream, partitionId);
            getServerData().getLocalMessageStore().
              readFieldsForPartition(partitionsStream, partitionId);
          } else {
            getServerData().getCurrentMessageStore().
              readFieldsForPartition(partitionsStream, partitionId);
          }

          if (asyncConf.isMultiPhase()) {
            getServerData().getNextPhaseRemoteMessageStore().
              readFieldsForPartition(partitionsStream, partitionId);
            getServerData().getNextPhaseLocalMessageStore().
              readFieldsForPartition(partitionsStream, partitionId);
          }

          partitionsStream.close();
          if (LOG.isInfoEnabled()) {
            LOG.info("loadCheckpoint: Loaded partition " +
                partition);
          }
          if (getPartitionStore().hasPartition(partitionId)) {
            throw new IllegalStateException(
                "loadCheckpoint: Already has partition owner " +
                    partitionOwner);
          }
          getPartitionStore().addPartition(partition);
          getContext().progress();
          ++loadedPartitions;
        } catch (IOException e) {
          throw new RuntimeException(
              "loadCheckpoint: Failed to get partition owner " +
                  partitionOwner, e);
        }
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("loadCheckpoint: Loaded " + loadedPartitions +
          " partitions of out " +
          workerGraphPartitioner.getPartitionOwners().size() +
          " total.");
    }

    // Load global stats and superstep classes
    GlobalStats globalStats = new GlobalStats();
    SuperstepClasses superstepClasses = new SuperstepClasses();
    String finalizedCheckpointPath =
        getCheckpointBasePath(superstep) + CHECKPOINT_FINALIZED_POSTFIX;
    try {
      DataInputStream finalizedStream =
          getFs().open(new Path(finalizedCheckpointPath));
      globalStats.readFields(finalizedStream);
      superstepClasses.readFields(finalizedStream);
      getConfiguration().updateSuperstepClasses(superstepClasses);
    } catch (IOException e) {
      throw new IllegalStateException(
          "loadCheckpoint: Failed to load global stats and superstep classes",
          e);
    }

    getServerData().prepareSuperstep();
    // Communication service needs to setup the connections prior to
    // processing vertices
/*if[HADOOP_NON_SECURE]
    workerClient.setup();
else[HADOOP_NON_SECURE]*/
    workerClient.setup(getConfiguration().authenticate());
/*end[HADOOP_NON_SECURE]*/
    return new VertexEdgeCount(globalStats.getVertexCount(),
        globalStats.getEdgeCount());
  }

  /**
   * Send the worker partitions to their destination workers
   *
   * @param workerPartitionMap Map of worker info to the partitions stored
   *        on this worker to be sent
   */
  private void sendWorkerPartitions(
      Map<WorkerInfo, List<Integer>> workerPartitionMap) {
    List<Entry<WorkerInfo, List<Integer>>> randomEntryList =
        new ArrayList<Entry<WorkerInfo, List<Integer>>>(
            workerPartitionMap.entrySet());
    Collections.shuffle(randomEntryList);
    WorkerClientRequestProcessor<I, V, E> workerClientRequestProcessor =
        new NettyWorkerClientRequestProcessor<I, V, E>(getContext(),
            getConfiguration(), this);
    for (Entry<WorkerInfo, List<Integer>> workerPartitionList :
      randomEntryList) {
      for (Integer partitionId : workerPartitionList.getValue()) {
        Partition<I, V, E> partition =
            getPartitionStore().removePartition(partitionId);
        if (partition == null) {
          throw new IllegalStateException(
              "sendWorkerPartitions: Couldn't find partition " +
                  partitionId + " to send to " +
                  workerPartitionList.getKey());
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("sendWorkerPartitions: Sending worker " +
              workerPartitionList.getKey() + " partition " +
              partitionId);
        }
        workerClientRequestProcessor.sendPartitionRequest(
            workerPartitionList.getKey(),
            partition);
      }
    }

    try {
      workerClientRequestProcessor.flush();
      workerClient.waitAllRequests();
    } catch (IOException e) {
      throw new IllegalStateException("sendWorkerPartitions: Flush failed", e);
    }
    String myPartitionExchangeDonePath =
        getPartitionExchangeWorkerPath(
            getApplicationAttempt(), getSuperstep(), getWorkerInfo());
    try {
      getZkExt().createExt(myPartitionExchangeDonePath,
          null,
          Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT,
          true);
    } catch (KeeperException e) {
      throw new IllegalStateException(
          "sendWorkerPartitions: KeeperException to create " +
              myPartitionExchangeDonePath, e);
    } catch (InterruptedException e) {
      throw new IllegalStateException(
          "sendWorkerPartitions: InterruptedException to create " +
              myPartitionExchangeDonePath, e);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("sendWorkerPartitions: Done sending all my partitions.");
    }
  }

  @Override
  public final void exchangeVertexPartitions(
      Collection<? extends PartitionOwner> masterSetPartitionOwners) {
    // 1. Fix the addresses of the partition ids if they have changed.
    // 2. Send all the partitions to their destination workers in a random
    //    fashion.
    // 3. Notify completion with a ZooKeeper stamp
    // 4. Wait for all my dependencies to be done (if any)
    // 5. Add the partitions to myself.
    PartitionExchange partitionExchange =
        workerGraphPartitioner.updatePartitionOwners(
            getWorkerInfo(), masterSetPartitionOwners, getPartitionStore());
    workerClient.openConnections();

    Map<WorkerInfo, List<Integer>> sendWorkerPartitionMap =
        partitionExchange.getSendWorkerPartitionMap();
    if (!getPartitionStore().isEmpty()) {
      sendWorkerPartitions(sendWorkerPartitionMap);
    }

    Set<WorkerInfo> myDependencyWorkerSet =
        partitionExchange.getMyDependencyWorkerSet();
    Set<String> workerIdSet = new HashSet<String>();
    for (WorkerInfo tmpWorkerInfo : myDependencyWorkerSet) {
      if (!workerIdSet.add(tmpWorkerInfo.getHostnameId())) {
        throw new IllegalStateException(
            "exchangeVertexPartitions: Duplicate entry " + tmpWorkerInfo);
      }
    }
    if (myDependencyWorkerSet.isEmpty() && getPartitionStore().isEmpty()) {
      if (LOG.isInfoEnabled()) {
        LOG.info("exchangeVertexPartitions: Nothing to exchange, " +
            "exiting early");
      }
      return;
    }

    String vertexExchangePath =
        getPartitionExchangePath(getApplicationAttempt(), getSuperstep());
    List<String> workerDoneList;
    try {
      while (true) {
        workerDoneList = getZkExt().getChildrenExt(
            vertexExchangePath, true, false, false);
        workerIdSet.removeAll(workerDoneList);
        if (workerIdSet.isEmpty()) {
          break;
        }
        if (LOG.isInfoEnabled()) {
          LOG.info("exchangeVertexPartitions: Waiting for workers " +
              workerIdSet);
        }
        getPartitionExchangeChildrenChangedEvent().waitForever();
        getPartitionExchangeChildrenChangedEvent().reset();
      }
    } catch (KeeperException | InterruptedException e) {
      throw new RuntimeException(
          "exchangeVertexPartitions: Got runtime exception", e);
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("exchangeVertexPartitions: Done with exchange.");
    }
  }

  /**
   * Get event when the state of a partition exchange has changed.
   *
   * @return Event to check.
   */
  public final BspEvent getPartitionExchangeChildrenChangedEvent() {
    return partitionExchangeChildrenChanged;
  }

  @Override
  protected boolean processEvent(WatchedEvent event) {
    boolean foundEvent = false;
    if (event.getPath().startsWith(masterJobStatePath) &&
        (event.getType() == EventType.NodeChildrenChanged)) {
      if (LOG.isInfoEnabled()) {
        LOG.info("processEvent: Job state changed, checking " +
            "to see if it needs to restart");
      }
      JSONObject jsonObj = getJobState();
      // in YARN, we have to manually commit our own output in 2 stages that we
      // do not have to do in Hadoop-based Giraph. So jsonObj can be null.
      if (getConfiguration().isPureYarnJob() && null == jsonObj) {
        LOG.error("BspServiceWorker#getJobState() came back NULL.");
        return false; // the event has been processed.
      }
      try {
        if ((ApplicationState.valueOf(jsonObj.getString(JSONOBJ_STATE_KEY)) ==
            ApplicationState.START_SUPERSTEP) &&
            jsonObj.getLong(JSONOBJ_APPLICATION_ATTEMPT_KEY) !=
            getApplicationAttempt()) {
          LOG.fatal("processEvent: Worker will restart " +
              "from command - " + jsonObj.toString());
          System.exit(-1);
        }
      } catch (JSONException e) {
        throw new RuntimeException(
            "processEvent: Couldn't properly get job state from " +
                jsonObj.toString());
      }
      foundEvent = true;
    } else if (event.getPath().contains(PARTITION_EXCHANGE_DIR) &&
        event.getType() == EventType.NodeChildrenChanged) {
      if (LOG.isInfoEnabled()) {
        LOG.info("processEvent : partitionExchangeChildrenChanged " +
            "(at least one worker is done sending partitions)");
      }
      partitionExchangeChildrenChanged.signal();
      foundEvent = true;
    }

    return foundEvent;
  }

  @Override
  public WorkerInfo getWorkerInfo() {
    return workerInfo;
  }

  @Override
  public PartitionStore<I, V, E> getPartitionStore() {
    return getServerData().getPartitionStore();
  }

  @Override
  public PartitionOwner getVertexPartitionOwner(I vertexId) {
    return workerGraphPartitioner.getPartitionOwner(vertexId);
  }

  @Override
  public Iterable<? extends PartitionOwner> getPartitionOwners() {
    return workerGraphPartitioner.getPartitionOwners();
  }

  @Override
  public int getPartitionId(I vertexId) {
    PartitionOwner partitionOwner = getVertexPartitionOwner(vertexId);
    return partitionOwner.getPartitionId();
  }

  @Override
  public boolean hasPartition(Integer partitionId) {
    return getPartitionStore().hasPartition(partitionId);
  }

  @Override
  public ServerData<I, V, E> getServerData() {
    return workerServer.getServerData();
  }

  @Override
  public WorkerAggregatorHandler getAggregatorHandler() {
    return aggregatorHandler;
  }

  @Override
  public void prepareSuperstep() {
    // YH: global processing for aggregators only occurs for global supersteps
    // (nothing needs to be done for local processing)
    if (getSuperstep() != INPUT_SUPERSTEP && asyncConf.needBarrier()) {
      aggregatorHandler.prepareSuperstep(workerAggregatorRequestProcessor);
    }
  }

  @Override
  public SuperstepOutput<I, V, E> getSuperstepOutput() {
    return superstepOutput;
  }

  @Override
  public VertexTypeStore<I, V, E> getVertexTypeStore() {
    return vertexTypeStore;
  }

  @Override
  public VertexPhilosophersTable<I, V, E> getVertexPhilosophersTable() {
    return vertexPTable;
  }

  @Override
  public PartitionPhilosophersTable<I, V, E> getPartitionPhilosophersTable() {
    return partitionPTable;
  }
}
