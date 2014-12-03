#!/bin/bash -e

if [ $# -ne 4 ]; then
    echo "usage: $0 input-graph machines exec-mode edge-type"
    echo ""
    echo "exec-mode: 0 for synchronous BSP"
    echo "           1 for asynchronous"
    echo "           2 for barrierless asynchronous"
    echo ""
    echo "edge-type: 0 for byte array edges"
    echo "           1 for hash map edges"
    exit -1
fi

source ../common/get-dirs.sh
source ../common/get-configs.sh

# place input in /user/${USER}/input/
# output is in /user/${USER}/giraph-output/
inputgraph=$(basename $1)
outputdir=/user/${USER}/giraph-output/
hadoop dfs -rmr "$outputdir" || true

# Technically this is the number of "workers", which can be more
# than the number of machines. However, using multiple workers per
# machine is inefficient! Use more Giraph threads instead (see below).
machines=$2

execmode=$3
case ${execmode} in
    0) execopt=""; class="MinimumSpanningTree";;     # sync BSP are used by default
    1) execopt="-Dgiraph.asyncDoAsync=true \
                -Dgiraph.asyncMultiPhase=true";
       class="MSTAsync";;
    2) execopt="-Dgiraph.asyncDisableBarriers=true \
                -Dgiraph.asyncMultiPhase=true";
       class="MSTAsync";;
    *) echo "Invalid exec-mode"; exit -1;;
esac

edgetype=$4
case ${edgetype} in
    0) edgeclass="";;     # byte array edges are used by default
    1) edgeclass="-Dgiraph.inputOutEdgesClass=org.apache.giraph.edge.HashMapEdges \
                  -Dgiraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges";;
    *) echo "Invalid edge-type"; exit -1;;
esac

## log names
logname=mst_${inputgraph}_${machines}_${execmode}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt       # running time


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
# -Dmapred.task.timeout=0 is needed to prevent Giraph job from getting killed after spending 10 mins on one superstep
# Giraph seems to ignore any mapred.task.timeout specified in Hadoop's mapred-site.xml
hadoop jar "$GIRAPH_DIR"/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.0.4-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
    ${execopt} \
    ${edgeclass} \
    -Dgiraph.numComputeThreads=${GIRAPH_THREADS} \
    -Dgiraph.numInputThreads=${GIRAPH_THREADS} \
    -Dgiraph.numOutputThreads=${GIRAPH_THREADS} \
    -Dmapred.task.timeout=0 \
    org.apache.giraph.examples.${class}Computation \
    -mc org.apache.giraph.examples.${class}Computation\$${class}MasterCompute \
    -vif org.apache.giraph.examples.io.formats.${class}InputFormat \
    -vip /user/${USER}/input/${inputgraph} \
    -vof org.apache.giraph.examples.${class}Computation\$${class}VertexOutputFormat \
    -op "$outputdir" \
    -w ${machines} 2>&1 | tee -a ./logs/${logfile}

# -wc org.apache.giraph.examples.${class}Computation\$${class}WorkerContext
# see giraph-core/.../utils/ConfigurationUtils.java for command line opts (or -h flag to GiraphRunner)

## finish logging memory + network usage
../common/bench-finish.sh ${logname}

## clean up step needed for Giraph
./kill-java-job.sh