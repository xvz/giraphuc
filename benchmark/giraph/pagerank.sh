#!/bin/bash -e

if [ $# -ne 3 ]; then
    echo "usage: $0 input-graph machines exec-mode supersteps"
    echo ""
    echo "exec-mode: 0 for synchronous BSP"
    echo "           1 for asynchronous"
    echo "           2 for barrierless asynchronous"
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
    0) execopt="";;     # sync BSP are used by default
    1) execopt="-Dgiraph.asyncLocalRead=true \
                -Dgiraph.asyncRemoteRead=true";;
    2) execopt="-Dgiraph.asyncLocalRead=true \
                -Dgiraph.asyncRemoteRead=true \
                -Digraph.asyncDisableBarriers=true";;
    *) echo "Invalid exec-mode"; exit -1;;
esac

supersteps=$4

## log names
logname=pagerank_${inputgraph}_${machines}_${execmode}_${supersteps}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt       # running time


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
hadoop jar "$GIRAPH_DIR"/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.0.4-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
    ${execopt} \
    -Dgiraph.numComputeThreads=${GIRAPH_THREADS} \
    -Dgiraph.numInputThreads=${GIRAPH_THREADS} \
    -Dgiraph.numOutputThreads=${GIRAPH_THREADS} \
    org.apache.giraph.examples.SimplePageRankComputation \
    -ca SimplePageRankComputation.maxSS=${supersteps} \
    -vif org.apache.giraph.examples.io.formats.SimplePageRankInputFormat \
    -vip /user/${USER}/input/${inputgraph} \
    -vof org.apache.giraph.examples.SimplePageRankComputation\$SimplePageRankVertexOutputFormat \
    -op "$outputdir" \
    -w ${machines} 2>&1 | tee -a ./logs/${logfile}

#    -ca SimplePageRankComputation.minTol=2.3 \
#    -Dgiraph.inputOutEdgesClass=org.apache.giraph.edge.HashMapEdges \
#    -Dgiraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges

#    -mc org.apache.giraph.examples.SimplePageRankComputation\$SimplePageRankMasterCompute \

#    -c org.apache.giraph.combiner.DoubleSumMessageCombiner \
# mc only needed when aggregators needed (which is for error tols)
# alternative output format: -vof org.apache.giraph.io.formats.IdWithValueTextOutputFormat

## finish logging memory + network usage
../common/bench-finish.sh ${logname}

## clean up step needed for Giraph
./kill-java-job.sh