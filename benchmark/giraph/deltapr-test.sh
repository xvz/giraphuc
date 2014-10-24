#!/bin/bash -e

if [ $# -ne 6 ]; then
    echo "usage: $0 input-graph machines exec-mode supersteps open-req send-buf"
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
    1) execopt="-Dgiraph.asyncDoAsync=true";;
    2) execopt="-Dgiraph.asyncDisableBarriers=true";;
    *) echo "Invalid exec-mode"; exit -1;;
esac

supersteps=$4

## log names
logname=deltapr_${inputgraph}_${machines}_${execmode}_${supersteps}_"$(date +%Y%m%d-%H%M%S)"-${5}-${6}
logfile=${logname}_time.txt       # running time


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
hadoop jar "$GIRAPH_DIR"/giraph-examples/target/giraph-examples-1.1.0-for-hadoop-1.0.4-jar-with-dependencies.jar org.apache.giraph.GiraphRunner \
    ${execopt} \
    -Dgiraph.numComputeThreads=${GIRAPH_THREADS} \
    -Dgiraph.numInputThreads=${GIRAPH_THREADS} \
    -Dgiraph.numOutputThreads=${GIRAPH_THREADS} \
    -Dgiraph.maxNumberOfOpenRequests=$5 \
    -Dgiraph.msgRequestSize=$6 \
    -Dgiraph.vertexValueFactoryClass=org.apache.giraph.examples.DeltaPageRankComputation\$DeltaPageRankVertexValueFactory \
    -Dmapred.task.timeout=0 \
    org.apache.giraph.examples.DeltaPageRankComputation \
    -c org.apache.giraph.combiner.DoubleSumMessageCombiner \
    -ca DeltaPageRankComputation.maxSS=${supersteps} \
    -vif org.apache.giraph.examples.io.formats.SimplePageRankInputFormat \
    -vip /user/${USER}/input/${inputgraph} \
    -vof org.apache.giraph.examples.DeltaPageRankComputation\$DeltaPageRankVertexOutputFormat \
    -op "$outputdir" \
    -w ${machines} 2>&1 | tee -a ./logs/${logfile}

## finish logging memory + network usage
../common/bench-finish.sh ${logname}

## output l1-norm to time logfile
prsln=${inputgraph}-300-0.txt
proutput=${inputgraph}-${supersteps}-${execmode}.txt

hadoop dfs -cat giraph-output/part-* | sort -nk1 --parallel=$(nproc) -S $(grep 'MemTotal' /proc/meminfo | awk '{print $2}') > ${proutput}

if [[ -f ${prsln} && ${proutput} != ${prsln} ]]; then
    l1norm=$(../parsers/prtolchecker ${prsln} ${proutput})
    echo "L1-NORM: ${l1norm}" >> ./logs/${logfile}
    rm -f ${proutput}
fi

## clean up step needed for Giraph 1.0 (not really needed for 1.1)
./kill-java-job.sh