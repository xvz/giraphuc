#!/bin/bash -e

if [ $# -ne 4 ]; then
    echo "usage: $0 input-graph machines engine-mode tolerance"
    echo ""
    echo "engine-mode: 0 for synchronous engine"
    echo "             1 for asynchronous engine"
    exit -1
fi

source ../common/get-dirs.sh

# place input in /user/${USER}/input/
# output is in /user/${USER}/graphlab-output/
inputgraph=$(basename $1)
outputdir=/user/${USER}/graphlab-output/
hadoop dfs -rmr "$outputdir" || true

hdfspath=$(grep hdfs "$HADOOP_DIR"/conf/core-site.xml | sed -e 's/.*<value>//' -e 's@</value>.*@@')

machines=$2

mode=$3
stop=$4
case ${mode} in
    0) execopt="--engine sync \
                --tol ${stop}";;
    1) execopt="--engine async \
                --tol ${stop}";;
    *) echo "Invalid engine-mode"; exit -1;;
esac

## log names
logname=pagerank_${inputgraph}_${machines}_${mode}_${stop}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
mpiexec -f ./machines -n ${machines} \
    "$GRAPHLAB_DIR"/release/toolkits/graph_analytics/pagerank \
    ${execopt} \
    --format adjgps \
    --graph_opts ingress=random \
    --graph "$hdfspath"/user/${USER}/input/${inputgraph} \
    --saveprefix "$hdfspath"/"$outputdir" 2>&1 | tee -a ./logs/${logfile}

## finish logging memory + network usage
../common/bench-finish.sh ${logname}

## output l1-norm to time logfile
prsln=${inputgraph}-300-0.txt
proutput=${inputgraph}-${stop}-${mode}.txt

hadoop dfs -cat graphlab-output/* | sort -nk1 --parallel=$(nproc) -S $(grep 'MemTotal' /proc/meminfo | awk '{print $2}') > ${proutput}

if [[ -f ${prsln} && ${proutput} != ${prsln} ]]; then
    l1norm=$(../parsers/prtolchecker ${prsln} ${proutput})
    echo "L1-NORM: ${l1norm}" >> ./logs/${logfile}
    rm -f ${proutput}
fi