#!/bin/bash -e

if [ $# -ne 4 ]; then
    echo "usage: $0 input-graph machines sync-mode tolerance"
    echo ""
    echo "sync-mode: 0 for factorized async"
    echo "           1 for serializable async"
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
case ${mode} in
    0) glab="$GRAPHLAB_DIR"; modeflag="true";;
    1) glab="$GRAPHLAB_OLD_DIR"; modeflag="false";;
    *) echo "Invalid engine-mode"; exit -1;;
esac

tol=$4

## log names
logname=pagerank_${inputgraph}_${machines}_${mode}_${tol}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
mpiexec -f ./machines -n ${machines} \
    "$glab"/release/toolkits/graph_analytics/pagerank \
    --tol ${tol} \
    --engine async \
    --engine_opts factorized=${modeflag} \
    --format adjgps \
    --graph_opts ingress=random \
    --graph "$hdfspath"/user/${USER}/input/${inputgraph} \
    --saveprefix "$hdfspath"/"$outputdir" 2>&1 | tee -a ./logs/${logfile}

## finish logging memory + network usage
../common/bench-finish.sh ${logname}

## output l1-norm to time logfile
prsln=${inputgraph}-300-0.txt
proutput=${inputgraph}-${tol}-${mode}.txt

hadoop dfs -cat graphlab-output/* | sort -nk1 --parallel=$(nproc) -S $(grep 'MemTotal' /proc/meminfo | awk '{print $2}') > ${proutput}

if [[ -f ${prsln} && ${proutput} != ${prsln} ]]; then
    l1norm=$(../parsers/prtolchecker ${prsln} ${proutput})
    echo "L1-NORM: ${l1norm}" >> ./logs/${logfile}
    rm -f ${proutput}
fi