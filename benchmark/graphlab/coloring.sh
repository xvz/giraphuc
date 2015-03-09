#!/bin/bash -e

if [ $# -ne 3 ]; then
    echo "usage: $0 input-graph machines sync-mode"
    echo ""
    echo "sync-mode: 0 for unserializable"
    echo "           1 for serializable"
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

# need older GraphLab for serializability (broken in newer versions)
mode=$3
case ${mode} in
    0) glab="$GRAPHLAB_DIR"; modeflag="false";;
    1) glab="$GRAPHLAB_OLD_DIR"; modeflag="true";;
    *) echo "Invalid engine-mode"; exit -1;;
esac

## log names
logname=color_${inputgraph}_${machines}_${mode}_"$(date +%Y%m%d-%H%M%S)"
logfile=${logname}_time.txt


## start logging memory + network usage
../common/bench-init.sh ${logname}

## start algorithm run
mpiexec -f ./machines -n ${machines} \
    "$glab"/release/toolkits/graph_analytics/simple_coloring \
    --edgescope ${modeflag} \
    --format adjgps \
    --graph_opts ingress=random \
    --graph "$hdfspath"/user/${USER}/input/${inputgraph} \
    --saveprefix "$hdfspath"/"$outputdir" 2>&1 | tee -a ./logs/${logfile}
#    --checkconflict true \

## finish logging memory + network usage
../common/bench-finish.sh ${logname}