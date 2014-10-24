#!/bin/bash -e                                                                                                                               
source ../common/get-dirs.sh

GRAPHS=(usaroad arabic twitter uk0705)
GRAPHS_MST=(usaroad arabic twitter)
SRC=(1 3 0 0)
RUNS=5

##############################
## Giraph
##############################
cd "$DIR_PREFIX"/benchmark/giraph/

for mode in 0 1 2; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS[$j]}-adj.txt" 64 ${mode} ${SRC[$j]}
        done
    done
done

for mode in 0 1 2; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj.txt" 64 ${mode}
        done
    done
done

for mode in 0 1 2; do
    for graph in "${GRAPHS_MST[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./mst.sh "${graph}-mst-adj.txt" 64 ${mode} 1
        done
    done
done


##############################
## GraphLab
##############################
cd "$DIR_PREFIX"/benchmark/graphlab/

for mode in 0; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS[$j]}-adj-split" 64 ${mode} ${SRC[$j]}
        done
    done
done

for mode in 0; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj-split" 64 ${mode}
        done
    done
done


GRAPHS=(arabic twitter uk0705)
SRC=(3 0 0)
for mode in 1; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS[$j]}-adj-split" 64 ${mode} ${SRC[$j]}
        done
    done
done

GRAPHS=(twitter)
for mode in 1; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj-split" 64 ${mode}
        done
    done
done