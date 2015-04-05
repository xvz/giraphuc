#!/bin/bash -e                                                                                                                               
source ./common/get-dirs.sh

GRAPHS=(orkut usaroad arabic twitter)
GRAPHS_VERTEX=(orkut usaroad)
SRC=(1 1 3 0)
RUNS=5
WORKERS=16

##############################
## Giraph
##############################
cd "$DIR_PREFIX"/benchmark/giraph/

for mode in 0 2 3 5; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS[$j]}-adj.txt" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 0 2 3 5; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 0 2 3 5; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj.txt" ${WORKERS} ${mode}
        done
    done
done

# vertex-based dist locking
for mode in 1 4; do
    for j in "${!GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS_VERTEX[$j]}-adj.txt" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 1 4; do
    for graph in "${GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 1 4; do
    for graph in "${GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj.txt" ${WORKERS} ${mode}
        done
    done
done


##############################
## GraphLab
##############################
cd "$DIR_PREFIX"/benchmark/graphlab/

for mode in 1; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp.sh "${GRAPHS[$j]}-adj-split" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 1; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc.sh "${graph}-adj-split" ${WORKERS} ${mode}
        done
    done
done

for mode in 1; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj-split" ${WORKERS} ${mode}
        done
    done
done