#!/bin/bash -e                                                                                                                               
source ./common/get-dirs.sh

GRAPHS=(orkut arabic twitter uk0705)
GRAPHS_VERTEX=(orkut)
SRC=(1 3 0 0)
TOL=(0.01 0.01 0.1 0.1)
RUNS=5
WORKERS=16  #32

##############################
## Giraph
##############################
cd "$DIR_PREFIX"/benchmark/giraph/

for mode in 3 5; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 3 5; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp-serial.sh "${GRAPHS[$j]}-adj.txt" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 3 5; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc-serial.sh "${graph}-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 3 5; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr-serial.sh "${GRAPHS[$j]}-adj.txt" ${WORKERS} ${mode} ${TOL[$j]}
        done
    done
done


# giraph async, vertex-based dist locking
for mode in 0 2 4; do
    for graph in "${GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 0 2 4; do
    for j in "${!GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp-serial.sh "${GRAPHS_VERTEX[$j]}-adj.txt" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 0 2 4; do
    for graph in "${GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc-serial.sh "${graph}-adj.txt" ${WORKERS} ${mode}
        done
    done
done

for mode in 0 2 4; do
    for j in "${!GRAPHS_VERTEX[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr-serial.sh "${GRAPHS_VERTEX[$j]}-adj.txt" ${WORKERS} ${mode} ${TOL[$j]}
        done
    done
done


##############################
## GraphLab
##############################
cd "$DIR_PREFIX"/benchmark/graphlab/

for mode in 1; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./coloring.sh "${graph}-und-adj-split" ${WORKERS} ${mode}
        done
    done
done

for mode in 1; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./sssp-serial.sh "${GRAPHS[$j]}-adj-split" ${WORKERS} ${mode} ${SRC[$j]}
        done
    done
done

for mode in 1; do
    for graph in "${GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./wcc-serial.sh "${graph}-adj-split" ${WORKERS} ${mode}
        done
    done
done

for mode in 1; do
    for j in "${!GRAPHS[@]}"; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank-serial.sh "${GRAPHS[$j]}-adj-split" ${WORKERS} ${mode} ${TOL[$j]}
        done
    done
done