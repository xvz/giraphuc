#!/bin/bash -e
source ./common/get-dirs.sh

RUNS=3

##############################
## Giraph
##############################
cd "$DIR_PREFIX"/benchmark/giraph/

#=============
#     US
#=============
for mode in 0; do
    for steps in 30 50 70 90 110 120; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh usaroad-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 1; do
    for steps in 30 50 70 90 110; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh usaroad-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 2; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr-test.sh usaroad-adj.txt 64 ${mode} ${steps} 32 65536
        done
    done
done


#=============
#     AR
#=============
for mode in 0; do
    for steps in 30 50 70 90 115; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh arabic-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 1; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh arabic-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 2; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr-test.sh arabic-adj.txt 64 ${mode} ${steps} 32 65536
        done
    done
done


#=============
#     TW
#=============
for mode in 0; do
    for steps in 30 60 90 120 150; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh twitter-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 1; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh twitter-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 2; do
    for steps in 20 40 60 80 90; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr-test.sh twitter-adj.txt 64 ${mode} ${steps} 32 65536
        done
    done
done


#=============
#     UK
#=============
for mode in 0; do
    for steps in 30 50 70 90 110 120; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh uk0705-adj.txt 64 ${mode} ${steps}
        done
    done
done
for mode in 1 2; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./deltapr.sh uk0705-adj.txt 64 ${mode} ${steps}
        done
    done
done



##############################
## GraphLab
##############################
cd "$DIR_PREFIX"/benchmark/graphlab/

#=============
#     US
#=============
for mode in 0; do
    for steps in 10 30 50 70 90; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh usaroad-adj-split 64 ${mode} ${steps}
        done
    done
done

#=============
#     AR
#=============
for mode in 0; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh arabic-adj-split 64 ${mode} ${steps}
        done
    done
done

#=============
#     TW
#=============
for mode in 0; do
    for steps in 10 30 50 70 90 110 130; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh twitter-adj-split 64 ${mode} ${steps}
        done
    done
done

#=============
#     UK
#=============
for mode in 0; do
    for steps in 20 40 60 80 100; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh uk0705-adj-split 64 ${mode} ${steps}
        done
    done
done


#=============
#    async
#=============
for mode in 1; do
    for tol in 0.01 0.0005 0.00001 0.000001 0.0000001; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh usaroad-adj-split 64 ${mode} ${tol}
        done
    done
    for tol in 0.01 0.0005 0.00001 0.000005 0.0000001; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh arabic-adj-split 64 ${mode} ${tol}
        done
    done
    for tol in 0.1 0.001 0.00001 0.0000001; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh twitter-adj-split 64 ${mode} ${tol}
        done
    done
    for tol in 0.01 0.0005 0.000005 0.0000001; do
        for ((i = 1; i <= RUNS; i++)); do
            ./pagerank.sh uk0705-adj-split 64 ${mode} ${tol}
        done
    done
done
