#!/bin/bash -e

commondir=$(dirname "${BASH_SOURCE[0]}")/../common
source "$commondir"/get-hosts.sh
source "$commondir"/get-dirs.sh

# recompile GraphLab
for glab in "$GRAPHLAB_DIR" "$GRAPHLAB_OLD_DIR"; do
    cd "$glab"/release/toolkits/graph_analytics/
    make -j $(nproc)

    for ((i = 1; i <= ${_NUM_MACHINES}; i++)); do
    # NOTE: only copy binaries that will actually be used.. it takes too long otherwise
        scp ./pagerank ${_MACHINES[$i]}:"$glab"/release/toolkits/graph_analytics/ &
        scp ./sssp ${_MACHINES[$i]}:"$glab"/release/toolkits/graph_analytics/ &
        scp ./connected_component ${_MACHINES[$i]}:"$glab"/release/toolkits/graph_analytics/ &
        scp ./approximate_diameter ${_MACHINES[$i]}:"$glab"/release/toolkits/graph_analytics/ &
        scp ./simple_coloring ${_MACHINES[$i]}:"$glab"/release/toolkits/graph_analytics/ &

        rsync -avz --exclude '*.make' --exclude '*.cmake' "$glab"/deps/local/ ${_MACHINES[$i]}:"$glab"/deps/local
    done
    wait
done

echo "OK."