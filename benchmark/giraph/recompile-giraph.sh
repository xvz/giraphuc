#!/bin/bash -e

# usage: ./recompile-giraph.sh [giraph-core?]
#
# giraph-core: 1 to compile giraph-core as well

commondir=$(dirname "${BASH_SOURCE[0]}")/../common
source "$commondir"/get-hosts.sh
source "$commondir"/get-dirs.sh

cd "$GIRAPH_DIR"

# -pl specifies what packages to compile (e.g., giraph-examples,giraph-core)
# -Dfindbugs.skip skips "find bugs" stage (saves quite a bit of time)
if [[ $# -eq 1 && $1 -eq 1 ]]; then
    # NOTE: giraph-examples MUST be recompiled when recompiling giraph-core
    # Otherwise there may be stale code
    mvn clean install -Phadoop_1 -Dhadoop.version=1.0.4 -DskipTests -pl giraph-examples,giraph-core -Dfindbugs.skip
else
    mvn clean install -Phadoop_1 -Dhadoop.version=1.0.4 -DskipTests -pl giraph-examples -Dfindbugs.skip
fi

# copy compiled jars to worker machines
for ((i = 1; i <= ${_NUM_MACHINES}; i++)); do
    scp ./giraph-examples/target/*.jar ${_MACHINES[$i]}:"$GIRAPH_DIR"/giraph-examples/target/ &
    scp ./giraph-core/target/*.jar ${_MACHINES[$i]}:"$GIRAPH_DIR"/giraph-core/target/ &
done
wait

echo "OK."