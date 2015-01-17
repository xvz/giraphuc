#!/bin/bash -e

# Pulls timing information from all workers.

if [ $# -ne 2 ]; then
    echo "usage: $0 job-id output-name"
    exit -1
fi

source "$(dirname "${BASH_SOURCE[0]}")"/../common/get-hosts.sh
source "$(dirname "${BASH_SOURCE[0]}")"/../common/get-dirs.sh

jobid=${1}
output=${2}

for ((i = 1; i <= ${_NUM_MACHINES}; i++)); do
    info=$(ssh ${_MACHINES[$i]} "cat \"$HADOOP_DIR\"/logs/userlogs/${jobid}/*/syslog | grep '__TIMING' | sed 's/.*TIMING\]\] //g'")
    # need double quotes to preserve new lines
    echo -e "${info}" >> "${output}"
done