#!/usr/bin/env python

"""Parser for timing files."""

import os, sys, glob
import argparse, itertools

import numpy as np
import matplotlib.pyplot as plt
from pandas import DataFrame  # or *

# do some parallel computing
#from joblib import Parallel, delayed

###############
# Constants
###############
US_PER_MS = 1000.0
MS_PER_SEC = 1000.0

###############
# Parse args
###############
def workers(workers):
    try:
        w = int(workers)
        if w < 1:
            raise argparse.ArgumentTypeError('Invalid worker count')
        return w
    except:
        raise argparse.ArgumentTypeError('Invalid worker count')

parser = argparse.ArgumentParser(description='Outputs timing graphs for specified timing log files.')
parser.add_argument('-w', '--workers', type=workers, dest='workers', default=64,
                    help='number of workers (> 0), default=64')
parser.add_argument('log', type=str, nargs='+',
                    help='a timing log file, can be a regular expression (e.g. job_20140101123050_0001_timing.txt or job_2014*timing.txt)')

workers = parser.parse_args().workers
logs_re = parser.parse_args().log

logs = [f for re in logs_re for f in glob.glob(re)]


###############
# Helper funcs
###############
def bar_parser(logname, offset):
    """Parses a single worker's timings.

    Arguments:
    logname -- the log file (str)
    offset -- the file offset (int)
    
    Returns:
    A tuple (worker, values, colors, new-offset).
    """

    worker = -1
    vals = []
    colors = []
    ret_offset = offset

    prev_val = 0

    log = open(logname)
    log.seek(offset)

    while True:
        ret_offset = log.tell()
        line = log.readline()

        # not line for EOF
        if not line or "who_am_i" in line:
            if ret_offset != offset:
                break
            else:
                worker = int(line.split()[0])
                continue
        
        # have to take difference for stacked bars
        vals.append((int(line.split()[0]) - prev_val)/1000.0)
        prev_val = int(line.split()[0])

        # note that global barrier blocking/waiting time is
        # melded together w/ global barrier processing time
        if "ss_end" in line or "ss_block" in line:
            colors.append('#00e600')  # green
        elif "local_barrier_end" in line:
            colors.append('#545454')  # dark gray
        elif "local_block_end" in line or "ss_block_end" in line:
            colors.append('#ff2349')  # pink/red
        elif "global_barrier_end" in line:
            colors.append('#b4b4b4')  # light gray

        # TODO: hatch patterns?

    return (worker, vals, colors, ret_offset)


def file_plotter(logname):
    """Plots a single timing log file

    Arguments:
    logname -- the log file (str)
    """

    vals = workers*[[]]
    colors = workers*[[]]
    offset = 0

    for i in range(0,workers):
        (w, v, c, offset) = bar_parser(logname, offset)
        # have to reverse indices b/c index 0 is plotted at bottom,
        # whereas we want worker 1 to be at top
        vals[workers-w] = v
        colors[workers-w] = c
        
    # columns=['W_' + str(i) for in range(1,workers)]
    # columns not required (it labels individual stacks rather than bars)
    df = DataFrame(vals)

    # izip_longest is to do transpose while padding with None if missing elements
    # 'm' is used as a dummy fill value
    df.plot(kind='barh', stacked=True, color=list(itertools.izip_longest(*colors, fillvalue='m')), legend=False)


####################
# Plot all files
####################
for log in logs:
    file_plotter(log)

plt.show()
