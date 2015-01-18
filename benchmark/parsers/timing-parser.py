#!/usr/bin/env python

"""Parser and plotter for timing files."""

import os, sys, glob
import argparse, itertools

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import LineCollection
from matplotlib.colors import ListedColormap

###############
# Constants
###############
US_PER_MS = 1000.0

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

parser = argparse.ArgumentParser(description='Parses and plots timing graphs for specified timing log files.')
parser.add_argument('-w', '--workers', type=workers, dest='workers', default=64,
                    help='number of workers (> 0), default=64')
parser.add_argument('log', type=str, nargs='+',
                    help='a timing log file, can be a regular expression (e.g. job_20140101123050_0001_timing.txt or job_2014*timing.txt)')

workers = parser.parse_args().workers
logs_re = parser.parse_args().log

logs = [f for re in logs_re for f in glob.glob(re)]

# some global vars for plotting
axes = []
max_xlim = 0


###############
# Helper funcs
###############
def worker_parser(logname, offset):
    """Parses a single worker's timings.

    Arguments:
    logname -- the log file (str)
    offset -- the file offset (int)

    Returns:
    A tuple (segments, colors, new-offset). Segments is a Nx2 list where
    each element is a coordinate tuple (x, y).
    """

    segments = []
    colors = []
    ret_offset = offset

    worker = -1
    prev_val = 0

    log = open(logname)
    log.seek(offset)

    while True:
        ret_offset = log.tell()
        line = log.readline()

        # "not line" for EOF
        if not line or "who_am_i" in line:
            if ret_offset != offset:
                break
            else:
                worker = int(line.split()[0])
                continue

        # a line segment is [(x_i, y_i), (x_f, y_f)]
        # "workers - worker + 1" is to put, e.g., W_1 above W_5
        segments.append([(prev_val/US_PER_MS, workers - worker + 1),
                         (int(line.split()[0])/US_PER_MS, workers - worker + 1)])
        prev_val = int(line.split()[0])

        # note that global barrier wait time is melded together
        # with global barrier processing time
        if "[ss_end]" in line or "[ss_block]" in line:
            colors.append('#00e600')  # green
        elif "[ss_block_end]" in line:
            #colors.append('#ff0000')  # dark red
            colors.append('#ff00e2')  # magenta
        elif "[local_barrier_end]" in line:
            colors.append('#333333')  # dark gray
        elif "[comm_block_end]" in line:
            colors.append('#ff2020')  # red
        elif "[global_barrier_end]" in line:
            colors.append('#676767')  # gray

        # TODO: hatch patterns?

    return (segments, colors, ret_offset)

def file_plotter(logname):
    """Plots a single timing log file.

    Arguments:
    logname -- the log file (str)
    """

    offset = 0

    for i in range(0,workers):
        (segments, colors, offset) = worker_parser(logname, offset)

        cmap = ListedColormap(colors)
        lc = LineCollection(segments, cmap=cmap)
        # this (probably?) sets which of cmap's colors to use
        lc.set_array(np.arange(0, len(colors)))
        lc.set_linewidth(8)

        plt.gca().add_collection(lc)

        # min()/max() finds single digit (i.e., flattens and then finds)
        # this makes sure there aren't messed up/"reversed" timestamps
        if np.array(segments).min() < 0:
            print 'WARNING: encountered negative value in ' + logname + '!'

        #plt.axis('auto')    # to get auto max-lims

        # ensure we modify global var (rather than create local one)
        global max_xlim
        max_xlim = max(max_xlim, np.array(segments).max()+500)

        # gca() returns actual Axes object
        plt.gca().set_xlim(left=0)
        plt.gca().minorticks_on()
        plt.xlabel('Time (ms)')   # or plt.gca().set_xlabel(...)

        plt.ylim(0.5, workers+0.5)
        plt.yticks(range(1, workers+1))
        plt.gca().set_yticklabels([r'W$_{' + str(i) + '}$' for i in range(workers,0,-1)])
        plt.gca().tick_params(axis='y',which='both',right='off',left='off')

        plt.title(logname)


####################
# Plot all files
####################
for log in logs:
    fig = plt.figure()      # new figure (auto-numbered)
    axes.append(plt.gca())  # save axis
    file_plotter(log)

# set xlim to be same across all figures
# (useful for comparing across computation models)
for ax in axes:
    ax.set_xlim(right=max_xlim)

plt.show()




########################################
## Old method using individual bars.
## - uses way too much memory
## - takes forever to plot
########################################
#from pandas import DataFrame  # or *
#
#def bar_parser(logname, offset):
#    """DEPRECATED. Parses a single worker's timings.
#
#    Arguments:
#    logname -- the log file (str)
#    offset -- the file offset (int)
#
#    Returns:
#    A tuple (worker, values, colors, new-offset).
#    """
#
#    worker = -1
#    vals = []
#    colors = []
#    ret_offset = offset
#
#    prev_val = 0
#
#    log = open(logname)
#    log.seek(offset)
#
#    while True:
#        ret_offset = log.tell()
#        line = log.readline()
#
#        # not line for EOF
#        if not line or "who_am_i" in line:
#            if ret_offset != offset:
#                break
#            else:
#                worker = int(line.split()[0])
#                continue
#
#        # have to take difference for stacked bars
#        new_val = int(line.split()[0]) - prev_val
#
#        # if too small to be visible, just drop it---too many bars
#        # uses way too much memory
#        if new_val < 500:
#            continue
#
#        vals.append(new_val/US_PER_MS)
#        prev_val = int(line.split()[0])
#
#        # note that global barrier blocking/waiting time is
#        # melded together w/ global barrier processing time
#        if "[ss_end]" in line or "[ss_block]" in line:
#            colors.append('#00e600')  # green
#        elif "[local_barrier_end]" in line:
#            colors.append('#545454')  # dark gray
#        elif "[local_block_end]" in line or "[ss_block_end]" in line:
#            colors.append('#ff2349')  # pink/red
#        elif "[global_barrier_end]" in line:
#            colors.append('#b4b4b4')  # light gray
#
#        # TODO: hatch patterns?
#
#    return (worker, vals, colors, ret_offset)
#
#def bar_plotter(logname):
#    """DEPRECATED. Plots a single timing log file using individual bars, very slow.
#
#    Arguments:
#    logname -- the log file (str)
#    """
#
#    vals = workers*[[]]
#    colors = workers*[[]]
#    offset = 0
#
#    for i in range(0,workers):
#        (w, v, c, offset) = bar_parser(logname, offset)
#
#        # have to reverse indices b/c index 0 is plotted at bottom,
#        # whereas we want worker 1 to be at top
#        vals[workers-w] = v
#        colors[workers-w] = c
#
#    # columns=['W_' + str(i) for in range(1,workers)]
#    # columns not required (it labels individual stacks rather than bars)
#    df = DataFrame(vals)
#
#    # izip_longest is to do transpose while padding with None if missing elements
#    # 'm' is used as a dummy fill value
#    df.plot(kind='barh', stacked=True, color=list(itertools.izip_longest(*colors, fillvalue='m')),
#            legend=False, linewidth=0)
