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
                    help='Number of workers (> 0), default=64')
parser.add_argument('-p', '--plot', action='store_true', default=False,
                    help='Plot graphs in addition to printing out stats')
parser.add_argument('-v', '--visible-local-barriers', action='store_true', default=False,
                    help='Make local barriers more visible by inflating their cost')
parser.add_argument('-s', '--simple-colours', action='store_true', default=False,
                    help='Use a simple colour scheme (red for comm, gray for barrier)')
parser.add_argument('log', type=str, nargs='+',
                    help='A timing log file, can be a regular expression (e.g. job_20140101123050_0001_timing.txt or job_2014*timing.txt)')

workers = parser.parse_args().workers
do_plot = parser.parse_args().plot
do_visible_lb = parser.parse_args().visible_local_barriers
do_simple = parser.parse_args().simple_colours
logs_re = parser.parse_args().log

logs = [f for re in logs_re for f in glob.glob(re)]

# some global vars for plotting
MIN_LB_SIZE = 100000.0   # in us
axes = []
max_xlim = 0

# constants for global per-file stats (index names)
NUM_STATS = 7
[T_TOTAL, T_COMPUTE, T_SS_COMM_BLOCK, T_COMM_BLOCK,
 T_LOCAL_BARRIER, T_LTW_BARRIER, T_GLOBAL_BARRIER] = range(0, NUM_STATS)

###############
# Helper funcs
###############
def worker_parser(logname, offset, summary_stats):
    """Parses a single worker's timings.

    Arguments:
    logname -- the log file (str)
    offset -- the file offset (int)
    summary_stats -- 8xN np.array of ints, where N is the number of workers;
                     will be modified by this function

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
                summary_stats[T_TOTAL, worker-1] = prev_val
                break
            else:
                worker = int(line.split()[0])
                continue

        new_val = int(line.split()[0])
        diff = new_val - prev_val

        # a line segment is [(x_i, y_i), (x_f, y_f)]
        # "workers - worker + 1" is to put, e.g., W_1 above W_5
        if do_visible_lb and "[local_barrier_end]" in line and diff < MIN_LB_SIZE:
            # this will draw over the previous segment
            segments.append([((new_val-MIN_LB_SIZE)/US_PER_MS, workers - worker + 1),
                             (new_val/US_PER_MS, workers - worker + 1)])
        else:
            segments.append([(prev_val/US_PER_MS, workers - worker + 1),
                             (new_val/US_PER_MS, workers - worker + 1)])

        # note that global barrier wait time is melded together
        # with global barrier processing time
        if "[ss_end]" in line or "[ss_block]" in line:
            colors.append('#33ff33')  # green
            t_index = T_COMPUTE

        elif "[ss_block_end]" in line:
            if do_simple:
                colors.append('#ff583d')  # red
            else:
                colors.append('#ff00e2')  # magenta
            t_index = T_SS_COMM_BLOCK
        elif "[comm_block_end]" in line:
            colors.append('#ff583d')  # red
            t_index = T_COMM_BLOCK

        elif "[local_barrier_end]" in line:
            colors.append('#000000')  # black
            t_index = T_LOCAL_BARRIER

        elif "[lightweight_barrier_end]" in line:
            if do_simple:
                colors.append('#808080')  # dark gray
            else:
                colors.append('#348abd')  # blue
            t_index = T_LTW_BARRIER
        elif "[global_barrier_end]" in line:
            colors.append('#808080')  # dark gray
            t_index = T_GLOBAL_BARRIER

        else:
            print 'WARNING: encountered bad line <' + line + '> in ' + logname + '!'
            t_index = -1     # this will cause script to crash

        summary_stats[t_index, worker-1] += diff

        prev_val = new_val
        # TODO: hatch patterns?

    return (segments, colors, ret_offset)

def file_plotter(logname):
    """Plots a single timing log file.

    Arguments:
    logname -- the log file (str)
    """

    summary_stats = np.zeros((NUM_STATS,workers))
    offset = 0

    for i in range(0,workers):
        (segments, colors, offset) = worker_parser(logname, offset, summary_stats)

        if do_plot:
            cmap = ListedColormap(colors)
            lc = LineCollection(segments, cmap=cmap)
            # this (probably?) sets which of cmap's colors to use
            lc.set_array(np.arange(0, len(colors)))
            lc.set_linewidth(8*60.0/workers)

            plt.gca().add_collection(lc)

            # min()/max() finds single digit (i.e., flattens and then finds)
            # this makes sure there aren't messed up/"reversed" timestamps
            if np.array(segments).min() < 0:
                print 'WARNING: encountered negative value in ' + logname + '!'

            #plt.axis('auto')    # to get auto max-lims

            # ensure we modify global var (rather than create local one)
            global max_xlim
            max_xlim = max(max_xlim, np.array(segments).max()+500)


    ## pretty plot
    if do_plot:
        # gca() returns actual Axes object
        plt.gca().set_xlim(left=0)
        plt.gca().minorticks_on()
        plt.xlabel('Time (ms)')   # or plt.gca().set_xlabel(...)

        plt.ylim(0.5, workers+0.5)
        plt.yticks(range(1, workers+1))
        plt.gca().set_yticklabels([r'W$_{' + str(i) + '}$' for i in range(workers,0,-1)])
        plt.gca().tick_params(axis='y',which='both',right='off',left='off')

        plt.title(logname)

    ## print statistics
    for w in range(0,workers):
        for s in range(1, NUM_STATS):
            summary_stats[s, w] /= float(summary_stats[T_TOTAL, w])
    stats = summary_stats[1:NUM_STATS]

    avg_stats = tuple([s.mean()*100 for s in stats])
    min_stats = tuple([s.min()*100 for s in stats])
    max_stats = tuple([s.max()*100 for s in stats])

    print '===================================================================================='
    print '   ' + logname
    print '===================================================================================='
    print '    |  compute  | ss comm |   comm   | local barrier | ltw barrier | global barrier'
    print '----+-----------+---------+----------+---------------+-------------+----------------'
    print 'avg |  %6.3f%%  | %6.3f%% |  %6.3f%% |    %6.3f%%    |   %6.3f%%   |     %6.3f%%' % avg_stats
    print 'min |  %6.3f%%  | %6.3f%% |  %6.3f%% |    %6.3f%%    |   %6.3f%%   |     %6.3f%%' % min_stats
    print 'max |  %6.3f%%  | %6.3f%% |  %6.3f%% |    %6.3f%%    |   %6.3f%%   |     %6.3f%%' % max_stats
    print ''

####################
# Plot all files
####################
print ''
for log in logs:
    if do_plot:
        fig = plt.figure()      # new figure (auto-numbered)
        axes.append(plt.gca())  # save axis
    file_plotter(log)

if do_plot:
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
#    # NOTE: do NOT use workers*[[]]---that inner list is a SINGLE object referenced multiple times!!
#    # (in this case we could, because inner list gets replaced.. but in general, don't do this)
#    vals = [[] for i in range(0, workers)]
#    colors = [[] for i in range(0, workers)]
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
