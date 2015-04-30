#!/usr/bin/env python
#coding: utf-8

import sys, os, re
from pprint import pprint
from collections import defaultdict, namedtuple
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cmx
import numpy as np

RecordRaw=namedtuple("RecordRaw", "type durtation data_size throughput_total throughput_per_node")
Record=namedtuple("Record", "type language durtation data_size throughput_total throughput_per_node")

def human_readable_size(n):
    "convert number into human readable string"
    if n<1000: return str(n)
    if n<800000: return "%.3fK" % (n/1000.0)
    if n<800000000: return "%.3fM" % (n/1000000.0)
    if n<800000000000: return "%.3fG" % (n/1000000000.0)
    return "%.3fT" % (n/1000000000000.0)

def group_by_type(datas):
    groups = defaultdict(dict)
    for i in datas:
        words = re.sub(r'((?<=[a-z])[A-Z]|(?<!\A)[A-Z](?=[a-z]))', r' \1', i.type).split()
        prefix = words[0].lower()
        suffix = "_".join([x.lower() for x in words[1:]])
        groups[suffix][prefix] = Record(type = "".join(words[1:]),
                                        language = prefix,
                                        durtation = i.durtation,
                                        data_size = i.data_size,
                                        throughput_total = i.throughput_total,
                                        throughput_per_node = i.throughput_per_node
                                        )
    return dict(groups)

def report_plot(fn):
    if not os.path.isfile(fn):
        print "Failed to find `sparkbench.report`"
        sys.exit(1)

    with open(fn) as f:
        data = [x.split() for x in f.readlines()[1:] if x.strip() and not x.strip().startswith('#')]

    pprint(data, width=300)
    groups = group_by_type([RecordRaw(type = x[0],
                                      data_size = int(x[3]),
                                      durtation = float(x[4]),
                                      throughput_total = int(x[5]) / 1024.0 / 1024,
                                      throughput_per_node = int(x[6]) / 1024.0 /1024
                                      ) for x in data])

    #print groups
    base_dir = os.path.dirname(fn)
    plot(groups, "Seconds of durtations (Less is better)", "Seconds", "durtation", os.path.join(base_dir, "durtation.png"))
#    plot(groups, "Throughput in total (Higher is better)", "MB/s", "throughput_total", os.path.join(base_dir, "throughput_total.png"))
#    plot(groups, "Throughput per node (Higher is better)", "MB/s", "throughput_per_node", os.path.join(base_dir, "throughput_per_node.png"))

def plot(groups, title="Seconds of durtations", ylabel="Seconds", value_field="durtation", fig_fn = "foo.png"):
    # plot it
    keys = groups.keys()
    languages = sorted(reduce(lambda x,y: x.union(y), [set([groups[x][y].language for y in groups[x]]) for x in groups]))
    width = 0.15
    rects = []

    fig = plt.figure()
    ax = plt.axes()
    colors='rgbcymw'
#    NCURVES=10
#    curves = [np.random.random(20) for i in range(NCURVES)]
#    values = range(NCURVES)
#    jet = colors.Colormap('jet')
#    cNorm  = colors.Normalize(vmin=0, vmax=values[-1])
#    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)
    patterns = ('-', '+', 'x', '\\', '/', '*', '.', 'O')
    for idx, lang in enumerate(languages):
        rects.append(ax.bar([x + width * (idx + 1) for x in range(len(keys))], # x index
                            [getattr(groups[x][lang], value_field) if x in groups and groups[x].has_key(lang) else 0 for x in keys], # value
                            width,
                            color = colors[idx],
                            hatch = patterns[idx]
                            )  # width
                     
                     )

    # Set the tick labels font
    for label in (ax.get_xticklabels() + ax.get_yticklabels()):
        label.set_fontname('Arial')
        label.set_fontsize(24)

    ax.set_ylabel(ylabel, fontname="Arial", size="32")
    ax.set_title(title, fontname="Arial", size="44")

    x_axis_offset = len(languages)* width /2.0
    ax.set_xticks([(x + width + x_axis_offset) for x in range(len(keys))])
    ax.set_xticklabels(["%s \n@%s" % (x, human_readable_size(groups[x].values()[0].data_size)) for x in keys])
    ax.grid(True)

    ax.legend([x[0] for x in rects],
              languages)

    def autolabel(rects):
        for rect in rects:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.05*height, '%d' % int(height),
                    ha='center', va='bottom')
#    [autolabel(x) for x in rects]

    fig = plt.gcf()
    fig.set_size_inches(18.5,10.5)
    
    plt.savefig(fig_fn, dpi=100)

if __name__ == "__main__":
    try:
        default_report_fn = sys.argv[1]
    except:
        default_report_fn = os.path.join(os.path.dirname(__file__), "..", "sparkbench.report")
        
    report_plot(default_report_fn)
