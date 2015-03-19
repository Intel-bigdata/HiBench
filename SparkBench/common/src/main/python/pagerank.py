#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Copied & adopted from spark's example
# Modification from origin:
#   Use saveAsText instead of print to present the result. See the commented
# code at the tail of the code.
#

import re
import sys
from operator import add

from pyspark import SparkContext


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, "Usage: pagerank <input_file> <output_file> <iterations>"
        exit(-1)

    # Initialize the spark context.
    sc = SparkContext(appName="PythonPageRank")

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = sc.textFile(sys.argv[1], 1)

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda (url, neighbors): (url, 1.0))

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in xrange(int(sys.argv[3])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(
            lambda (url, (urls, rank)): computeContribs(urls, rank))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Collects all URL ranks and dump them to console.
    # for (link, rank) in ranks.collect():
    #     print "%s has rank: %s." % (link, rank)
    ranks.saveAsTextFile(sys.argv[2])