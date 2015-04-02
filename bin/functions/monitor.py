#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import threading, subprocess
from collections import namedtuple

class PatchedNameTuple(object):
    def __sub__(self, other):
        assert isinstance(other, self.__class__)
        assert self[0] == other[0]
        cls = self.__class__
        return cls(self[0], *[a-b for a, b in zip(self[1:], other[1:])])

    def __add__(self, other):
        assert isinstance(other, self.__class__)
        assert self[0] == other[0]
        cls = self.__class__
        return cls(self[0], *[a+b for a, b in zip(self[1:], other[1:])])


class RemoteProc(threading.Thread):
    def __init__(self, host, interval=1):
        self.host = host
        self.cmds = []
        self.interval = interval
        super(RemoteProc, self).__init__()

    def register(self, cmds):
        self.cmds.append(cmds)

    def run(self):
        pass
    
class RemoteProc(threading.Thread):
    SEP="----SEP----"
    IGNORE_KEYS=[]
    def __init__(self, host, file, interval=1):
        self.host = host
        self.file = file
        self.interval = interval
        self._last = None
        super(RemoteProc, self).__init__()

    def run(self):
        with self.ssh_client(self.host,
                             'while true ; do cat /proc/{file}; echo "{sep}"; sleep {interval}; done' \
                             .format(file=self.file, interval = self.interval, sep=self.SEP)
                             ) as f:
            container=[]
            for i in f.readline():
                if f == self.SEP:
                    self.feed(container)
                    container = []
                else:
                    container.append(i)
        self.ssh_close()
                    
    def feed(self, container):            # override to parse pulled data files
        raise NotImplementedError()

    def ssh_client(self, host, shell):    # override for opening ssh client
        raise NotImplementedError()

    def ssh_close(self):                  # override for clear up ssh client
        raise NotImplementedError()

    def commit(self, header, stat):
        if self._last is None: self._last = stat
        else:
            stat_delta = dict([(header+'/'+k, stat[k] - self._last[k]) \
                                for k in set(self._last.keys()).union(set(stat.keys()))\
                                if k in stat and k in self._last and k not in self.IGNORE_KEYS
                                ])
            stat_delta[header+'/total'] = sum(stat_delta.values())
            self.commit_aggregate(stat_delta)
            
    def commit_aggregate(self, datas):           # override for record & aggregation data
        raise NotImplementedError()

class BashSSHClient(object):
    def ssh_client(self, host, shell):
        self.proc = subprocess.Popen(["ssh", host, shell], bufsize=1, stdout=subprocess.PIPE)
        return self.proc.stdout

    def ssh_close(self):
        assert self.proc
        self.proc.wait()
        return self.proc.returncode
    
_cpu=namedtuple("cpu", ['label', 'user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq'])
class cpu(_cpu, PatchedNameTuple):
   def percentage(self):
        return cpu(self[0], *[x*100.0 / self[1] for x in self[1:]])
    
class BashSSHBasedMonitor(RemoteProc, BashSSHClient): pass
    
class CPUMonitor(BashSSHBasedMonitor):
    def __init__(self, host, interval=1):
        super(CPUMonitor, self).__init__(host, "stat", interval)
        
    def feed(self, container):
        "parse /proc/stat"
        self.commit(dict([self._parse_stat(line) for line in container if line.startswith('cpu')]))

    def _parse_stat(self, line):
        "parse one line of /proc/stat"
        assert line.strip(), "BUG! empty line in /proc/stat"
        fields = line.split()
        if fields[0]=='cpu':
            fields[0]='total'
        return (fields[0], cpu(fields[0], *[int(x) for x in field[1:9]]))

    def commit(self, cpu_stat):
        if self._last is None:
            self._last = cpu_stat
        else:
            cpu_usage = dict([("cpu/"+k, (cpu_stat[k] - self._last[k]).percentage()) for k in self._last])
            self.commit_aggregate(cpu_usage)

_network=namedtuple("net", ['label', "recv_bytes", "recv_packets", "recv_errs", "recv_drop",
                            "send_bytes", "send_packets", "send_errs", "send_drop"])

class NetworkMonitor(BashSSHBasedMonitor):
    IGNORE_KEYS=["lo"]
    def __init__(self, host, interval=1):
        self._filter = re.compile('^.*(lo|bond\d+|eth\d+|.+\.\d+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*$')
        super(NetworkMonitor, self).__init__(host, "net/dev", interval)    

    def feed(self, container):
        "parse /proc/net/dev"
        self.commit("net", dict([self._parse_net_dev(line) for line in container]))

    def _parse_net_dev(self, line):
        matched = self._filter.match(line)
        if matched:
            obj = network(*matched.groups())
            return (obj[0], obj)



_disk=namedtuple("disk", ["label", "io_read", "bytes_read", "time_spent_read", "io_write", "bytes_write", "time_spent_write"])

class disk(_disk, PatchedNameTuple): pass
    
class DiskMonitor(BashSSHBasedMonitor):
    def __init__(self, host, interval=1):
        super(DiskMonitor, self).__init__(host, "diskstats", interval)

    def run(self):
        with self.ssh_client(self.host,
                             'while true ; do cat /proc/diskstats; echo "{sep}-1"; ls /sys/block; echo "{sep}-2"; sleep {interval}; done' \
                             .format(file=self.file, interval = self.interval, sep=self.SEP)
                             ) as f:
            container=[]
            disk_status = []
            for i in f.readline():
                if f == self.SEP+"-1":
                    disk_status = container
                    container = []
                elif f == self.SEP+"-2":
                    self.feed(disk_status, container)
                    container = []
                else:
                    container.append(i)
        self.ssh_close()

    def feed(self, disk_status, block_devices):
        "parse /proc/diskstats"
        block_devices = [x.strip() for x in block_devices]
        self.commit("disk", dict([self._parse_disk_stat(line, block_devices) for line in container]))

    def _parse_disk_stat(self, line, block_devices):
        fields = [x.strip() for x in line.split()[2:] if x.strip() in block_devices]
        obj = disk(fields[0],
                    io_read=int(fields[2]), bytes_read=int(fields[3])*512, time_spent_read=int(fields[4])/1000.0,
                    io_write=int(fields[6]), bytes_write=int(fields[7])*512, time_spent_write=int(fields[8])/1000.0)
        return (obj[0], obj)


_memory=namedtuple("memory", ["label", "total", "used", "buffer_cache", "free", "map"])
class memory(_memory, PatchedNameTuple): pass

class MemoryMonitor(BashSSHBasedMonitor):
    def __init__(self, host, interval=1):
        super(DiskMonitor, self).__init__(host, "meminfo", interval)

    def feed(self, memory_status):
        "parse /proc/meminfo"
        mem = dict([[(a.strip(), int(b.strip().split()[0])) for a, b in x.split(":")] for x in container])
        
        self.commit_aggregate({"memory/total":memory(label="total", total=mem['MemTotal'],
                                              used=mem['MemTotal'] - mem['MemAvailable'],
                                              buffer_cache=mem['Buffers'] + mem['Cached'],
                                              free=mem['MemFree'],
                                              map=mem['Mapped'])})


def test():
    p = BashSSHClient()
    script=r"""exec('
import time, os, sys
while 1:
  with open("/proc/stat") as f: print f.read(),
  print "---hello---"
  time.sleep(1)
')"""
    s = script.replace('"', r'\"').replace("\n", r"\n")
    with p.ssh_client("localhost", "python -u -c \"{}\"".format(s)) as f:
        while 1:
            l = f.readline()
            print l.rstrip()
            if not l: break
    p.ssh_close()
        
if __name__=="__main__":
    test()
