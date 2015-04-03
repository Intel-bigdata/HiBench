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

import threading, subprocess, re
from collections import namedtuple
from pprint import pprint

class PatchedNameTuple(object):
    def __sub__(self, other):
        assert isinstance(other, self.__class__)
        assert self[0] == other[0]
        cls = self.__class__
        return cls(self[0], *[a-b for a, b in zip(self[1:], other[1:])])

    def _add(self, other, override_title=None):
        assert isinstance(other, self.__class__)
        cls = self.__class__
        title = self[0] if not override_title else override_title
        return cls(title, *[a+b for a, b in zip(self[1:], other[1:])])

def ident(size, s):
    return "\n".join((" "*size + x for x in s.split("\n")))

class RemoteProc(threading.Thread):
    SEP="----SEP----"
    template=r"""exec('
import time, os, sys
{func_template}
while True:
  print "{SEP}",time.time()
{call_template}
  time.sleep({interval})
')"""

    def __init__(self, host, interval=1):
        self.host = host
        self.cmds = []
        self.interval = interval
        self.monitor_ins = {}
        super(RemoteProc, self).__init__()

    def register(self, monitor_ins, cmds):
        assert isinstance(monitor_ins, BaseMonitor)
        self.monitor_ins[len(self.cmds)] = monitor_ins # monitor command seq id => monitor instance
        self.cmds.append(cmds)

    def run(self):
        func_template = "\n".join(["def func_{id}():\n{func}"\
                                       .format(id=id,
                                               func=ident(2,
                                                          func+'\nprint "{SEP}={id}"'\
                                                              .format(SEP=self.SEP, id=id))) \
                                       for id, func in enumerate(self.cmds)])
        call_template="\n".join(["  func_{id}()"\
                                     .format(id=id) for id in range(len(self.cmds))]
                                )
        script = self.template.format(func_template=func_template, 
                                      call_template=call_template,
                                      interval = self.interval,
                                      SEP = self.SEP)

        s = script.replace('"', r'\"').replace("\n", r"\n")
        container=[]
        with self.ssh_client(self.host, "python -u -c \"{}\"".format(s)) as f:
            while 1:
                try:
                    l = f.readline()
                except KeyboardInterrupt:
                    break
                if not l: break
                if l.startswith(self.SEP):
                    tail = l.lstrip(self.SEP)
                    if tail[0]==' ': # timestamp
                        cur_timestamp = float(tail[1:])
                    else:
                        id = int(tail[1:])
                        if self.monitor_ins[id]:
                            self.monitor_ins[id].feed(container, cur_timestamp)
                    container = []
                else:
                    container.append(l.rstrip())
        self.ssh_close()

class BaseMonitor(object):
    IGNORE_KEYS=[]
    def __init__(self, rproc):
        self.rproc = rproc
        self._last = None

    def feed(self, container, timestamp):            # override to parse pulled data files
        raise NotImplementedError()

    def ssh_client(self, host, shell):    # override for opening ssh client
        raise NotImplementedError()

    def ssh_close(self):                  # override for clear up ssh client
        raise NotImplementedError()

    def commit(self, timestamp, header, stat):
        if self._last is None: self._last = stat
        else:
            stat_delta = dict([(header+'/'+k, stat[k] - self._last[k]) \
                                for k in set(self._last.keys()).union(set(stat.keys()))\
                                if k in stat and k in self._last and k not in self.IGNORE_KEYS
                                ])
            self._last = stat
            stat_delta[header+'/total'] = reduce(lambda a,b: a._add(b, 'total'), stat_delta.values())
            self.commit_aggregate(timestamp, stat_delta)
            
    def commit_aggregate(self, timestamp, datas):           # override for record & aggregation data
        pprint((timestamp, datas))
#        raise NotImplementedError()

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
        total = sum(self[1:])
        return cpu(self[0], *[x*100.0 / total for x in self[1:]]) if total>0 else self
       
class CPUMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(CPUMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/stat") as f:
  sys.stdout.write("".join([x for x in f.readlines() if x.startswith("cpu")]))
""")    
        
    def feed(self, container, timestamp):
        "parse /proc/stat"
        self.commit(timestamp, dict([self._parse_stat(line) for line in container]))

    def _parse_stat(self, line):
        "parse one line of /proc/stat"
        assert line.strip(), "BUG! empty line in /proc/stat"
        fields = line.split()
        if fields[0]=='cpu':
            fields[0]='total'
        return (fields[0], cpu(fields[0], *[int(x) for x in fields[1:8]]))

    def commit(self, timestamp, cpu_stat):
        if self._last is None:
            self._last = cpu_stat
        else:
            cpu_usage = dict([("cpu/"+k, (cpu_stat[k] - self._last[k]).percentage()) for k in self._last])
            self._last = cpu_stat
            self.commit_aggregate(timestamp, cpu_usage)

_network=namedtuple("net", ['label', "recv_bytes", "recv_packets", "recv_errs", "recv_drop",
                            "send_bytes", "send_packets", "send_errs", "send_drop"])
class network(_network, PatchedNameTuple): pass

class NetworkMonitor(BaseMonitor):
    IGNORE_KEYS=["lo"]
    def __init__(self, rproc):
        rproc.register(self, """with open("/proc/net/dev") as f:
  sys.stdout.write("".join([x for x in f.readlines()]))
""")
        self._filter = re.compile('^.*(lo|bond\d+|eth\d+|.+\.\d+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*$')
        super(NetworkMonitor, self).__init__(rproc)

    def feed(self, container, timestamp):
        "parse /proc/net/dev"
        self.commit(timestamp, "net", dict(filter(lambda x:x, [self._parse_net_dev(line) for line in container])))

    def _parse_net_dev(self, line):
        matched = self._filter.match(line)
        if matched:
            obj = network(matched.groups()[0], *[int(x) for x in matched.groups()[1:]])
            return (obj[0], obj)

_disk=namedtuple("disk", ["label", "io_read", "bytes_read", "time_spent_read", "io_write", "bytes_write", "time_spent_write"])

class disk(_disk, PatchedNameTuple): pass
    
class DiskMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(DiskMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/diskstats") as f:
  blocks = os.listdir("/sys/block")
  sys.stdout.write("".join([x for x in f.readlines() if x.split()[2] in blocks and not x.split()[2].startswith("loop")]))
""")    

    def feed(self, container, timestamp):
        "parse /proc/diskstats"
        self.commit(timestamp, "disk", dict([self._parse_disk_stat(line) for line in container]))

    def _parse_disk_stat(self, line):
        fields = line.split()[2:]
        obj = disk(fields[0],
                    io_read=int(fields[1]), bytes_read=int(fields[3])*512, time_spent_read=int(fields[4])/1000.0,
                    io_write=int(fields[5]), bytes_write=int(fields[7])*512, time_spent_write=int(fields[8])/1000.0)
        return (obj[0], obj)


_memory=namedtuple("memory", ["label", "total", "used", "buffer_cache", "free", "map"])
class memory(_memory, PatchedNameTuple): pass

class MemoryMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(MemoryMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/meminfo") as f:
  mem = dict([(a, b.split()[0].strip()) for a, b in [x.split(":") for x in f.readlines()]])
  print ":".join([mem[field] for field in ["MemTotal", "MemAvailable", "Buffers", "Cached", "MemFree", "Mapped"]])
""")   

    def feed(self, memory_status, timestamp):
        "parse /proc/meminfo"
        total, avail, buffers, cached, free, mapped= [int(x) for x in memory_status[0].split(":")]
        
        self.commit_aggregate(timestamp, {"memory/total":memory(label="total", total=total,
                                              used=total - avail,
                                              buffer_cache=buffers + cached,
                                              free=free, map=mapped)})


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
        
def test2():
    class P(RemoteProc, BashSSHClient):  pass

    p = P("localhost", 0.3)
    CPUMonitor(p)
    NetworkMonitor(p)
    DiskMonitor(p)
    MemoryMonitor(p)

    p.run()

if __name__=="__main__":
    test2()
