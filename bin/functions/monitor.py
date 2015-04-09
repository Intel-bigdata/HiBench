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

import threading, subprocess, re, os, sys, signal, ast
from time import sleep, time

from collections import namedtuple
from pprint import pprint
from itertools import groupby


#FIXME: use log helper later
def log(*s):
    if len(s)==1: s=s[0]
    else: s= " ".join([str(x) for x in s])
    sys.stderr.write( str(s) +'\n')

entered=False
def sig_term_handler(signo, stack):
    global entered
    global log_path
    global report_path
    global workload_title
    global na
    if entered:
        while True: sleep(1)    # block to avoid re-enter started generate_report
        entered=True            # FIXME: Not atomic
    na.stop()
    generate_report(workload_title, log_path, report_path)
    sys.exit(0)

def samedir(fn):
    """
    return abspath of fn in the same directory where this python file stores
    """
    return os.path.abspath(os.path.join(os.path.dirname(__file__), fn))

class PatchedNameTuple(object):
    def __sub__(self, other):
        assert isinstance(other, self.__class__)
        assert self[0] == other[0]
        cls = self.__class__
        return cls(self[0], *[a-b for a, b in zip(self[1:], other[1:])])

    def __div__(self, other):
        return self.__class__(self[0], *[a/other for a in self[1:]])

    def _add(self, other, override_title=None):
        if other == None: return self
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
  print "{SEP}+%s" % time.time()
{call_template}
  print "{SEP}#end"
  time.sleep({interval})
')"""

    def __init__(self, host, interval=1):
        self.host = host
        self.cmds = []
        self.interval = interval
        self.monitor_ins = {}
        self.local_aggr_container={}
        self._running=True

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
            while self._running:
                try:
                    l = f.readline()
                except KeyboardInterrupt:
                    break
                if not l: break
                if l.startswith(self.SEP):
                    tail = l.lstrip(self.SEP)
                    if tail[0]=='+': # timestamp
                        remote_timestamp = float(tail[1:])
                        cur_timestamp = time()
                    elif tail.startswith('#end'): # end sign
                        self.na_push(cur_timestamp)
                    else:
                        id = int(tail[1:])
                        if self.monitor_ins[id]:
                            self.monitor_ins[id].feed(container, cur_timestamp)
                    container = []
                else:
                    container.append(l.rstrip())
        self.ssh_close()

    def stop(self):
        self._running=False

    def aggregate(self, timestamp, data):
        if not self.local_aggr_container:
            self.local_aggr_container['timestamp']=timestamp
        assert timestamp == self.local_aggr_container['timestamp']
        assert type(data) is dict
        self.local_aggr_container.update(data)
        self.local_aggr_container['timestamp'] = timestamp

    def na_register(self, na):
        assert isinstance(na, NodeAggregator)
        self.node_aggr_parent = na

    def na_push(self, timestamp):
        assert self.local_aggr_container.get('timestamp', -1) == timestamp
        self.node_aggr_parent.commit_aggregate(self.host, self.local_aggr_container)
        self.local_aggr_container.clear()

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
            self.rproc.aggregate(timestamp, stat_delta)


class BashSSHClientMixin(object):
    def ssh_client(self, host, shell):
        self.proc = subprocess.Popen(["ssh", host, shell], bufsize=1, stdout=subprocess.PIPE)
        return self.proc.stdout

    def ssh_close(self):
        assert self.proc
        self.proc.terminate()
        self.proc.wait()
        return self.proc.returncode

_CPU=namedtuple("CPU", ['label', 'user', 'nice', 'system', 'idle', 'iowait', 'irq', 'softirq'])
class CPU(_CPU, PatchedNameTuple):
    def percentage(self):
        total = sum(self[1:])
        return CPU(self[0], *[x*100.0 / total for x in self[1:]]) if total>0 else self

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
        return (fields[0], CPU(fields[0], *[int(x) for x in fields[1:8]]))

    def commit(self, timestamp, cpu_stat):
        if self._last is None:
            self._last = cpu_stat
        else:
            cpu_usage = dict([("cpu/"+k, (cpu_stat[k] - self._last[k]).percentage()) for k in self._last])
            self._last = cpu_stat
            self.rproc.aggregate(timestamp, cpu_usage)

_Network=namedtuple("Network", ['label', "recv_bytes", "recv_packets", "recv_errs", "recv_drop",
                                "send_bytes", "send_packets", "send_errs", "send_drop"])
class Network(_Network, PatchedNameTuple): pass

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
            obj = Network(matched.groups()[0], *[int(x) for x in matched.groups()[1:]])
            return (obj[0], obj)

_Disk=namedtuple("Disk", ["label", "io_read", "bytes_read", "time_spent_read", "io_write", "bytes_write", "time_spent_write"])

class Disk(_Disk, PatchedNameTuple): pass

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
        obj = Disk(fields[0],
                    io_read=int(fields[1]), bytes_read=int(fields[3])*512, time_spent_read=int(fields[4])/1000.0,
                    io_write=int(fields[5]), bytes_write=int(fields[7])*512, time_spent_write=int(fields[8])/1000.0)
        return (obj[0], obj)


_Memory=namedtuple("Memory", ["label", "total", "used", "buffer_cache", "free", "map"])
class Memory(_Memory, PatchedNameTuple): pass

class MemoryMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(MemoryMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/meminfo") as f:
  mem = dict([(a, b.split()[0].strip()) for a, b in [x.split(":") for x in f.readlines()]])
  print ":".join([mem[field] for field in ["MemTotal", "Buffers", "Cached", "MemFree", "Mapped"]])
""")

    def feed(self, memory_status, timestamp):
        "parse /proc/meminfo"
        total, buffers, cached, free, mapped= [int(x) for x in memory_status[0].split(":")]

        self.rproc.aggregate(timestamp, {"memory/total":Memory(label="total", total=total,
                                                               used=total - free - buffers-cached,
                                                               buffer_cache=buffers + cached,
                                              free=free, map=mapped)})

class NodeAggregator(object):
    def __init__(self, log_name):
        self.node_pool = {}
        self.log_name = log_name
        try:
            os.unlink(self.log_name)
        except OSError:
            pass

    def append(self, node):
        assert isinstance(node, RemoteProc)
        self.node_pool[node.host] = node
        node.na_register(self)

    def commit_aggregate(self, node, datas):
        datas['hostname'] = node
        with file(self.log_name, "a") as f:
            f.write(repr(datas)+"\n")

    def run(self):
        for v in self.node_pool.values():
            v.start()

    def stop(self):
        for v in self.node_pool.values():
            v.stop()
        for v in self.node_pool.values():
            v.join()

def round_to_base(v, b):
    """
    >>> round_to_base(0.1, 0.3)
    0.0
    >>> round_to_base(0.3, 0.3)
    0.3
    >>> round_to_base(0.0, 0.3)
    0.0
    >>> round_to_base(0.5, 0.3)
    0.3
    >>> round_to_base(0.51, 0.3)
    0.3
    """
    for i in range(10):
        base = int(b * 10**i)
        if abs(base - b * 10**i) < 0.001: break
    assert base>0
    return float(int(v * 10**i) / base * base) / (10**i)

def filter_dict_with_prefix(d, prefix, sort=True):
    keys = sorted(d.keys()) if sort else d.keys()
    if prefix[0]=='!':
        return  dict([(x, d[x]) for x in keys if not x.startswith(prefix[1:])])
    else:
        return  dict([(x, d[x]) for x in keys if x.startswith(prefix)])

def test():
    p = BashSSHClientMixin()
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
    class P(RemoteProc, BashSSHClientMixin):  pass

    p = P("localhost", 0.3)
    CPUMonitor(p)
    NetworkMonitor(p)
    DiskMonitor(p)
    MemoryMonitor(p)

    p.run()

def start_monitor(log_filename, nodes):
    class P(RemoteProc, BashSSHClientMixin):
        def __init__(self, *args):
            RemoteProc.__init__(self, *args)
            CPUMonitor(self)
            NetworkMonitor(self)
            DiskMonitor(self)
            MemoryMonitor(self)
    global na
    na = NodeAggregator(log_filename)
    nodes = sorted(list(set(nodes)))
    for node in nodes:
        na.append(P(node, 1))
    na.run()

def generate_report(workload_title, log_fn, report_fn):
    with open(log_fn) as f:
        datas=[ast.literal_eval(x) for x in f.readlines()]

    all_hosts = sorted(list(set([x['hostname'] for x in datas])))
#    print all_hosts
    data_slices = groupby(datas, lambda x:round_to_base(x['timestamp'], 1)) # round to 0.3 and groupby

    cpu_heatmap = ["x,y,value,hostname,coreid"]
    cpu_overall = ["x,idle,user,system,iowait,others"]
    network_overall = ["x,recv_bytes,send_bytes,|recv_packets,send_packets,errors"]
    disk_overall = ["x,read_bytes,write_bytes,|read_io,write_io"]
    memory_overall = ["x,free,buffer_cache,used"]
    for t, sub_data in data_slices:
        classed_by_host = dict([(x['hostname'], x) for x in sub_data])
        # total cpus, plot user/sys/iowait/other
        data_by_all_hosts = [classed_by_host.get(h, {}) for h in all_hosts]

        # all cpus, total
        summed1 = [x['cpu/total'] for x in data_by_all_hosts if x.has_key('cpu/total')]
        if not summed1: continue
        summed = reduce(lambda a,b: a._add(b), summed1) / len(summed1)
        for x in data_by_all_hosts:
            cpu = x.get('cpu/total', None)
            if not cpu: continue
            # user, system, io, idle, others
#            print t, x['hostname'], cpu.user, cpu.system, cpu.iowait, cpu.idle, cpu.nice+cpu.irq+cpu.softirq
#        print t, summed
        cpu_overall.append("{time},{idle},{user},{system},{iowait},{others}".format(time=int(t*1000), user=summed.user, system=summed.system, iowait=summed.iowait, idle=summed.idle, others=summed.nice+summed.irq+summed.softirq))

        # all cpus, plot heatmap according to cpus/time/usage(100%-idle)

        count={}
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefix(filter_dict_with_prefix(x, "cpu"), "!cpu/total").values()):
                try:
                    pos = count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(count)
                    count[(idx, idy, x['hostname'])] = pos
#                print t, pos, 100-y.idle, x['hostname'], y.label
                cpu_heatmap.append("{time},{pos},{value},{host},{cpuid}".format(time=int(t*1000), pos=pos, value = 100-y.idle, host = x['hostname'], cpuid = y.label))

        # all disk, total
        summed1=[x['disk/total'] for x in data_by_all_hosts if x.has_key('disk/total')]
        if not summed1: continue
        summed = reduce(lambda a,b: a._add(b), summed1)
        for x in data_by_all_hosts:
            disk = x.get('disk/total', None)
            if not disk: continue
            # io-read, io-write, bytes-read, bytes-write
#            print t, x['hostname'], disk.io_read, disk.io_write, disk.bytes_read, disk.bytes_write
 #       print t, summed
        disk_overall.append("{time},{bytes_read},{bytes_write},{io_read},{io_write}".format(time=int(t*1000), bytes_read=summed.bytes_read, bytes_write=summed.bytes_write,io_read=summed.io_read,io_write=summed.io_write))


        # all memory, total
        summed1 = [x['memory/total'] for x in data_by_all_hosts if x.has_key('memory/total')]
        if not summed1: continue
        summed = reduce(lambda a,b: a._add(b), summed1)
        for x in data_by_all_hosts:
            mem = x.get("memory/total", None)
            if not mem: continue
            # mem-total, mem-used, mem-buffer&cache, mem-free, KB
#            print t, x['hostname'], mem.total, mem.used, mem.buffer_cache, mem.free
        #print t, summed
        memory_overall.append("{time},{free},{buffer_cache},{used}".format(time=int(t*1000),free=summed.free,used=summed.used,buffer_cache=summed.buffer_cache))


        # all network, total
        summed1 = [x['net/total'] for x in data_by_all_hosts if x.has_key('net/total')]
        if not summed1: continue
        summed = reduce(lambda a,b: a._add(b), summed1)
        for x in data_by_all_hosts:
            net = x.get("net/total", None)
            if not net: continue
            # recv-byte, send-byte, recv-packet, send-packet, errors
#            print t, x['hostname'], net.recv_bytes, net.send_bytes, net.recv_packets, net.send_packets, net.recv_errs+net.send_errs+net.recv_drop+net.send_drop
#        print t, summed
        network_overall.append("{time},{recv_bytes},{send_bytes},{recv_packets},{send_packets},{errors}".format(time=int(t*1000), recv_bytes=summed.recv_bytes, send_bytes=summed.send_bytes,recv_packets=summed.recv_packets, send_packets=summed.send_packets,errors=summed.recv_errs+summed.send_errs+summed.recv_drop+summed.send_drop))
        
    with open(samedir("chart-template.html")) as f:
        template = f.read()
    with open(report_fn, 'w') as f:
        f.write(template.replace("{cpu_heatmap}",  "\n".join(cpu_heatmap))\
                    .replace("{cpu_overall}", "\n".join(cpu_overall))\
                    .replace("{network_overall}", "\n".join(network_overall))\
                    .replace("{diskio_overall}", "\n".join(disk_overall))
                    .replace("{memory_overall}", "\n".join(memory_overall))\
                    .replace("{workload_name}", workload_title)
                )

def show_usage():
    log("""Usage:
    monitor.py <workload_title> <parent_pid> <log_path.log> <report_path.html> <monitor_node_name1> ... <monitor_node_nameN>
""")
    
if __name__=="__main__":
    if len(sys.argv)<5:
        log(sys.argv)
        show_usage()
        sys.exit(1)

    global log_path
    global report_path
    global workload_title

    workload_title = sys.argv[1]
    parent_pid = sys.argv[2]
    log_path = sys.argv[3]
    report_path = sys.argv[4]
    nodes_to_monitor = sys.argv[5:]
    pid=os.fork()
    if pid:                               #parent
        print pid
    else:                                 #child
        os.close(0)
        os.close(1)
        os.close(2)
        signal.signal(signal.SIGTERM, sig_term_handler)
        start_monitor(log_path, nodes_to_monitor)
        while  os.path.exists("/proc/%s" % parent_pid):
            sleep(1)
        # parent pid lost, kill self
        sig_term_handler(None, None)

