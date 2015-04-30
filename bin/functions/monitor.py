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

import threading, subprocess, re, os, sys, signal, socket
from time import sleep, time
from contextlib import closing
import traceback, thread
from datetime import datetime
from collections import namedtuple
from pprint import pprint
from itertools import groupby

# Probe intervals, in seconds.
# Warning: a value too short may get wrong results due to lack of data when system load goes high.
#          and must be float!
PROBE_INTERVAL=float(5)

#FIXME: use log helper later
#log_lock = threading.Lock()
def log(*s):
    if len(s)==1: s=s[0]
    else: s= " ".join([str(x) for x in s])
#    with log_lock:
#        with open("/home/zhihui/monitor_proc.log", 'a') as f:
    log_str = str(thread.get_ident())+":"+str(s) +'\n'
    #        f.write( log_str )
    sys.stderr.write(log_str)
        
entered=False
def sig_term_handler(signo, stack):
    global entered
    global log_path
    global report_path
    global workload_title
    global bench_log_path
    global na

    if not entered:
        entered=True            # FIXME: Not atomic
    else: return

    na.stop()
    generate_report(workload_title, log_path, bench_log_path, report_path)
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
    template_debug=r"""exec('
import time, os, sys, socket, traceback
socket.setdefaulttimeout(1)
def log(*x, **kw):
 with open("/home/zhihui/probe.log", kw.get("mode","a")) as f:
  f.write(repr(x)+chr(10))
try:
 log("create socket", mode="w")
 s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
 log("bind socket")
 s.bind(("0.0.0.0",0))
 log("listen socket")
 s.listen(5)
 log("bind socket to:", s.getsockname())
 while True:
  log("accepting")
  try:
   print s.getsockname()[1]
   s2,peer=s.accept()
   break
  except socket.timeout:
   log("accept timeout, retry")
 log("accepted, peer:",peer)
except Exception as e:
 import traceback
 log(traceback.format_exc())
{func_template}
while True:
  s2.send(("{SEP}+%s" % time.time())+chr(10))
{call_template}
  s2.send("{SEP}#end"+chr(10))
  time.sleep({interval})
')"""
    template=r"""exec('
import time, os, sys, socket, traceback
s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("0.0.0.0",0))
s.listen(5)
print s.getsockname()[1]
s2,peer=s.accept()
{func_template}
while True:
  s2.send(("{SEP}+%s" % time.time())+chr(10))
{call_template}
  s2.send("{SEP}#end"+chr(10))
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
                                                          func+'\ns2.send("{SEP}={id}"+chr(10))'\
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
#        log("ssh client to:", self.host)
        with self.ssh_client(self.host, "python -u -c \"{script}\"".format(script=s)) as f:
#            log("ssh client %s connected" % self.host)
            try:
                port_line = f.readline()
#                log("host:", self.host, "got port,", port_line)
                port = int(port_line.rstrip())
                s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(0.5)
                for i in range(30): # try to connect 30 times maximum
                    try:
#                        log("try to connect:", self.host, port)
                        s.connect((self.host, port))
#                        log("connectted to:", self.host, port)
                        break
                    except socket.timeout:
#                        log("connecting to:", self.host, port, "timedout")
                        pass
                else:           # not connectted after 30 times trying
#                    log("cann't connectted to:", self.host, port)
                    s.shutdown(socket.SHUT_RDWR)
                    self.ssh_close()              
                    return
                s.settimeout(None)
            except Exception as e:
                log(traceback.format_exc())

            with closing(s.makefile()) as f2:
                while self._running:
                    try:
                        l = f2.readline()
                    except KeyboardInterrupt:
                        break
                    if not l: break
                    if l.startswith(self.SEP):
                        tail = l.lstrip(self.SEP)
                        if tail[0]=='+': # timestamp
                            remote_timestamp = float(tail[1:])
                            cur_timestamp = time()
                        elif tail.startswith('#end'): # end sign
#                            log("na push, timestamp:", cur_timestamp)
                            self.na_push(cur_timestamp)
                        else:
                            id = int(tail[1:])
                            if self.monitor_ins[id]:
                                self.monitor_ins[id].feed(container, cur_timestamp)
                        container = []
                    else:
                        container.append(l.rstrip())
            s.shutdown(socket.SHUT_RDWR)
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
        if self.local_aggr_container:
            assert self.local_aggr_container.get('timestamp', -1) == timestamp
            self.node_aggr_parent.commit_aggregate(self.host, self.local_aggr_container)
            self.local_aggr_container={}

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
#            if header.startswith("net"):
#                print stat_delta
            stat_delta[header+'/total'] = reduce_patched(lambda a,b: a._add(b, 'total'), stat_delta.values())
            self.rproc.aggregate(timestamp, stat_delta)


class BashSSHClientMixin(object):
    ssh_lock = threading.Lock()
    def ssh_client(self, host, shell):
        with open(os.devnull, 'rb', 0) as DEVNULL:
            with BashSSHClientMixin.ssh_lock:
                self.proc = subprocess.Popen(["ssh", host, shell], bufsize=1, 
                                             stdin=DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
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
  s2.send("".join([x for x in f.readlines() if x.startswith("cpu")]))
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
  s2.send("".join([x for x in f.readlines()]))
""")
        self._filter = re.compile('^.*(lo|bond\d+|eth\d+|.+\.\d+):\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*$')
        super(NetworkMonitor, self).__init__(rproc)

    def feed(self, container, timestamp):
        "parse /proc/net/dev"
        self.commit(timestamp, "net", dict(filter(lambda x:x, [self._parse_net_dev(line) for line in container])))

    def _parse_net_dev(self, line):
        matched = self._filter.match(line)
        if matched:
            obj = Network(matched.groups()[0], *[int(x) for x in matched.groups()[1:]])
            if not (obj.recv_bytes==0 and obj.send_bytes==0):
                return (obj[0], obj)

_Disk=namedtuple("Disk", ["label", "io_read", "bytes_read", "time_spent_read", "io_write", "bytes_write", "time_spent_write"])

class Disk(_Disk, PatchedNameTuple): pass

class DiskMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(DiskMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/diskstats") as f:
  blocks = os.listdir("/sys/block")
  s2.send("".join([x for x in f.readlines() if x.split()[2] in blocks and not x.split()[2].startswith("loop") and x.split()[3]!="0"]))
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
  s2.send(":".join([mem[field] for field in ["MemTotal", "Buffers", "Cached", "MemFree", "Mapped"]])+chr(10))
""")

    def feed(self, memory_status, timestamp):
        "parse /proc/meminfo"
        total, buffers, cached, free, mapped= [int(x) for x in memory_status[0].split(":")]

        self.rproc.aggregate(timestamp, {"memory/total":Memory(label="total", total=total,
                                                               used=total - free - buffers-cached,
                                                               buffer_cache=buffers + cached,
                                                               free=free, map=mapped)})
_Proc=namedtuple("Proc", ["label", "load5", "load10", "load15", "running", "procs"])
class Proc(_Proc, PatchedNameTuple): pass

class ProcMonitor(BaseMonitor):
    def __init__(self, rproc):
        super(ProcMonitor, self).__init__(rproc)
        rproc.register(self, """with open("/proc/loadavg") as f:
  s2.send(f.read())
""")

    def feed(self, load_status, timestamp):
        "parse /proc/meminfo"
        load5, load10, load15, running_procs= load_status[0].split()[:4]
        running, procs = running_procs.split('/')

        self.rproc.aggregate(timestamp, {"proc":Proc(label="total", load5=float(load5), load10=float(load10),
                                                     load15=float(load15), running=int(running), procs=int(procs))})
        

class NodeAggregator(object):
    def __init__(self, log_name):
        self.node_pool = {}
        self.log_name = log_name
        self.log_lock = threading.Lock()
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
        with self.log_lock:
            with file(self.log_name, "a") as f:
                f.write(repr(datas) + "\n")

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

def reduce_patched(func, data):
    if len(data)==1:
        return data[0]
    elif len(data)==0:
        return data
    else:
        return reduce(func, data)

def filter_dict_with_prefixes(d, *prefixes):
    if len(prefixes)==1:
        return filter_dict_with_prefix(d, prefixes[0])
    else:
        return reduce_patched(lambda a,b: filter_dict_with_prefix(filter_dict_with_prefix(d, a),b),
                      prefixes)

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
    with p.ssh_client("localhost", "python -u -c \"{s}\"".format(s=s)) as f:
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
            ProcMonitor(self)
    global na
    na = NodeAggregator(log_filename)
    nodes = sorted(list(set(nodes)))
    for node in nodes:
        na.append(P(node, PROBE_INTERVAL))
    na.run()

def parse_bench_log(benchlog_fn):
    events=["x,event"]
    _spark_stage_submit = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO [a-zA-Z0-9_\.]*DAGScheduler: Submitting (Stage \d+) \((.*)\).+$") # submit spark stage
    _spark_stage_finish = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO [a-zA-Z0-9_\.]*DAGScheduler: (Stage \d+) \((.*)\) finished.+$")   # spark stage finish
    _hadoop_run_job = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO mapred.*\.Job.*: Running job: job_([\d_]+)$") # hadoop run job
    _hadoop_map_reduce_progress = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO mapred.*\.Job.*:\s+map (\d{1,2})% reduce (\d{1,2})%$") # hadoop reduce progress
    _hadoop_job_complete_mr1 = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO mapred.JobClient: Job complete: job_([\d_]+)$")
    _hadoop_job_complete_mr2 = re.compile("^(\d{2}\/\d{2}\/\d{2} \d{2}:\d{2}:\d{2}) INFO mapreduce.Job: Job job_([\d_]+) completed successfully$")

    """
# MR1 sample
14/06/24 11:18:39 INFO mapred.JobClient: Running job: job_201406241116_0001
14/06/24 11:18:40 INFO mapred.JobClient:  map 0% reduce 0%
...
13/11/21 14:38:55 INFO mapred.JobClient: Job complete: job_201311150128_0050

# MR2 sample
15/04/10 17:20:01 INFO mapreduce.Job: Running job: job_1427781540447_0448
15/04/10 17:20:07 INFO mapreduce.Job: Job job_1427781540447_0448 running in uber mode : false
15/04/10 17:20:07 INFO mapreduce.Job:  map 0% reduce 0%
...
15/04/10 17:20:25 INFO mapreduce.Job: Job job_1427781540447_0448 completed successfully
    """
    flag={}
    with open(benchlog_fn) as f:
        while True:
            line = f.readline().rstrip()
            if not line: break
            for rule in [_spark_stage_submit, _spark_stage_finish, _hadoop_run_job, _hadoop_map_reduce_progress, _hadoop_job_complete_mr1, _hadoop_job_complete_mr2]:
                matched = rule.match(line)
                if matched:
                    result = matched.groups()
                    timestamp = datetime.strptime(result[0], r"%y/%m/%d %H:%M:%S").strftime("%s")+"000" # convert to millsec for js
                    if rule is _spark_stage_submit:
                        events.append("{t},Start {v1} ({v2})".format(t=timestamp, v1=result[1], v2=result[2]))
                    elif rule is _spark_stage_finish:
                        events.append("{t},Finish {v1} ({v2})".format(t=timestamp, v1=result[1], v2=result[2]))
                    elif rule is _hadoop_run_job:
                        events.append("{t},Start Job {v1}".format(t=timestamp, v1=result[1]))
                        flag={}
                    elif rule is _hadoop_map_reduce_progress:
                        map_progress,reduce_progress = int(result[1]), int(result[2])
                        op={'map':False, 'reduce':False}
                        if map_progress == 100:
                            if not "map" in flag:
                                op['map'] = True
                                flag['map'] = True
                        elif reduce_progress>0:
                            if not 'reduce' in flag:
                                op['reduce'] = True
                                flag['reduce'] = True
                        if op['map'] and op['reduce']:
                            events.append("{t},Map finish and Reduce start".format(t=timestamp))
                        elif op['map']:
                            events.append("{t},Map finish".format(t=timestamp))
                        elif op['reduce']:
                            events.append("{t},Reduce start".format(t=timestamp))
                    elif rule is _hadoop_job_complete_mr1 or rule is _hadoop_job_complete_mr2:
                        events.append("{t},Finsih Job {v1}".format(t=timestamp, v1=result[1]))
                    else:
                        assert 0, "should never reach here"


        # limit maximum string length of events
        for i in range(len(events)):
            event_time, event_str = re.split(',', events[i], 1)
            if len(event_str) > 45:
                event_str = event_str[:21]+ '...' + event_str[-21:]
                events[i]="%s,%s" % (event_time, event_str)

        # merge events occurred at sametime:
        i = 1
        while i < len(events)-1:
            cur = events[i].split(',')[0]
            next = events[i+1].split(',')[0]
            if abs(int(cur)/1000 - int(next)/1000) < 1:
                events[i] = events[i] + "<br>" + re.split(',', events[i+1], 1)[1]
                del events[i+1]
                continue
            i += 1
        return events

def generate_report(workload_title, log_fn, benchlog_fn, report_fn):
    c =- 1
    with open(log_fn) as f:
        datas=[eval(x) for x in f.readlines()]

    all_hosts = sorted(list(set([x['hostname'] for x in datas])))
    data_slices = groupby(datas, lambda x:round_to_base(x['timestamp'], PROBE_INTERVAL)) # round to time interval and groupby

    # Generating CSVs
    cpu_heatmap = ["x,y,value,hostname,coreid"]
    cpu_overall = ["x,idle,user,system,iowait,others"]
    network_heatmap = ["x,y,value,hostname,adapterid"]
    network_overall = ["x,recv_bytes,send_bytes,|recv_packets,send_packets,errors"]
    diskio_heatmap = ["x,y,value,hostname,diskid"]
    diskio_overall = ["x,read_bytes,write_bytes,|read_io,write_io"]
    memory_heatmap = ["x,y,value,hostname"]
    memory_overall = ["x,free,buffer_cache,used"]
    procload_heatmap = ["x,y,value,hostname"]
    procload_overall = ["x,load5,load10,load15,|running,procs"]
    events = parse_bench_log(benchlog_fn)

    cpu_count={}
    network_count={}
    diskio_count={}
    memory_count={}
    proc_count={}

    for t, sub_data in data_slices:
        classed_by_host = dict([(x['hostname'], x) for x in sub_data])
        # total cpus, plot user/sys/iowait/other
        data_by_all_hosts = [classed_by_host.get(h, {}) for h in all_hosts]

        # all cpu cores, total cluster
        summed1 = [x['cpu/total'] for x in data_by_all_hosts if x.has_key('cpu/total')]
        if summed1: 
            summed = reduce_patched(lambda a,b: a._add(b), summed1) / len(summed1)
            for x in data_by_all_hosts:
                cpu = x.get('cpu/total', None)
                if not cpu: continue
            # user, system, io, idle, others
#            print t, x['hostname'], cpu.user, cpu.system, cpu.iowait, cpu.idle, cpu.nice+cpu.irq+cpu.softirq
#        print t, summed
            cpu_overall.append("{time},{idle},{user},{system},{iowait},{others}" \
                                   .format(time   = int(t*1000), user = summed.user, system = summed.system, 
                                           iowait = summed.iowait, idle = summed.idle, 
                                           others = summed.nice + summed.irq + summed.softirq))

        # all cpu cores, plot heatmap according to cpus/time/usage(100%-idle)
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefixes(x, "cpu", "!cpu/total").values()):
                try:
                    pos = cpu_count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(cpu_count)
                    cpu_count[(idx, idy, x['hostname'])] = pos
#                print t, pos, 100-y.idle, x['hostname'], y.label
                cpu_heatmap.append("{time},{pos},{value},{host},{cpuid}" \
                                       .format(time = int(t*1000), pos = pos, value = 100-y.idle,
                                               host = x['hostname'], cpuid = y.label))

        # all disk of each node, total cluster
        summed1=[x['disk/total'] for x in data_by_all_hosts if x.has_key('disk/total')]
        if summed1:
            summed = reduce_patched(lambda a,b: a._add(b), summed1)
            for x in data_by_all_hosts:
                disk = x.get('disk/total', None)
                if not disk: continue
            # io-read, io-write, bytes-read, bytes-write
#            print t, x['hostname'], disk.io_read, disk.io_write, disk.bytes_read, disk.bytes_write
 #       print t, summed
            diskio_overall.append("{time},{bytes_read},{bytes_write},{io_read},{io_write}" \
                                      .format(time        = int(t*1000), 
                                              bytes_read  = summed.bytes_read / PROBE_INTERVAL,
                                              bytes_write = summed.bytes_write / PROBE_INTERVAL,
                                              io_read     = summed.io_read / PROBE_INTERVAL,
                                              io_write    = summed.io_write / PROBE_INTERVAL))


        # all disks, plot heatmap according to disks/bytes_read+bytes_write
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefixes(x, "disk", "!disk/total").values()):
                try:
                    pos = diskio_count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(diskio_count)
                    diskio_count[(idx, idy, x['hostname'])] = pos
#                print t, pos, 100-y.idle, x['hostname'], y.label
                diskio_heatmap.append("{time},{pos},{value},{host},{diskid}" \
                                       .format(time   = int(t*1000), 
                                               pos    = pos, 
                                               value  = (y.bytes_read + y.bytes_write) / PROBE_INTERVAL,
                                               host   = x['hostname'], 
                                               diskid = y.label))

        # memory of each node, total cluster
        summed1 = [x['memory/total'] for x in data_by_all_hosts if x.has_key('memory/total')]
        if summed1:
            summed = reduce_patched(lambda a,b: a._add(b), summed1)
            for x in data_by_all_hosts:
                mem = x.get("memory/total", None)
                if not mem: continue
                # mem-total, mem-used, mem-buffer&cache, mem-free, KB
    #            print t, x['hostname'], mem.total, mem.used, mem.buffer_cache, mem.free
            #print t, summed
            memory_overall.append("{time},{free},{buffer_cache},{used}" \
                                      .format(time = int(t*1000),
                                              free = summed.free,
                                              used = summed.used,
                                              buffer_cache = summed.buffer_cache))

        # all memory, plot heatmap according to memory/total - free
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefixes(x, "memory/total").values()):
                try:
                    pos = memory_count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(memory_count)
                    memory_count[(idx, idy, x['hostname'])] = pos
#                print t, pos, 100-y.idle, x['hostname'], y.label
                memory_heatmap.append("{time},{pos},{value},{host}" \
                                       .format(time  = int(t*1000), 
                                               pos   = pos, 
                                               value = (y.total - y.free)*1000,
                                               host  = x['hostname']))


        # proc of each node, total cluster
        summed1 = [x['proc'] for x in data_by_all_hosts if x.has_key('proc')]
        if summed1: 
            summed = reduce_patched(lambda a,b: a._add(b), summed1)
            for x in data_by_all_hosts:
                procs = x.get("proc", None)
                if not procs: continue
            procload_overall.append("{time},{load5},{load10},{load15},{running},{procs}"\
                                        .format(time   = int(t*1000),
                                                load5  = summed.load5,load10=summed.load10,
                                                load15 = summed.load15,running=summed.running,
                                                procs  = summed.procs))
        
        # all nodes' proc, plot heatmap according to proc/proc.procs
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefixes(x, "proc").values()):
                try:
                    pos = proc_count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(proc_count)
                    proc_count[(idx, idy, x['hostname'])] = pos
#                print t, pos, 100-y.idle, x['hostname'], y.label
                procload_heatmap.append("{time},{pos},{value},{host}" \
                                            .format(time = int(t*1000), pos = pos, value = y.procs,
                                                    host = x['hostname']))

        # all network interface, total cluster
        summed1 = [x['net/total'] for x in data_by_all_hosts if x.has_key('net/total')]

        if summed1: 
            summed = reduce_patched(lambda a,b: a._add(b), summed1)
            for x in data_by_all_hosts:
                net = x.get("net/total", None)
                if not net: continue
                # recv-byte, send-byte, recv-packet, send-packet, errors
    #            print t, x['hostname'], net.recv_bytes, net.send_bytes, net.recv_packets, net.send_packets, net.recv_errs+net.send_errs+net.recv_drop+net.send_drop
    #        print t, summed
            network_overall.append("{time},{recv_bytes},{send_bytes},{recv_packets},{send_packets},{errors}" \
                                       .format(time         = int(t*1000),
                                               recv_bytes   = summed.recv_bytes / PROBE_INTERVAL, 
                                               send_bytes   = summed.send_bytes / PROBE_INTERVAL,
                                               recv_packets = summed.recv_packets / PROBE_INTERVAL, 
                                               send_packets = summed.send_packets / PROBE_INTERVAL,
                                               errors = (summed.recv_errs + summed.send_errs + \
                                                             summed.recv_drop + summed.send_drop) / PROBE_INTERVAL)
                                   )

        # all network adapters, plot heatmap according to net/recv_bytes + send_bytes
        for idx, x in enumerate(data_by_all_hosts):
            for idy, y in enumerate(filter_dict_with_prefixes(x, "net", "!net/total").values()):
                try:
                    pos = network_count[(idx, idy, x['hostname'])]
                except:
                    pos =  len(network_count)
                    network_count[(idx, idy, x['hostname'])] = pos
                network_heatmap.append("{time},{pos},{value},{host},{networkid}" \
                                       .format(time  = int(t*1000), 
                                               pos   = pos*2, 
                                               value = y.recv_bytes / PROBE_INTERVAL,
                                               host  = x['hostname'], 
                                               networkid = y.label+".recv"))
                network_heatmap.append("{time},{pos},{value},{host},{networkid}" \
                                       .format(time  = int(t*1000), 
                                               pos   = pos*2+1, 
                                               value = y.send_bytes / PROBE_INTERVAL,
                                               host  = x['hostname'], 
                                               networkid = y.label+".send"))
        
    with open(samedir("chart-template.html")) as f:
        template = f.read()
    
    variables = locals()
    def my_replace(match):
        match = match.group()[1:-1]
        if match.endswith('heatmap') or match.endswith('overall'):
            return "\n".join(variables[match])
        elif match =='events':
            return "\n".join(events)
        elif match == 'probe_interval':
            return str(PROBE_INTERVAL * 1000)
        elif match == 'workload_name':
            return workload_title
        else:
            return '{%s}' % match
            
    with open(report_fn, 'w') as f:
        f.write(re.sub(r'{\w+}', my_replace, template))

def show_usage():
    log("""Usage:
    monitor.py <workload_title> <parent_pid> <log_path.log> <benchlog_fn.log> <report_path.html> <monitor_node_name1> ... <monitor_node_nameN>
""")

if __name__=="__main__":
    if len(sys.argv)<6:
        log(sys.argv)
        show_usage()
        sys.exit(1)

#    log(sys.argv)
    global log_path
    global report_path
    global workload_title
    global bench_log_path
    global na

    workload_title = sys.argv[1]
    parent_pid = sys.argv[2]
    log_path = sys.argv[3]
    bench_log_path = sys.argv[4]
    report_path = sys.argv[5]
    nodes_to_monitor = sys.argv[6:]
    pid=os.fork()
    if pid:                               #parent
        print pid
    else:                                 #child
        os.close(0)
        os.close(1)
        os.close(2)
#        log("child process start")
        signal.signal(signal.SIGTERM, sig_term_handler)
        start_monitor(log_path, nodes_to_monitor)
        while  os.path.exists("/proc/%s" % parent_pid):
            sleep(1)
        # parent lost, stop!
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        na.stop()
        generate_report(workload_title, log_path, bench_log_path, report_path)
