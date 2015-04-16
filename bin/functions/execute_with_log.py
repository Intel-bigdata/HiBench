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

import sys, os, subprocess
from terminalsize import get_terminal_size
from time import time, sleep
import re

def load_colors():
    color_script_fn = os.path.join(os.path.dirname(__file__), "color.enabled.sh")
    with open(color_script_fn) as f:
        return dict([(k,v.split("'")[1].replace('\e[', "\033[")) for k,v in [x.strip().split('=') for x in f.readlines() if x.strip() and not x.strip().startswith('#')]])

Color=load_colors()

tab_matcher = re.compile("\t")
tabstop = 8
def replace_tab_to_space(s):
    def tab_replacer(match):
        pos = match.start()
        length = pos % tabstop
        if not length: length += tabstop
        return " " * length
    return tab_matcher.sub(tab_replacer, s)

class _Matcher:
    hadoop = re.compile(r"^.*map\s*=\s*(\d+)%,\s*reduce\s*=\s*(\d+)%.*$")
    hadoop2 = re.compile(r"^.*map\s+\s*(\d+)%\s+reduce\s+\s*(\d+)%.*$")
    spark = re.compile(r"^.*finished task \S+ in stage \S+ \(tid \S+\) in.*on.*\((\d+)/(\d+)\)\s*$")
    def match(self, line):
        for p in [self.hadoop, self.hadoop2]:
            m = p.match(line)
            if m:
                return (float(m.groups()[0]) + float(m.groups()[1]))/2

        for p in [self.spark]:
            m = p.match(line)
            if m:
                return float(m.groups()[0]) / float(m.groups()[1]) * 100
        
matcher = _Matcher()

def show_with_progress_bar(line, progress, line_width):
    """
    Show text with progress bar.

    @progress:0-100
    @line: text to show
    @line_width: width of screen
    """
    pos = int(line_width * progress / 100)
    if len(line) < line_width:
        line = line + " " * (line_width - len(line))
    line = "{On_Yellow}{line_seg1}{On_Blue}{line_seg2}{Color_Off}\r".format(
        line_seg1 = line[:pos], line_seg2 = line[pos:], **Color)
    sys.stdout.write(line)

def execute(workload_result_file, command_lines):
    proc = subprocess.Popen(" ".join(command_lines), shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    count = 100
    last_time=0
    log_file = open(workload_result_file, 'w')
    while True:
        count += 1
        if count > 100 or time()-last_time>1: # refresh terminal size for 100 lines or each seconds
            count, last_time = 0, time()
            width, height = get_terminal_size()
            width -= 1

        try:
            line = proc.stdout.readline().rstrip()
            log_file.write(line+"\n")
            log_file.flush()
        except KeyboardInterrupt:
            proc.terminate()
            break
        line = line.decode('utf-8')
        if not line: break
        line = replace_tab_to_space(line)
        #print "{Red}log=>{Color_Off}".format(**Color), line
        lline = line.lower()
        if ("warn" in lline or 'err' in lline or 'exception' in lline) and lline.lstrip() == lline:
            COLOR="Yellow" if "warn" in lline else "Red"
            sys.stdout.write((u"{%s}{line}{Color_Off}{ClearEnd}\n" % COLOR).format(line=line,**Color).encode('utf-8'))
            
        else:
            if len(line) >= width:
                line = line[:width-4]+'...'
            progress = matcher.match(lline)
            if progress is not None:
                show_with_progress_bar(line, progress, width)
            else:
                sys.stdout.write(u"{line}{ClearEnd}\r".format(line=line, **Color).encode('utf-8'))
        sys.stdout.flush()
    print
    log_file.close()
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.kill()
        return 1
    return proc.returncode

def test_progress_bar():
    for i in range(101):
        show_with_progress_bar("test progress : %d" % i, i, 80)
        sys.stdout.flush()

        sleep(0.05)

if __name__=="__main__":
    sys.exit(execute(workload_result_file=sys.argv[1],
                     command_lines=sys.argv[2:]))
#    test_progress_bar()
