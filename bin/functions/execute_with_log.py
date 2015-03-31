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

def load_colors():
    color_script_fn = os.path.join(os.path.dirname(__file__), "color.enabled.sh")
    with open(color_script_fn) as f:
        return {k:v.split("'")[1].replace('\e[', "\033[") for k,v in [x.strip().split('=') for x in f.readlines() if x.strip() and not x.strip().startswith('#')]}

Color=load_colors()

def execute(workload_result_folder, command_lines):
    proc = subprocess.Popen(" ".join(command_lines), shell=True, bufsize=1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    count=100
    last_time=0
    while True:
        count+=1
        if count>100 or time()-last_time>1: # refresh terminal size for 100 lines or each seconds
            count, last_time = 0, time()
            width, height = get_terminal_size()

        line = proc.stdout.readline().rstrip()
        line = line.decode('utf-8')
        if not line: break
        #print "{Red}log=>{Color_Off}".format(**Color), line
        lline = line.lower()
        if "warn" in lline or 'err' in lline:
            print u"{Red}{line}{Color_Off}{ClearEnd}".format(line=line,**Color)
        else:
            if len(line) >= width:
                line = line[:width-4]+'...'
            print u"{line}{ClearEnd}\r".format(line=line, **Color),
            sys.stdout.flush()
    print
    proc.wait()
    return proc.returncode
if __name__=="__main__":
    sys.exit(execute(workload_result_folder=sys.argv[1],
                     command_lines=sys.argv[2:]))