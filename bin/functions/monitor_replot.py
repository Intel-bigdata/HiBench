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

from monitor import generate_report
import sys

if len(sys.argv)<4:
    print """Usage:
    monitor_replot.py <workload_name> <log_path.log> <bench_log_path.log> <report_path.html>
"""
    sys.exit(1)

generate_report(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
