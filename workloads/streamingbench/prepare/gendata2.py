#!/usr/bin/env python2
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

"Gen data2 for streaming bench"

import random, sys
r=random.randrange
fmt_str = "%12d"*16 + '\n'
if len(sys.argv)<2:
    print "Usage:\n   gendate2.py <output filename>"
fn = sys.argv[1]
with open(fn, 'w') as f:
    for i in range(10):
        for j in range(1400):
            f.write(fmt_str % (j, r(2000000), r(100), r(100000), r(300), r(3000), 
                               r(400000), r(30000), r(500000), r(2000), r(300000),
                               r(50000), r(300000), r(30000), r(40000), r(20000)))
