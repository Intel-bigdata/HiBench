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

import sys, os, glob, re
from collections import defaultdict
from hibench_prop_env_mapping import HiBenchEnvPropMappingMandatory, HiBenchEnvPropMapping, HiBenchPropEnvMapping, HiBenchPropEnvMappingMandatory

HibenchConf={}
HibenchConfRef={}

def log(s):
    sys.stderr.write(s+'\n')

def shell(cmd):
    return os.popen(cmd).read()

def OneAndOnlyOne(filename_pattern):
    files = glob.glob(filename_pattern)
    if len(files)==1: return files[0]
    else:
        log('This filename pattern "%s" is required to match only one file.' % filename_pattern)
        if len(files)==0:
            log("However, there's no file found, please fix it.")
        else:
            log("However, there's several files found, please remove the redundant files:\n", "\n".join(files))
        raise Exception("Need to match one and only one file!")

def load_config(conf_root, workload_root, workload_folder):
    abspath = os.path.abspath
    conf_root = abspath(conf_root)
    workload_root = abspath(workload_root)
    workload_folder = abspath(workload_folder)
    workload_tail = os.path.dirname(workload_folder[len(workload_root):])[1:]
    workload_name = os.path.basename(workload_root)

    conf_files = sorted(glob.glob(conf_root+"/*.conf")) + \
        sorted(glob.glob("%s/workloads/%s/*.conf" % (conf_root, workload_name))) + \
        sorted(glob.glob("%s/workloads/%s/%s/*.conf" % (conf_root, workload_name, workload_tail)))
    
    # load values from conf files
    for filename in conf_files:
        log("Parsing conf: %s" % filename)
        with open(filename) as f:
            for line in f.readlines():
                line = line.strip()
                if not line: continue     # skip empty lines
                if line[0]=='#': continue # skip comments
                key, value = re.split("\s", line, 1)
                HibenchConf[key] = value.strip()
                HibenchConfRef[key] = filename

    # override values from os environment variable settings
    for env_name, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
        if env_name in os.environ:
            env_value = os.getenv(env_name)
            HibenchConf[prop_name] = env_value
            HibenchConfRef[prop_name] = "OS environment variable:%s" % env_name

    # generate ref values
    waterfall_config()
    # generate auto probe values
    generate_optional_value()
    # generate ref values again to ensure all values can be found
    waterfall_config()
    # check
    check_config()
    # Export config to file, let bash script to import as local variables.
    print export_config(workload_name, workload_tail)

def check_config():             # check configures
    # Ensure mandatory configures are available
    for _, prop_name in HiBenchEnvPropMappingMandatory.items():
        assert HibenchConf.get(prop_name, ""), "Mandatory configure missing: %s" % prop_name
    # Ensure all ref values in configure has been expanded
    for _, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
        assert "${" not in HibenchConf.get(prop_name, ""), "Unsolved ref key: %s. defined at %s" % (prop_name, HibenchConfRef.get(prop_name, "unknown"))

def waterfall_config():         # replace "${xxx}" to its values
    def process_replace(m):
        raw_key = m.groups()[0]
        key = raw_key[2:-1].strip()
        return HibenchConf.get(key, raw_key)
    p = re.compile("(\$\{\s*[^\s]+\s*\})")

    finish = False
    while not finish:
        finish = True
        for key, value in HibenchConf.items():
            old_value = value
            value = p.sub(process_replace, value)
            if value != old_value: # we have updated value, try again
                log("Waterfall conf: %s: %s -> %s" % (key, old_value, value))
                HibenchConf[key] = value
                finish = False

def generate_optional_value():  # get values from environment or make a guess
    d = os.path.dirname
    HibenchConf['hibench.home']=d(d(d(os.path.abspath(__file__))))
    del d
    HibenchConfRef['hibench.home']="Inferred from %s" % __file__
    version=""
    if not HibenchConf.get("hibench.hadoop.version", ""):
        if not version:
            version = shell(HibenchConf['hibench.hadoop.executable'] +' version | head -1 | cut -d \    -f 2')
        HibenchConf["hibench.hadoop.version"] = "hadoop"+version[0]
        HibenchConfRef["hibench.hadoop.version"]="Probed by: " + HibenchConf['hibench.hadoop.executable'] +' version | head -1 | cut -d \    -f 2'
    if not HibenchConf.get("hibench.hadoop.release", ""):
        if not version:
            version = shell(HibenchConf['hibench.hadoop.executable'] +' version | head -1 | cut -d \    -f 2')
        HibenchConf["hibench.hadoop.release"]= \
            "cdh4" if "cdh4" in version else \
            "cdh5" if "cdh5" in version else \
            HibenchConf["hibench.hadoop.version"]
        HibenchConfRef["hibench.hadoop.version"]="Probed by: " + HibenchConf['hibench.hadoop.executable'] +' version | head -1 | cut -d \    -f 2'
    if not HibenchConf.get("hibench.hadoop.examples.jar", ""):
        if HibenchConf["hibench.hadoop.version"] == "hadoop1":
            HibenchConf["hibench.hadoop.examples.jar"] = OneAndOnlyOne(HibenchConf['hibench.hadoop.home']+"/hadoop-examples*.jar")
            HibenchConfRef["hibench.hadoop.examples.jar"]= "Probed by: " + HibenchConf['hibench.hadoop.home']+"/hadoop-examples*.jar"
        else:
            raise Exception("FIXME! for hadoop2, use a HADOOP_JOBCLIENT_TESTS_JAR as hadoop.example.jar")

def export_config(workload_name, workload_tail):
    report_dir = HibenchConf['hibench.report.dir']
    conf_dir = os.path.join(report_dir, workload_name, workload_tail, 'conf')
    conf_filename= os.path.join(conf_dir, "%s.conf" % workload_name)
    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)
    sources=defaultdict(list)
    for env_name, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
        source = HibenchConfRef.get(prop_name, 'None')
        sources[source].append('%s=%s' % (env_name, HibenchConf.get(prop_name, '')))

    with open(conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            f.write("# Source: %s\n" % source)
            f.write("\n".join(sorted(sources[source])))
            f.write("\n\n")
        
    return conf_filename

if __name__=="__main__":
    if len(sys.argv)<4:
        raise Exception("Please supply <conf root path>, <workload root path>, <workload folder path>")
    conf_root, workload_root, workload_folder = sys.argv[1], sys.argv[2], sys.argv[3]
    load_config(conf_root, workload_root, workload_folder)
