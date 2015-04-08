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

#FIXME: use log helper later
def log(*s):
    if len(s)==1: s=s[0]
    else: s= " ".join([str(x) for x in s])
    sys.stderr.write( str(s) +'\n')

def log_debug(*s):
    #log(*s)
    pass

def shell(cmd):
    return os.popen(cmd).read()

def OneAndOnlyOneFile(filename_pattern):
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
    workload_tail = workload_folder[len(workload_root):][1:]
    workload_api = os.path.dirname(workload_tail) if os.path.dirname(workload_tail) else workload_tail
    workload_name = os.path.basename(workload_root)

    conf_files = sorted(glob.glob(conf_root+"/*.conf")) + \
        sorted(glob.glob("%s/conf/*.conf" % (workload_root,))) + \
        sorted(glob.glob("%s/%s/*.conf" % (workload_root, workload_api)))

    # load values from conf files
    for filename in conf_files:
        log("Parsing conf: %s" % filename)
        with open(filename) as f:
            for line in f.readlines():
                line = line.strip()
                if not line: continue     # skip empty lines
                if line[0]=='#': continue # skip comments
                try:
                    key, value = re.split("\s", line, 1)
                except ValueError:
                    key = line.strip()
                    value = ""
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
    waterfall_config(force=True)
    # check
    check_config()
    # Export config to file, let bash script to import as local variables.
    print export_config(workload_name, workload_api)

def check_config():             # check configures
    # Ensure mandatory configures are available
    for _, prop_name in HiBenchEnvPropMappingMandatory.items():
        assert HibenchConf.get(prop_name, None) is not None, "Mandatory configure missing: %s" % prop_name
    # Ensure all ref values in configure has been expanded
    for _, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
        assert "${" not in HibenchConf.get(prop_name, ""), "Unsolved ref key: %s. \n    Defined at %s:\n    Unsolved value:%s\n" % (prop_name,
                                                                                                                              HibenchConfRef.get(prop_name, "unknown"),
                                                                                                                              HibenchConf.get(prop_name, "unknown"))

def waterfall_config(force=False):         # replace "${xxx}" to its values
    def process_replace(m):
        raw_key = m.groups()[0]
        key = raw_key[2:-1].strip()
        log_debug("key:", key, " value:", HibenchConf.get(key, "RAWKEY:"+raw_key))
        if force:
            return HibenchConf.get(key, raw_key)
        else:
            return HibenchConf.get(key, "") or raw_key

    p = re.compile("(\$\{\s*[^\s^\$^\}]+\s*\})")

    finish = False
    while not finish:
        finish = True
        for key, value in HibenchConf.items():
            old_value = value
            value = p.sub(process_replace, value)
            if value != old_value: # we have updated value, try again
#                log("Waterfall conf: %s: %s -> %s" % (key, old_value, value))
                HibenchConf[key] = value
                finish = False

def generate_optional_value():  # get some critical values from environment or make a guess
    d = os.path.dirname
    join = os.path.join
    HibenchConf['hibench.home']=d(d(d(os.path.abspath(__file__))))
    del d
    HibenchConfRef['hibench.home']="Inferred from relative path of dirname(%s)/../../" % __file__

    # probe hadoop version & release.
    if not HibenchConf.get("hibench.hadoop.version", "") or not HibenchConf.get("hibench.hadoop.release", ""):
        # check hadoop version first
        hadoop_version = ""
        cmd = HibenchConf['hibench.hadoop.executable'] +' version | head -1 | cut -d \    -f 2'
        if not HibenchConf.get("hibench.hadoop.version", ""):
            hadoop_version = shell(cmd).strip()
            if hadoop_version[0] != '1': # hadoop2? or CDH's MR1?
                cmd2 = HibenchConf['hibench.hadoop.executable'] + " mradmin 2>&1 | grep yarn"
                mradm_result = shell(cmd2).strip()
                
                if mradm_result: # match with keyword "yarn", must be CDH's MR2, do nothing
                    pass
                else:           # didn't match with "yarn", however it calms as hadoop2, must be CDH's MR1
                    HibenchConf["hibench.hadoop.version"] = "hadoop1"
                    HibenchConfRef["hibench.hadoop.version"] = "Probed by: `%s` and `%s`" % (cmd, cmd2)
            if not HibenchConf.get("hibench.hadoop.version", ""):
                HibenchConf["hibench.hadoop.version"] = "hadoop" + hadoop_version[0]
                HibenchConfRef["hibench.hadoop.version"] = "Probed by: " + cmd
                
        assert HibenchConf["hibench.hadoop.version"] in ["hadoop1", "hadoop2"], "Unknown hadoop version (%s). Auto probe failed, please override `hibench.hadoop.version` to explicitly define this property" % HibenchConf["hibench.hadoop.version"]

        # check hadoop release
        if not HibenchConf.get("hibench.hadoop.release", ""):
            if not hadoop_version:
                hadoop_version = shell(cmd).strip()
            HibenchConf["hibench.hadoop.release"] = \
                "cdh4" if "cdh4" in hadoop_version else \
                "cdh5" if "cdh5" in hadoop_version else \
                "apache" if "hadoop" in HibenchConf["hibench.hadoop.version"] else \
                "UNKNOWN"
            HibenchConfRef["hibench.hadoop.release"] = "Inferred by: hadoop version, which is:\"%s\"" % hadoop_version

        assert HibenchConf["hibench.hadoop.release"] in ["cdh4", "cdh5", "apache"],  "Unknown hadoop release. Auto probe failed, please override `hibench.hadoop.release` to explicitly define this property"


    # probe spark version
    if not HibenchConf.get("hibench.spark.version", ""):
        spark_home = HibenchConf.get("hibench.spark.home", "")
        assert spark_home, "`hibench.spark.home` undefined, please fix it and retry"
        try:
            release_file = join(spark_home, "RELEASE")
            with open(release_file) as f:
                spark_version_raw = f.readlines()[0]
                #spark_version_raw="Spark 1.2.2-SNAPSHOT (git revision f9d8c5e) built for Hadoop 1.0.4\n"      # version sample
                spark_version = spark_version_raw.split()[1].strip()
                HibenchConfRef["hibench.spark.version"] = "Probed from file %s, parsed by value:%s" % (release_file, spark_version_raw)
        except IOError as e:    # no release file, fall back to hard way
            log("Probing spark verison, may last long time...")
            shell_cmd = '( cd %s; mvn help:evaluate -Dexpression=project.version 2> /dev/null | grep -v "INFO" | tail -n 1)' % spark_home
            spark_version = shell(shell_cmd).strip()
            HibenchConfRef["hibench.spark.version"] = "Probed by shell command: %s, value: %s" % (shell_cmd, spark_version)

        assert spark_version, "Spark version probe failed, please override `hibench.spark.version` to explicitly define this property"
        HibenchConf["hibench.spark.version"] = "spark" + spark_version[:3]

    # probe hadoop example jars
    if not HibenchConf.get("hibench.hadoop.examples.jar", ""):
        if HibenchConf["hibench.hadoop.version"] == "hadoop1": # MR1
            if HibenchConf['hibench.hadoop.release'] == 'apache': # Apache release
                HibenchConf["hibench.hadoop.examples.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home']+"/hadoop-examples*.jar")
                HibenchConfRef["hibench.hadoop.examples.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/hadoop-examples*.jar"
            elif HibenchConf['hibench.hadoop.release'].startswith('cdh'): # CDH release
                HibenchConf["hibench.hadoop.examples.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce1/hadoop-examples*.jar")
                HibenchConfRef["hibench.hadoop.examples.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce1/hadoop-examples*.jar"
        else:                   # MR2
            if HibenchConf['hibench.hadoop.release'] == 'apache': # Apache release
                HibenchConf["hibench.hadoop.examples.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home'] + "/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar")
                HibenchConfRef["hibench.hadoop.examples.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar"
            elif HibenchConf['hibench.hadoop.release'].startswith('cdh'): # CDH release
                HibenchConf["hibench.hadoop.examples.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home'] + "/share/hadoop/mapreduce2/hadoop-mapreduce-examples-*.jar")
                HibenchConfRef["hibench.hadoop.examples.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce2/hadoop-mapreduce-examples-*.jar"

    # probe hadoop examples test jars (for sleep in hadoop2 only)
    if not HibenchConf.get("hibench.hadoop.examples.test.jar", ""):
        if HibenchConf["hibench.hadoop.version"] == "hadoop1" and HibenchConf["hibench.hadoop.release"] == "apache":
            HibenchConf["hibench.hadoop.examples.test.jar"] = "dummy"
            HibenchConfRef["hibench.hadoop.examples.test.jar"]= "Dummy value, not available in hadoop1"
        else:
            if HibenchConf['hibench.hadoop.release'] == 'apache':
                HibenchConf["hibench.hadoop.examples.test.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home'] + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient*-tests.jar")
                HibenchConfRef["hibench.hadoop.examples.test.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient*-tests.jar"
            elif HibenchConf['hibench.hadoop.release'].startswith('cdh'):
                if HibenchConf["hibench.hadoop.version"] == "hadoop2":
                    HibenchConf["hibench.hadoop.examples.test.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home'] + "/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient*-tests.jar")
                    HibenchConfRef["hibench.hadoop.examples.test.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient*-tests.jar"
                elif HibenchConf["hibench.hadoop.version"] == "hadoop1":
                    HibenchConf["hibench.hadoop.examples.test.jar"] = OneAndOnlyOneFile(HibenchConf['hibench.hadoop.home'] + "/share/hadoop/mapreduce1/hadoop-examples-*.jar")
                    HibenchConfRef["hibench.hadoop.examples.test.jar"]= "Inferred by: " + HibenchConf['hibench.hadoop.home']+"/share/hadoop/mapreduce1/hadoop-mapreduce-client-jobclient*-tests.jar"            

    # set hibench.sleep.job.jar
    if not HibenchConf.get('hibench.sleep.job.jar', ''):
        if HibenchConf['hibench.hadoop.release'] == 'apache' and HibenchConf["hibench.hadoop.version"] == "hadoop1":
            HibenchConf["hibench.sleep.job.jar"] = HibenchConf['hibench.hadoop.examples.jar']
            HibenchConfRef["hibench.sleep.job.jar"] = "Refer to `hibench.hadoop.examples.jar` according to the evidence of `hibench.hadoop.release` and `hibench.hadoop.version`"
        else:
#            log("probe sleep jar:", HibenchConf['hibench.hadoop.examples.test.jar'])
            HibenchConf["hibench.sleep.job.jar"] = HibenchConf['hibench.hadoop.examples.test.jar']
            HibenchConfRef["hibench.sleep.job.jar"] = "Refer to `hibench.hadoop.examples.test.jar` according to the evidence of `hibench.hadoop.release` and `hibench.hadoop.version`"

    # probe hadoop configuration files
    if not HibenchConf.get("hibench.hadoop.configure.dir", ""):
        if HibenchConf["hibench.hadoop.release"] == "apache": # Apache release
            HibenchConf["hibench.hadoop.configure.dir"] = join(HibenchConf["hibench.hadoop.home"], "conf") if HibenchConf["hibench.hadoop.version"] == "hadoop1" \
                else join(HibenchConf["hibench.hadoop.home"], "etc", "hadoop")
            HibenchConfRef["hibench.hadoop.configure.dir"] = "Inferred by: 'hibench.hadoop.version' & 'hibench.hadoop.release'"
        elif HibenchConf["hibench.hadoop.release"].startswith("cdh"): # CDH release
            HibenchConf["hibench.hadoop.configure.dir"] = join(HibenchConf["hibench.hadoop.home"], "etc", "hadoop-mapreduce1") if HibenchConf["hibench.hadoop.version"] == "hadoop1" \
                else join(HibenchConf["hibench.hadoop.home"], "etc", "hadoop")
            HibenchConfRef["hibench.hadoop.configure.dir"] = "Inferred by: 'hibench.hadoop.version' & 'hibench.hadoop.release'"

    # set hadoop mapper/reducer property names
    if not HibenchConf.get("hibench.hadoop.mapper.name", ""):
        HibenchConf["hibench.hadoop.mapper.name"] = "mapred.map.tasks" if HibenchConf["hibench.hadoop.version"] == "hadoop1" else "mapreduce.job.maps"
        HibenchConfRef["hibench.hadoop.mapper.name"] = "Inferred by: 'hibench.hadoop.version'"
    if not HibenchConf.get("hibench.hadoop.reducer.name", ""):
        HibenchConf["hibench.hadoop.reducer.name"] = "mapred.reduce.tasks" if HibenchConf["hibench.hadoop.version"] == "hadoop1" else "mapreduce.job.reduces"
        HibenchConfRef["hibench.hadoop.reducer.name"] = "Inferred by: 'hibench.hadoop.version'"

    # probe masters, slaves hostnames
    # determine running mode according to spark master
    spark_master = HibenchConf['hibench.spark.master']
    if spark_master.startswith("local"):   # local mode
        HibenchConf['hibench.masters.hostnames'] = ''             # no master
        HibenchConf['hibench.slaves.hostnames'] = 'localhost'     # localhost as slaves
        HibenchConfRef['hibench.masters.hostnames'] = HibenchConfRef['hibench.slaves.hostnames'] = "Probed by the evidence of 'hibench.spark.master=%s'" % spark_master
    elif spark_master.startswith("spark"):   # spark standalone mode
        HibenchConf['hibench.masters.hostnames'] = spark_master.lstrip("spark://").split(":")[0]
        HibenchConfRef['hibench.masters.hostnames'] = HibenchConfRef['hibench.slaves.hostnames'] = "Probed by the evidence of 'hibench.spark.master=%s'" % spark_master

        
def export_config(workload_name, workload_tail):
    join = os.path.join
    report_dir = HibenchConf['hibench.report.dir']
    conf_dir = join(report_dir, workload_name, workload_tail, 'conf')
    conf_filename= join(conf_dir, "%s.conf" % workload_name)

    spark_conf_dir = join(conf_dir, "sparkbench")
    spark_prop_conf_filename = join(spark_conf_dir, "spark.conf")
    sparkbench_prop_conf_filename = join(spark_conf_dir, "sparkbench.conf")

    if not os.path.exists(spark_conf_dir):      os.makedirs(spark_conf_dir)
    if not os.path.exists(conf_dir):      os.makedirs(conf_dir)

    # generate configure for hibench
    sources=defaultdict(list)
    for env_name, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
        source = HibenchConfRef.get(prop_name, 'None')
        sources[source].append('%s=%s' % (env_name, HibenchConf.get(prop_name, '')))

    with open(conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            f.write("# Source: %s\n" % source)
            f.write("\n".join(sorted(sources[source])))
            f.write("\n\n")
        f.write("#Source: add for internal usage\n")
        f.write("SPARKBENCH_PROPERTIES_FILES=%s\n" % sparkbench_prop_conf_filename)
        f.write("SPARK_PROP_CONF=%s\n" % spark_prop_conf_filename)
        f.write("WORKLOAD_RESULT_FOLDER=%s\n" % join(conf_dir, ".."))
        f.write("HIBENCH_WORKLOAD_CONF=%s\n" % conf_filename)
        f.write("export HADOOP_EXECUTABLE\n")
        f.write("export HADOOP_CONF_DIR\n")

    # generate properties for spark & sparkbench
    sources=defaultdict(list)
    for prop_name, prop_value in HibenchConf.items():
        source = HibenchConfRef.get(prop_name, 'None')
        sources[source].append('%s\t%s' % (prop_name, prop_value))
    # generate configure for sparkbench
    with open(spark_prop_conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            items = [x for x in sources[source] if x.startswith("spark.")]
            if items:
                f.write("# Source: %s\n" % source)
                f.write("\n".join(sorted(items)))
                f.write("\n\n")
    # generate configure for spark
    with open(sparkbench_prop_conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            items = [x for x in sources[source] if x.startswith("sparkbench.") or x.startswith("hibench.")]
            if items:
                f.write("# Source: %s\n" % source)
                f.write("\n".join(sorted(items)))
                f.write("\n\n")

    return conf_filename

if __name__=="__main__":
    if len(sys.argv)<4:
        raise Exception("Please supply <conf root path>, <workload root path>, <workload folder path>")
    conf_root, workload_root, workload_folder = sys.argv[1], sys.argv[2], sys.argv[3]
    load_config(conf_root, workload_root, workload_folder)
