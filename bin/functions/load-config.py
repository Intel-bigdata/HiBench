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

import sys
import glob
import re
import urllib
import socket
from contextlib import closing
from collections import defaultdict
from hibench_prop_env_mapping import HiBenchEnvPropMappingMandatory, HiBenchEnvPropMapping

HibenchConf = {}
HibenchConfRef = {}

# FIXME: use log helper later


def log(*s):
    if len(s) == 1:
        s = s[0]
    else:
        s = " ".join([str(x) for x in s])
    sys.stderr.write(str(s) + '\n')


def log_debug(*s):
    #    log(*s)
    pass

# copied from http://stackoverflow.com/questions/3575554/python-subprocess-with-timeout-and-large-output-64k
# Comment: I have a better solution, but I'm too lazy to write.
import fcntl
import os
import subprocess
import time


def nonBlockRead(output):
    fd = output.fileno()
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
    try:
        return output.read()
    except:
        return ''


def execute_cmd(cmdline, timeout):
    """
    Execute cmdline, limit execution time to 'timeout' seconds.
    Uses the subprocess module and subprocess.PIPE.

    Raises TimeoutInterrupt
    """

    p = subprocess.Popen(
        cmdline,
        bufsize=0,  # default value of 0 (unbuffered) is best
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    t_begin = time.time()  # Monitor execution time
    seconds_passed = 0

    stdout = ''
    stderr = ''

    while p.poll() is None and (
            seconds_passed < timeout or timeout == 0):  # Monitor process
        time.sleep(0.1)  # Wait a little
        seconds_passed = time.time() - t_begin

        stdout += nonBlockRead(p.stdout)
        stderr += nonBlockRead(p.stderr)

    if seconds_passed >= timeout and timeout > 0:
        try:
            p.stdout.close()  # If they are not closed the fds will hang around until
            p.stderr.close()  # os.fdlimit is exceeded and cause a nasty exception
            p.terminate()     # Important to close the fds prior to terminating the process!
            # NOTE: Are there any other "non-freed" resources?
        except:
            pass

        return ('Timeout', stdout, stderr)

    return (p.returncode, stdout, stderr)


def shell(cmd, timeout=5):
    assert not "${" in cmd, "Error, missing configurations: %s" % ", ".join(
        re.findall("\$\{(.*)\}", cmd))
    retcode, stdout, stderr = execute_cmd(cmd, timeout)
    if retcode == 'Timeout':
        log("ERROR, execute cmd: '%s' timedout." % cmd)
        log("  STDOUT:\n" + stdout)
        log("  STDERR:\n" + stderr)
        log("  Please check!")
        assert 0, cmd + " executed timedout for %d seconds" % timeout

    return stdout


def exactly_one_file(filename_candidate_list):
    for filename_pattern in filename_candidate_list:
        result = exactly_one_file_one_candidate(filename_pattern)
        if result != "":
            return result
    assert 0, "The pattern " + filename_pattern + \
        " matches no files, please set `hibench.hadoop.examples.jar` manually"


def exactly_one_file_one_candidate(filename_pattern):
    files = glob.glob(filename_pattern)
    if len(files) == 0:
        return ""
    elif len(files) == 1:
        return files[0]
    else:
        assert 0, "The pattern " + filename_pattern + \
            " matches more than one file, please remove the redundant files"


def load_config(
        conf_root,
        workload_config_file,
        workload_name,
        patching_config=""):
    abspath = os.path.abspath
    conf_root = abspath(conf_root)
    workload_config_file = abspath(workload_config_file)

    conf_files = sorted(glob.glob(conf_root + "/*.conf")) + \
        sorted(glob.glob(workload_config_file))

    # load values from conf files
    for filename in conf_files:
        log("Parsing conf: %s" % filename)
        with open(filename) as f:
            for line in f.readlines():
                line = line.strip()
                if not line:
                    continue     # skip empty lines
                if line[0] == '#':
                    continue  # skip comments
                try:
                    key, value = re.split("\s", line, 1)
                except ValueError:
                    key = line.strip()
                    value = ""
                HibenchConf[key] = value.strip()
                HibenchConfRef[key] = filename

    # override values from os environment variable settings
    # for env_name, prop_name in HiBenchEnvPropMappingMandatory.items() + HiBenchEnvPropMapping.items():
    #     if env_name in os.environ:
    #         env_value = os.getenv(env_name)
    #         HibenchConf[prop_name] = env_value
    #         HibenchConfRef[prop_name] = "OS environment variable:%s" % env_name

    # override values by patching config
    for item in [x for x in patching_config.split(',') if x]:
        key, value = re.split('=', item, 1)
        HibenchConf[key] = value.strip()
        HibenchConfRef[
            key] = "Overrided by parent script during calling: " + item

    # generate ref values
    waterfall_config()
    # generate auto probe values
    generate_optional_value()
    # generate ref values again to ensure all values can be found
    waterfall_config(force=True)
    # check
    check_config()
    #import pdb;pdb.set_trace()
    # Export config to file, let bash script to import as local variables.
    print export_config(workload_name)


def check_config():             # check configures
    # Ensure mandatory configures are available
    for _, prop_name in HiBenchEnvPropMappingMandatory.items():
        assert HibenchConf.get(
            prop_name, None) is not None, "Mandatory configure missing: %s" % prop_name
    # Ensure all ref values in configure has been expanded
    for _, prop_name in HiBenchEnvPropMappingMandatory.items() + \
            HiBenchEnvPropMapping.items():
        assert "${" not in HibenchConf.get(prop_name, ""), "Unsolved ref key: %s. \n    Defined at %s:\n    Unsolved value:%s\n" % (
            prop_name, HibenchConfRef.get(prop_name, "unknown"), HibenchConf.get(prop_name, "unknown"))


def waterfall_config(force=False):         # replace "${xxx}" to its values
    no_value_sign = "___###NO_VALUE_SIGN###___"

    def process_replace(m):
        raw_key = m.groups()[0]
#        key, default_value = (raw_key[2:-1].strip().split(":-") + [None])[:2]
        key, spliter, default_value = (
            re.split("(:-|:_)", raw_key[2:-1].strip()) + [None, None])[:3]

        log_debug(
            "key:",
            key,
            " value:",
            HibenchConf.get(
                key,
                "RAWKEY:" +
                raw_key),
            "default value:" +
            repr(default_value))
        if force:
            if default_value is None:
                return HibenchConf.get(key)
#                return HibenchConf.get(key, raw_key)
            else:
                if spliter == ':_' and not default_value:  # no return
                    return no_value_sign
                return HibenchConf.get(key, default_value)
        else:
            return HibenchConf.get(key, "") or raw_key

    def wildcard_replacement(key, value):
        if "*" in key:          # we have wildcard replacement
            assert len(key.split("*")) == len(value.split("*")
                                              ), "ERROR! The number of wildcards defined in key and value must be identical: %s -> %s" % (key, value)
            key_searcher = re.compile("^" + "(.*)".join(key.split("*")) + "$")
            matched_keys_to_remove = []
            for k in HibenchConf.keys():
                matched_keys = key_searcher.match(k)
                if matched_keys:
                    matched_keys_to_remove.append(k)
                    if not "*" in k:
                        splited_value = value.split("*")
                        new_key = splited_value[
                            0] + "".join([matched_keys.groups()[idx] + x for idx, x in enumerate(splited_value[1:])])

                        HibenchConf[new_key] = HibenchConf[k]
                        HibenchConfRef[
                            new_key] = "Generated by wildcard rule: %s -> %s" % (key, value)
#                        log_debug(new_key, HibenchConf[k])
            for key in matched_keys_to_remove:
                del HibenchConf[key]
            return True
        return

    p = re.compile("(\$\{\s*[^\s^\$^\}]+\s*\})")

    wildcard_rules = []
    finish = False
    while True:
        while not finish:
            finish = True
            for key, value in HibenchConf.items():
                old_value = value
                old_key = key
                key = p.sub(process_replace, key)
                value = p.sub(process_replace, value)
                if key != old_key:
                    #log_debug("update key:", key, old_key)
                    HibenchConf[key] = HibenchConf[old_key]
                    del HibenchConf[old_key]
                    finish = False
                elif value != old_value:  # we have updated value, try again
                    #                    log_debug("Waterfall conf: %s: %s -> %s" % (key, old_value, value))
                    HibenchConf[key] = value
                    finish = False

        wildcard_rules = [(key, HibenchConf[key])
                          for key in HibenchConf if "*" in key]
        # now, let's check wildcard replacement rules
        for key, value in wildcard_rules:
            # check if we found a rule like: aaa.*.ccc.*.ddd    ->   bbb.*.*

            # wildcard replacement is useful for samza conf, which
            # seems can place anything under its conf namespaces.

            # The first wildcard in key will match the first wildcard
            # in value, etc. The number of wildcard in key and value
            # must be identical. However, it'll be impossible to
            # switch the order of two wildcards, something like the
            # first wildcard in key to match the second wildcard in
            # value. I just don't think it'll be needed.
            if not wildcard_replacement(
                    key, value):     # not wildcard rules? re-add
                HibenchConf[key] = value
        if wildcard_rules:      # need try again
            wildcard_rules = []
        else:
            break

    # all finished, remove values contains no_value_sign
    for key in [x for x in HibenchConf if no_value_sign in HibenchConf[x]]:
        del HibenchConf[key]
        del HibenchConfRef[key]


def probe_java_bin():
    # probe JAVA_HOME
    if not HibenchConf.get("java.bin", ""):
        # probe java bin
        if os.environ.get('JAVA_HOME', ''):
            # lookup in os environment
            HibenchConf['java.bin'] = os.path.join(
                os.environ.get('JAVA_HOME'), "bin", "java")
            HibenchConfRef[
                'java.bin'] = "probed from os environment of JAVA_HOME"
        else:
            # lookup in path
            path_dirs = os.environ.get('PATH', '').split(':')
            for path in path_dirs:
                if os.path.isfile(os.path.join(path, "java")):
                    HibenchConf['java.bin'] = os.path.join(path, "java")
                    HibenchConfRef[
                        'java.bin'] = "probed by lookup in $PATH: " + path
                    break
            else:
                # still not found?
                assert 0, "JAVA_HOME unset or can't found java executable in $PATH"


def probe_hadoop_release():
    # probe hadoop release. CDH(only support CDH 5 in HiBench 6.0), HDP, or
    # apache
    if not HibenchConf.get("hibench.hadoop.release", ""):
        cmd_release_and_version = HibenchConf[
            'hibench.hadoop.executable'] + ' version | head -1'
        # version here means, for example apache hadoop {2.7.3}
        hadoop_release_and_version = shell(cmd_release_and_version).strip()

        HibenchConf["hibench.hadoop.release"] = \
            "cdh4" if "cdh4" in hadoop_release_and_version else \
            "cdh5" if "cdh5" in hadoop_release_and_version else \
            "apache" if "Hadoop" in hadoop_release_and_version else \
            "UNKNOWN"
        HibenchConfRef["hibench.hadoop.release"] = "Inferred by: hadoop executable, the path is:\"%s\"" % HibenchConf[
            'hibench.hadoop.executable']

        assert HibenchConf["hibench.hadoop.release"] in ["cdh4", "cdh5", "apache",
                                                         "hdp"], "Unknown hadoop release. Auto probe failed, please override `hibench.hadoop.release` to explicitly define this property"
        assert HibenchConf[
            "hibench.hadoop.release"] != "cdh4", "Hadoop release CDH4 is not supported in HiBench6.0, please upgrade to CDH5 or use Apache Hadoop/HDP"


def probe_spark_version():
    # probe spark version
    if not HibenchConf.get("hibench.spark.version", ""):
        spark_home = HibenchConf.get("hibench.spark.home", "")
        assert spark_home, "`hibench.spark.home` undefined, please fix it and retry"
        try:
            release_file = join(spark_home, "RELEASE")
            with open(release_file) as f:
                spark_version_raw = f.readlines()[0]
                # spark_version_raw="Spark 1.2.2-SNAPSHOT (git revision
                # f9d8c5e) built for Hadoop 1.0.4\n"      # version sample
                spark_version = spark_version_raw.split()[1].strip()
                HibenchConfRef["hibench.spark.version"] = "Probed from file %s, parsed by value:%s" % (
                    release_file, spark_version_raw)
        except IOError as e:  # no release file, fall back to hard way
            log("Probing spark verison, may last long at first time...")
            shell_cmd = '( cd %s; mvn help:evaluate -Dexpression=project.version 2> /dev/null | grep -v "INFO" | tail -n 1)' % spark_home
            spark_version = shell(shell_cmd, timeout=600).strip()
            HibenchConfRef["hibench.spark.version"] = "Probed by shell command: %s, value: %s" % (
                shell_cmd, spark_version)

        assert spark_version, "Spark version probe failed, please override `hibench.spark.version` to explicitly define this property"
        HibenchConf["hibench.spark.version"] = "spark" + spark_version[:3]


def probe_hadoop_examples_jars():
    # probe hadoop example jars
    if not HibenchConf.get("hibench.hadoop.examples.jar", ""):
        examples_jars_candidate_apache0 = HibenchConf[
            'hibench.hadoop.home'] + "/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar"
        examples_jars_candidate_cdh0 = HibenchConf[
            'hibench.hadoop.home'] + "/share/hadoop/mapreduce2/hadoop-mapreduce-examples-*.jar"
        examples_jars_candidate_cdh1 = HibenchConf[
            'hibench.hadoop.home'] + "/../../jars/hadoop-mapreduce-examples-*.jar"
        examples_jars_candidate_hdp0 = HibenchConf[
            'hibench.hadoop.home'] + "/hadoop-mapreduce-examples.jar"
        examples_jars_candidate_list = [
            examples_jars_candidate_apache0,
            examples_jars_candidate_cdh0,
            examples_jars_candidate_cdh1,
            examples_jars_candidate_hdp0]

        HibenchConf["hibench.hadoop.examples.jar"] = exactly_one_file(
            examples_jars_candidate_list)
        HibenchConfRef["hibench.hadoop.examples.jar"] = "Inferred by " + \
            HibenchConf["hibench.hadoop.examples.jar"]


def probe_hadoop_examples_test_jars():
    # probe hadoop examples test jars
    if not HibenchConf.get("hibench.hadoop.examples.test.jar", ""):
        examples_test_jars_candidate_apache0 = HibenchConf[
            'hibench.hadoop.home'] + "/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient*-tests.jar"
        examples_test_jars_candidate_cdh0 = HibenchConf[
            'hibench.hadoop.home'] + "/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient*-tests.jar"
        examples_test_jars_candidate_cdh1 = HibenchConf[
            'hibench.hadoop.home'] + "/../../jars/hadoop-mapreduce-client-jobclient*-tests.jar"
        examples_test_jars_candidate_hdp0 = HibenchConf[
            'hibench.hadoop.home'] + "/hadoop-mapreduce-client-jobclient-tests.jar"
        examples_test_jars_candidate_list = [
            examples_test_jars_candidate_apache0,
            examples_test_jars_candidate_cdh0,
            examples_test_jars_candidate_cdh1,
            examples_test_jars_candidate_hdp0]

        HibenchConf["hibench.hadoop.examples.test.jar"] = exactly_one_file(
            examples_test_jars_candidate_list)
        HibenchConfRef["hibench.hadoop.examples.test.jar"] = "Inferred by " + \
            HibenchConf["hibench.hadoop.examples.test.jar"]


def probe_sleep_job_jar():
    # set hibench.sleep.job.jar
    if not HibenchConf.get('hibench.sleep.job.jar', ''):
        log("probe sleep jar:", HibenchConf[
            'hibench.hadoop.examples.test.jar'])
        HibenchConf["hibench.sleep.job.jar"] = HibenchConf[
            'hibench.hadoop.examples.test.jar']
        HibenchConfRef[
            "hibench.sleep.job.jar"] = "Refer to `hibench.hadoop.examples.test.jar` according to the evidence of `hibench.hadoop.release`"


def probe_hadoop_configure_dir():
    # probe hadoop configuration files
    if not HibenchConf.get("hibench.hadoop.configure.dir", ""):
        # For Apache, HDP, and CDH release
        HibenchConf["hibench.hadoop.configure.dir"] = join(
            HibenchConf["hibench.hadoop.home"], "etc", "hadoop")
        HibenchConfRef[
            "hibench.hadoop.configure.dir"] = "Inferred by: `hibench.hadoop.home`"


def probe_mapper_reducer_names():
    # set hadoop mapper/reducer property names
    if not HibenchConf.get("hibench.hadoop.mapper.name", ""):
        HibenchConf["hibench.hadoop.mapper.name"] = "mapreduce.job.maps"
        HibenchConfRef[
            "hibench.hadoop.mapper.name"] = "Use default mapper name"
    if not HibenchConf.get("hibench.hadoop.reducer.name", ""):
        HibenchConf["hibench.hadoop.reducer.name"] = "mapreduce.job.reduces"
        HibenchConfRef[
            "hibench.hadoop.reducer.name"] = "Use default reducer name"


def probe_spark_port(port_name, default_port):
    spark_home = HibenchConf.get("hibench.spark.home", "")
    assert spark_home, "`hibench.spark.home` undefined, please fix it and retry"
    join = os.path.join
    spark_env_file = join(spark_home, "conf/spark-env.sh")
    master_port = default_port

    if(len(glob.glob(spark_env_file)) == 1):
        with open(spark_env_file) as f:
            file_content = f.readlines()
        for line in file_content:
            if not line.strip().startswith(
                    "#") and port_name in line:
                if "\"" in line:
                    master_port = line.split("=")[1].split("\"")[1]
                elif "\'" in line:
                    master_port = line.split("=")[1].split("\'")[1]
                else:
                    master_port = line.split("=")[1]
        master_port = master_port.strip()
    return master_port


def probe_spark_master_webui_port():
    return probe_spark_port("SPARK_MASTER_WEBUI_PORT", "8080")


def probe_spark_worker_webui_port():
    return probe_spark_port("SPARK_WORKER_WEBUI_PORT", "8081")


def probe_masters_slaves_hostnames():
    # probe masters, slaves hostnames
    # determine running mode according to spark master configuration
    if not (
        HibenchConf.get(
            "hibench.masters.hostnames",
            "") or HibenchConf.get(
            "hibench.slaves.hostnames",
            "")):  # no pre-defined hostnames, let's probe
        spark_master = HibenchConf['hibench.spark.master']
        # local mode
        if spark_master.startswith("local"):
            HibenchConf['hibench.masters.hostnames'] = ''  # no master
            # localhost as slaves
            HibenchConf['hibench.slaves.hostnames'] = 'localhost'
            HibenchConfRef['hibench.masters.hostnames'] = HibenchConfRef[
                'hibench.slaves.hostnames'] = "Probed by the evidence of 'hibench.spark.master=%s'" % spark_master
        # spark standalone mode
        elif spark_master.startswith("spark"):
            HibenchConf['hibench.masters.hostnames'] = spark_master[8:].split(":")[
                0]
            HibenchConfRef[
                'hibench.masters.hostnames'] = "Probed by the evidence of 'hibench.spark.master=%s'" % spark_master

            log(spark_master, HibenchConf['hibench.masters.hostnames'])
            master_port = probe_spark_master_webui_port()
            worker_port = probe_spark_worker_webui_port()
            with closing(urllib.urlopen('http://%s:%s' % (HibenchConf['hibench.masters.hostnames'], master_port))) as page:
                worker_hostnames = [
                    re.findall(
                        "http:\/\/([a-zA-Z\-\._0-9]+):%s" %
                        worker_port,
                        x)[0] for x in page.readlines() if "%s" %
                    worker_port in x and "worker" in x]
                HibenchConf['hibench.slaves.hostnames'] = " ".join(
                    worker_hostnames)
                HibenchConfRef['hibench.slaves.hostnames'] = "Probed by parsing " + \
                    'http://%s:%s' % (HibenchConf['hibench.masters.hostnames'], master_port)
                assert HibenchConf['hibench.slaves.hostnames'] != "" and HibenchConf[
                    'hibench.masters.hostnames'] != "", "Get workers from spark master's web UI page failed, \nPlease check your configurations, network settings, proxy settings, or set `hibench.masters.hostnames` and `hibench.slaves.hostnames` manually"
        # yarn mode
        elif spark_master.startswith("yarn"):
            yarn_executable = os.path.join(os.path.dirname(
                HibenchConf['hibench.hadoop.executable']), "yarn")
            cmd = "( " + yarn_executable + \
                " node -list 2> /dev/null | grep RUNNING )"
            try:
                worker_hostnames = [
                    line.split(":")[0] for line in shell(cmd).split("\n")]
                HibenchConf['hibench.slaves.hostnames'] = " ".join(
                    worker_hostnames)
                HibenchConfRef[
                    'hibench.slaves.hostnames'] = "Probed by parsing results from: " + cmd

                # parse yarn resource manager from hadoop conf
                yarn_site_file = os.path.join(
                    HibenchConf["hibench.hadoop.configure.dir"], "yarn-site.xml")
                with open(yarn_site_file) as f:
                    file_content = f.read()
                    match_address = re.findall(
                        "\<property\>\s*\<name\>\s*yarn.resourcemanager.address[.\w\s]*\<\/name\>\s*\<value\>([a-zA-Z\-\._0-9]+)(:\d+)?\<\/value\>",
                        file_content)
                    match_hostname = re.findall(
                        "\<property\>\s*\<name\>\s*yarn.resourcemanager.hostname[.\w\s]*\<\/name\>\s*\<value\>([a-zA-Z\-\._0-9]+)(:\d+)?\<\/value\>",
                        file_content)
                if match_address:
                    resourcemanager_hostname = match_address[0][0]
                    HibenchConf[
                        'hibench.masters.hostnames'] = resourcemanager_hostname
                    HibenchConfRef[
                        'hibench.masters.hostnames'] = "Parsed from " + yarn_site_file
                elif match_hostname:
                    resourcemanager_hostname = match_hostname[0][0]
                    HibenchConf[
                        'hibench.masters.hostnames'] = resourcemanager_hostname
                    HibenchConfRef[
                        'hibench.masters.hostnames'] = "Parsed from " + yarn_site_file
                else:
                    assert 0, "Unknown resourcemanager, please check `hibench.hadoop.configure.dir` and \"yarn-site.xml\" file"
            except Exception as e:
                assert 0, "Get workers from yarn-site.xml page failed, reason:%s\nplease set `hibench.masters.hostnames` and `hibench.slaves.hostnames` manually" % e

    # reset hostnames according to gethostbyaddr
    names = set(
        HibenchConf['hibench.masters.hostnames'].split() +
        HibenchConf['hibench.slaves.hostnames'].split())
    new_name_mapping = {}
    for name in names:
        try:
            new_name_mapping[name] = socket.gethostbyaddr(name)[0]
        # host name lookup failure?
        except:
            new_name_mapping[name] = name
    HibenchConf['hibench.masters.hostnames'] = repr(" ".join(
        [new_name_mapping[x] for x in HibenchConf['hibench.masters.hostnames'].split()]))
    HibenchConf['hibench.slaves.hostnames'] = repr(" ".join(
        [new_name_mapping[x] for x in HibenchConf['hibench.slaves.hostnames'].split()]))


def probe_java_opts():
    file_name = os.path.join(
        HibenchConf['hibench.hadoop.configure.dir'],
        'mapred-site.xml')
    cnt = 0
    map_java_opts_line = ""
    reduce_java_opts_line = ""
    try:
        with open(file_name) as f:
            content = f.read()
    except IOError:
        return
    # Do the split for itself so as to deal with any weird xml style
    content = content.split("<value>")
    for line in content:
        if "mapreduce.map.java.opts" in line and cnt + 1 < len(content):
            map_java_opts_line = content[cnt + 1]
        if "mapreduce.reduce.java.opts" in line and cnt + 1 < len(content):
            reduce_java_opts_line = content[cnt + 1]
        cnt += 1
    if map_java_opts_line != "":
        HibenchConf['hibench.dfsioe.map.java_opts'] = map_java_opts_line.split("<")[
            0].strip()
        HibenchConfRef['hibench.dfsioe.map.java_opts'] = "Probed by configuration file:'%s'" % os.path.join(
            HibenchConf['hibench.hadoop.configure.dir'], 'mapred-site.xml')
    if reduce_java_opts_line != "":
        HibenchConf['hibench.dfsioe.red.java_opts'] = reduce_java_opts_line.split("<")[
            0].strip()
        HibenchConfRef['hibench.dfsioe.red.java_opts'] = "Probed by configuration file:'%s'" % os.path.join(
            HibenchConf['hibench.hadoop.configure.dir'], 'mapred-site.xml')


def generate_optional_value():
    # get some critical values from environment or make a guess
    d = os.path.dirname
    join = os.path.join
    HibenchConf['hibench.home'] = d(d(d(os.path.abspath(__file__))))
    del d
    HibenchConfRef[
        'hibench.home'] = "Inferred from relative path of dirname(%s)/../../" % __file__

    probe_java_bin()
    probe_hadoop_release()
    probe_spark_version()
    probe_hadoop_examples_jars()
    probe_hadoop_examples_test_jars()
    probe_sleep_job_jar()
    probe_hadoop_configure_dir()
    probe_mapper_reducer_names()
    probe_masters_slaves_hostnames()
    probe_java_opts()


def test_succeed():
    # Can only be used to test if auto probe executes correctly, will effect
    # the latter export_config function and get a no such file or directory
    # error
    print("1 " + HibenchConf['java.bin'] + '\n')
    print("2 " + HibenchConf["hibench.hadoop.release"] + "\n")
    print("3 " + HibenchConf["hibench.spark.version"] + "\n")
    print("4 " + HibenchConf["hibench.hadoop.examples.jar"] + "\n")
    print("5 " + HibenchConf["hibench.hadoop.examples.test.jar"] + "\n")
    print("6 " + HibenchConf["hibench.sleep.job.jar"] + "\n")
    print("7 " + HibenchConf["hibench.hadoop.configure.dir"] + "\n")
    print("8 " + HibenchConf["hibench.hadoop.mapper.name"] + "\n")
    print("9 " + HibenchConf["hibench.hadoop.reducer.name"] + "\n")
    print("10 " + HibenchConf['hibench.masters.hostnames'] + "\n")
    print("11 " + HibenchConf['hibench.slaves.hostnames'] + "\n")
    print("12 " + HibenchConf['hibench.dfsioe.map.java_opts'] + "\n")
    print("13 " + HibenchConf['hibench.dfsioe.red.java_opts'] + "\n")


def export_config(workload_name):
    join = os.path.join
    report_dir = HibenchConf['hibench.report.dir']
    conf_dir = join(report_dir, workload_name, 'conf')
    conf_filename = join(conf_dir, "%s.conf" % workload_name)

    spark_conf_dir = join(conf_dir, "sparkbench")
    spark_prop_conf_filename = join(spark_conf_dir, "spark.conf")
    samza_prop_conf_filename = join(spark_conf_dir, "samza.conf")
    sparkbench_prop_conf_filename = join(spark_conf_dir, "sparkbench.conf")

    if not os.path.exists(spark_conf_dir):
        os.makedirs(spark_conf_dir)
    if not os.path.exists(conf_dir):
        os.makedirs(conf_dir)

    # generate configure for hibench
    sources = defaultdict(list)
    for env_name, prop_name in HiBenchEnvPropMappingMandatory.items() + \
            HiBenchEnvPropMapping.items():
        source = HibenchConfRef.get(prop_name, 'None')
        sources[source].append('%s=%s' %
                               (env_name, HibenchConf.get(prop_name, '')))

    with open(conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            f.write("# Source: %s\n" % source)
            f.write("\n".join(sorted(sources[source])))
            f.write("\n\n")
        f.write("#Source: add for internal usage\n")
        f.write(
            "SPARKBENCH_PROPERTIES_FILES=%s\n" %
            sparkbench_prop_conf_filename)
        f.write("SPARK_PROP_CONF=%s\n" % spark_prop_conf_filename)
        f.write("SAMZA_PROP_CONF=%s\n" % samza_prop_conf_filename)
        f.write("WORKLOAD_RESULT_FOLDER=%s\n" % join(conf_dir, ".."))
        f.write("HIBENCH_WORKLOAD_CONF=%s\n" % conf_filename)
        f.write("export HADOOP_EXECUTABLE\n")
        f.write("export HADOOP_CONF_DIR\n")

    # generate properties for spark & sparkbench
    sources = defaultdict(list)
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
    # generate configure for samza
    with open(samza_prop_conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            items = [x for x in sources[source] if x.startswith("samza.")]
            if items:
                f.write("# Source: %s\n" % source)
                f.write("\n".join(sorted(items)))
                f.write("\n\n")
    # generate configure for spark
    with open(sparkbench_prop_conf_filename, 'w') as f:
        for source in sorted(sources.keys()):
            items = [x for x in sources[source] if x.startswith(
                "sparkbench.") or x.startswith("hibench.")]
            if items:
                f.write("# Source: %s\n" % source)
                f.write("\n".join(sorted(items)))
                f.write("\n\n")

    return conf_filename

if __name__ == "__main__":
    if len(sys.argv) < 4:
        raise Exception(
            "Please supply <conf root path>, <workload root path>, <workload folder path> [<patch config lists, seperated by comma>")
    conf_root, workload_config, workload_name = sys.argv[
        1], sys.argv[2], sys.argv[3]
    if len(sys.argv) > 4:
        patching_config = sys.argv[4]
    else:
        patching_config = ''
    load_config(conf_root, workload_config, workload_name, patching_config)
