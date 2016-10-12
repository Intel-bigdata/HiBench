import unittest
import os
from load_config import *


def mkdir(dir_path):
    shell("mkdir -p " + dir_path)

def touch(file_path):
    shell("touch " + file_path)

def remove(dir_path):
    shell("rm -rf " + dir_path)

def print_hint_seperator(hint):
    print("\n" + hint)
    print("--------------------------------")

def runTest(test_case):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    unittest.TextTestRunner(verbosity=2).run(suite)

def parse_conf():
    conf_root = os.path.abspath(".") + "/../../conf"
    conf_files = sorted(glob.glob(conf_root + "/*.conf"))
    # load values from conf files
    for filename in conf_files:
        with open(filename) as f:
            for line in f.readlines():
                line = line.strip()
                if not line:
                    continue  # skip empty lines
                if line[0] == '#':
                    continue  # skip comments
                try:
                    key, value = re.split("\s", line, 1)
                except ValueError:
                    key = line.strip()
                    value = ""
                HibenchConf[key] = value.strip()
                HibenchConfRef[key] = filename

def parse_conf_before_probe():
    parse_conf()
    waterfall_config()

def test_probe_hadoop_examples_jars():

    def test_probe_hadoop_examples_jars_generator(jar_path):
        def test(self):
            dir_path = os.path.dirname(jar_path)
            mkdir(dir_path)
            touch(jar_path)
            probe_hadoop_examples_jars()
            answer = HibenchConf["hibench.hadoop.examples.jar"]
            self.assertEqual(os.path.abspath(jar_path), os.path.abspath(jar_path))

        return test

    hadoop_examples_jars_list = [["apache0", "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.0.jar"], ["cdh0", "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.0.jar"],
                                 ["cdh1", "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-examples-*.jar"], ["hdp0", "/tmp/test/hadoop_home/hadoop-mapreduce-examples.jar"]]

    for case in hadoop_examples_jars_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_hadoop_examples_jars_generator(case[1])
        setattr(ProbeHadoopExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop examples jars:")
    runTest(ProbeHadoopExamplesTestCase)

def test_probe_hadoop_test_examples_jars():
    def test_probe_hadoop_examples_jars_generator(jar_path):
        def test(self):
            dir_path = os.path.dirname(jar_path)
            mkdir(dir_path)
            touch(jar_path)
            probe_hadoop_examples_jars()
            answer = HibenchConf["hibench.hadoop.examples.test.jar"]
            self.assertEqual(os.path.abspath(jar_path), os.path.abspath(jar_path))

        return test

    hadoop_examples_jars_list = [["apache0", "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"], ["cdh0", "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"],
                                 ["cdh1", "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"], ["hdp0", "/tmp/test/hadoop_home/hadoop-mapreduce-client-jobclient-tests.jar"]]

    for case in hadoop_examples_jars_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_hadoop_examples_jars_generator(case[1])
        setattr(ProbeHadoopTestExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop test examples jars:")
    runTest(ProbeHadoopExamplesTestCase)

def test_probe_java_bin():
    print_hint_seperator("Test probe java bin:")
    runTest(ProbeJavaBinTestCase)

def test_probe_hadoop_release():
    print_hint_seperator("Test probe hadoop release:")
    runTest(ProbeHadoopReleaseTestCase)

def test_probe_spark_version():
    print_hint_seperator("Test probe spark version")
    runTest(ProbeSparkVersionTestCase)


# def test_probe_sleep_jar():
#     print_hint_seperator("Test probe sleep jar")
#     runTest(ProbeSleepJarTestCase)

def test_probe_hadoop_conf_dir():
    print_hint_seperator("Test probe hadoop conf dir")
    runTest(ProbeHadoopConfDirTestCase)

def test_probe_spark_conf_value():
    def test_probe_spark_conf_value_generator(conf_name, line, default):
        def test(self):
            HibenchConf["hibench.spark.home"] = "/tmp/test/spark_home"
            spark_env_dir = HibenchConf["hibench.spark.home"] + "/conf"
            spark_env_path = HibenchConf["hibench.spark.home"] + "/conf/spark-env.sh"
            mkdir(spark_env_dir)
            shell("echo " + line + " >> " + spark_env_path)
            value = probe_spark_conf_value(conf_name, default)
            expected = default
            if len(line.split("=")) >= 2:
                expected = line.split("=")[1]
            expected = expected.strip("\'")
            expected = expected.strip("\"")
            self.assertEqual(str(value), expected)
        return test

    spark_conf_test_case_list = [["spark_master_webui_port_simple", "SPARK_MASTER_WEBUI_PORT", "export SPARK_MASTER_WEBUI_PORT=8880", None], ["spark_master_webui_port_single_quotes", "SPARK_MASTER_WEBUI_PORT", "export SPARK_MASTER_WEBUI_PORT=\'8880\'", None], ["spark_master_webui_port_double_quotes", "SPARK_MASTER_WEBUI_PORT", "export SPARK_MASTER_WEBUI_PORT=\"8880\"", None],
                                 ]

    for case in spark_conf_test_case_list:
        test_name = 'test_%s' % case[0]
        test = test_probe_spark_conf_value_generator(case[1], case[2], case[3])
        setattr(ProbeSparkConfValueTestCase, test_name, test)
    print_hint_seperator("Test probe spark conf value")
    runTest(ProbeSparkConfValueTestCase)

def test_probe_java_opts():
    print_hint_seperator("Test probe java opts")
    runTest(ProbeJavaOptsTestCase)

class ProbeHadoopExamplesTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        HibenchConf["hibench.hadoop.examples.jar"] = ""
        remove(HibenchConf["hibench.hadoop.home"])

class ProbeHadoopTestExamplesTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        HibenchConf["hibench.hadoop.examples.test.jar"] = ""
        remove(HibenchConf["hibench.hadoop.home"])

class ProbeJavaBinTestCase(unittest.TestCase):
    def setUp(self):
        self.java_home = "/tmp/test/java_home"
        self.java_bin = self.java_home + "/bin/java"
        pass
    def tearDown(self):
        remove(self.java_home)
        pass
    def test_probe_java_bin(self):
        os.environ["JAVA_HOME"] = self.java_home # visible in this process + all children
        touch(self.java_bin)
        probe_java_bin()
        answer = HibenchConf["java.bin"]
        expected = self.java_bin
        self.assertEqual(answer, expected)

class ProbeHadoopReleaseTestCase(unittest.TestCase):
    def expected_hadoop_release(self):
        if not HibenchConf.get("hibench.hadoop.release", ""):
            cmd_release_and_version = HibenchConf[
                                          'hibench.hadoop.executable'] + ' version | head -1'
            # An example for version here: apache hadoop 2.7.3
            hadoop_release_and_version = shell(cmd_release_and_version).strip()
            expected_hadoop_release = \
                "cdh4" if "cdh4" in hadoop_release_and_version else \
                    "cdh5" if "cdh5" in hadoop_release_and_version else \
                        "apache" if "Hadoop" in hadoop_release_and_version else \
                            "UNKNOWN"
        else:
            expected_hadoop_release = HibenchConf["hibench.hadoop.release"]
        return expected_hadoop_release

    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_probe_hadoop_release(self):
        parse_conf_before_probe()
        probe_hadoop_release()
        answer = HibenchConf["hibench.hadoop.release"]
        expected = self.expected_hadoop_release()
        self.assertEqual(answer, expected)

class ProbeSparkVersionTestCase(unittest.TestCase):
    def expected_spark_version(self):
        if not HibenchConf.get("hibench.spark.version", ""):
            spark_home = HibenchConf.get("hibench.spark.home", "")
            assert spark_home, "`hibench.spark.home` undefined, please fix it and retry"
            try:
                release_file = os.path.join(spark_home, "RELEASE")
                with open(release_file) as f:
                    spark_version_raw = f.readlines()[0]
                    # spark_version_raw="Spark 1.2.2-SNAPSHOT (git revision
                    # f9d8c5e) built for Hadoop 1.0.4\n"      # version sample
                    spark_version = spark_version_raw.split()[1].strip()
                    HibenchConfRef[
                        "hibench.spark.version"] = "Probed from file %s, parsed by value:%s" % (
                        release_file, spark_version_raw)
            except IOError as e:  # no release file, fall back to hard way
                log("Probing spark verison, may last long at first time...")
                shell_cmd = '( cd %s; mvn help:evaluate -Dexpression=project.version 2> /dev/null | grep -v "INFO" | tail -n 1)' % spark_home
                spark_version = shell(shell_cmd, timeout=600).strip()
                HibenchConfRef[
                    "hibench.spark.version"] = "Probed by shell command: %s, value: %s" % (
                    shell_cmd, spark_version)
            spark_version = "spark" + spark_version[:3]
        else:
            spark_version = HibenchConf["hibench.spark.version"]
        return spark_version
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_probe_spark_version(self):
        parse_conf_before_probe()
        probe_spark_version()
        answer = HibenchConf["hibench.spark.version"]
        expected = self.expected_spark_version()
        self.assertEqual(answer, expected)

# class ProbeSleepJarTestCase(unittest.TestCase):
#     def expected_sleep_jar(self):
#         if not HibenchConf.get('hibench.sleep.job.jar', ''):
#             sleep_jar = HibenchConf['hibench.hadoop.examples.test.jar']
#         else:
#             sleep_jar = HibenchConf['hibench.sleep.job.jar']
#         return sleep_jar
#     def setUp(self):
#         pass
#     def tearDown(self):
#         pass
#     def test_probe_spark_version(self):
#         parse_conf_before_probe()
#         probe_sleep_job_jar()
#         answer = HibenchConf["hibench.sleep.job.jar"]
#         expected = self.expected_spark_version()
#         self.assertEqual(answer, expected)

class ProbeHadoopConfDirTestCase(unittest.TestCase):
    def expected_hadoop_conf_dir(self):
        if not HibenchConf.get("hibench.hadoop.configure.dir", ""):
            hadoop_conf_dir = os.path.join(
                HibenchConf["hibench.hadoop.home"], "etc", "hadoop")
        else:
            hadoop_conf_dir = HibenchConf["hibench.hadoop.configure.dir"]
        return hadoop_conf_dir
    def setUp(self):
        pass
    def tearDown(self):
        pass
    def test_probe_hadoop_conf_dir(self):
        parse_conf_before_probe()
        probe_hadoop_configure_dir()
        answer = HibenchConf["hibench.hadoop.configure.dir"]
        expected = self.expected_hadoop_conf_dir()
        self.assertEqual(answer, expected)

class ProbeSparkConfValueTestCase(unittest.TestCase):
    def setUp(self):
        pass
    def tearDown(self):
        remove(HibenchConf["hibench.spark.home"])

    def test_probe_spark_conf_value_default_value(self):
        parse_conf_before_probe()
        conf_name = "Whatever~"
        default_value = "RIGHT_ANSWER"
        answer = probe_spark_conf_value(conf_name, default_value)
        expected = default_value
        self.assertEqual(answer, expected)

class ProbeJavaOptsTestCase(unittest.TestCase):
    def setUp(self):
        HibenchConf["hibench.hadoop.home"] = "/tmp/test/hadoop_home"
        HibenchConf["hibench.hadoop.configure.dir"] = "/tmp/test/hadoop_home/etc/hadoop"
    def tearDown(self):
        remove(HibenchConf["hibench.spark.home"])
    def test_probe_java_opts(self):
        mkdir(HibenchConf["hibench.hadoop.configure.dir"])
        mapred_site_path = HibenchConf["hibench.hadoop.configure.dir"] + "/mapred-site.xml"

        mapred_site_content = "<property><name>mapreduce.map.java.opts</name><value>-Xmx1536M -DpreferIPv4Stack=true</value></property><property><name>mapreduce.reduce.java.opts</name><value>-Xmx1536M -DpreferIPv4Stack=true</value></property>"
        expected_map_java_opts = "-Xmx1536M -DpreferIPv4Stack=true"
        expected_reduce_java_opts = "-Xmx1536M -DpreferIPv4Stack=true"
        echo = "echo \"" + mapred_site_content + "\" >> " + mapred_site_path
        shell(echo)
        probe_java_opts()
        answer_map_java_opts = HibenchConf['hibench.dfsioe.map.java_opts']
        answer_red_java_opts = HibenchConf['hibench.dfsioe.red.java_opts']
        if answer_map_java_opts.startswith("\'"):
            expected_map_java_opts = "\'" + expected_map_java_opts + "\'"
        elif answer_map_java_opts.startswith("\""):
            expected_map_java_opts = "\"" + expected_map_java_opts + "\""
        self.assertEqual(answer_map_java_opts, expected_map_java_opts)
        if answer_red_java_opts.startswith("\'"):
            expected_reduce_java_opts = "\'" + expected_reduce_java_opts + "\'"
        elif answer_red_java_opts.startswith("\""):
            expected_reduce_java_opts = "\"" + expected_reduce_java_opts + "\""
        self.assertEqual(answer_red_java_opts, expected_reduce_java_opts)

if __name__ == '__main__':
    test_probe_hadoop_examples_jars()
    test_probe_hadoop_test_examples_jars()
    test_probe_java_bin()
    test_probe_hadoop_release()
    test_probe_spark_version()
    # test_probe_sleep_jar()
    test_probe_hadoop_conf_dir()
    test_probe_spark_conf_value()
    test_probe_java_opts()

