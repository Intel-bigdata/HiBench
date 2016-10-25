import unittest
import os
import load_config
import mock
import fnmatch
import re
import glob


def print_hint_seperator(hint):
    print("\n" + hint)
    print("--------------------------------")


def run_test(test_case):
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
                load_config.HibenchConf[key] = value.strip()
                load_config.HibenchConfRef[key] = filename


def parse_conf_before_probe():
    parse_conf()
    load_config.waterfall_config()


def get_expected(name):
    expected_probe_conf_path = "/test_load_config_answer.conf"
    test_load_config_answer = os.path.abspath(".") + expected_probe_conf_path
    content = load_config.read_file_content(test_load_config_answer)
    for line in content:
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
        if key == name:
            return value.strip()
    return ""


def test_probe_hadoop_examples_jars():

    def test_probe_hadoop_examples_jars_generator(case_num):
        def test(self):

            def exactly_one_file_one_candidate(filename_pattern):
                regex = fnmatch.translate(filename_pattern)
                reobj = re.compile(regex)
                if reobj.match(hadoop_examples_jars_list[case_num][1]):
                    return hadoop_examples_jars_list[case_num][1]
                else:
                    return ""

            mock_exactly_one_file_one_candidate = mock.Mock(
                side_effect=exactly_one_file_one_candidate)
            with mock.patch("load_config.exactly_one_file_one_candidate",
                            mock_exactly_one_file_one_candidate):
                try:
                    from load_config import probe_hadoop_examples_jars
                    probe_hadoop_examples_jars()
                except:
                    pass
                answer = load_config.HibenchConf["hibench.hadoop.examples.jar"]
                self.assertEqual(
                    os.path.abspath(answer), os.path.abspath(
                        hadoop_examples_jars_list[case_num][1]))

        return test

    hadoop_examples_jars_list = [["apache0",
                                  "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.3.jar"],
                                 ["cdh0",
                                  "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-examples-2.7.3.jar"],
                                 ["cdh1",
                                  "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-examples-2.7.3.jar"],
                                 ["hdp0",
                                  "/tmp/test/hadoop_home/hadoop-mapreduce-examples.jar"]]

    for i in range(len(hadoop_examples_jars_list)):
        test_name = 'test_%s' % hadoop_examples_jars_list[i][0]
        test = test_probe_hadoop_examples_jars_generator(i)
        setattr(ProbeHadoopExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop examples jars:")
    run_test(ProbeHadoopExamplesTestCase)


def test_probe_hadoop_test_examples_jars():
    def test_probe_hadoop_examples_jars_generator(case_num):
        def test(self):

            def exactly_one_file_one_candidate(filename_pattern):
                regex = fnmatch.translate(filename_pattern)
                reobj = re.compile(regex)
                if reobj.match(hadoop_test_examples_jars_list[case_num][1]):
                    return hadoop_test_examples_jars_list[case_num][1]
                else:
                    return ""

            mock_exactly_one_file_one_candidate = mock.Mock(
                side_effect=exactly_one_file_one_candidate)
            with mock.patch("load_config.exactly_one_file_one_candidate",
                            mock_exactly_one_file_one_candidate):
                try:
                    from load_config import probe_hadoop_examples_test_jars
                    probe_hadoop_examples_test_jars()
                except:
                    pass
                answer = load_config.HibenchConf[
                    "hibench.hadoop.examples.test.jar"]
                self.assertEqual(
                    os.path.abspath(answer), os.path.abspath(
                        hadoop_test_examples_jars_list[case_num][1]))

        return test

    hadoop_test_examples_jars_list = [["apache0",
                                       "/tmp/test/hadoop_home/share/hadoop/mapreduce/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"],
                                      ["cdh0",
                                       "/tmp/test/hadoop_home/share/hadoop/mapreduce2/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"],
                                      ["cdh1",
                                       "/tmp/test/hadoop_home/../../jars/hadoop-mapreduce-client-jobclient-2.7.3-tests.jar"],
                                      ["hdp0",
                                       "/tmp/test/hadoop_home/hadoop-mapreduce-client-jobclient-tests.jar"]]

    for i in range(len(hadoop_test_examples_jars_list)):
        test_name = 'test_%s' % hadoop_test_examples_jars_list[i][0]
        test = test_probe_hadoop_examples_jars_generator(i)
        setattr(ProbeHadoopTestExamplesTestCase, test_name, test)

    print_hint_seperator("Test probe hadoop test examples jars:")
    run_test(ProbeHadoopTestExamplesTestCase)


def test_probe_java_bin():
    print_hint_seperator("Test probe java bin:")
    run_test(ProbeJavaBinTestCase)


def test_probe_hadoop_release():
    print_hint_seperator("Test probe hadoop release:")
    run_test(ProbeHadoopReleaseTestCase)


def test_probe_spark_version():
    print_hint_seperator("Test probe spark version")
    run_test(ProbeSparkVersionTestCase)


def test_probe_hadoop_conf_dir():
    print_hint_seperator("Test probe hadoop conf dir")
    run_test(ProbeHadoopConfDirTestCase)


def test_probe_spark_conf_value():
    def test_probe_spark_conf_value_generator(case_num):
        def test(self):
            load_config.HibenchConf[
                "hibench.spark.home"] = "/tmp/test/spark_home"

            conf_name = spark_conf_test_case_list[case_num][1]
            line = spark_conf_test_case_list[case_num][2]
            default = spark_conf_test_case_list[case_num][3]

            def read_file_content(filepath):
                if filepath == "/tmp/test/spark_home/conf/spark-env.sh":
                    return [line]
                else:
                    return []

            mock_read_file_content = mock.Mock(side_effect=read_file_content)
            with mock.patch("load_config.read_file_content",
                            mock_read_file_content):
                answer = ""

                try:
                    from load_config import probe_spark_conf_value
                    answer = probe_spark_conf_value(conf_name, default)
                except:
                    pass
                expected = default
                if len(line.split("=")) >= 2:
                    expected = line.split("=")[1]
                expected = expected.strip("\'")
                expected = expected.strip("\"")
                self.assertEqual(str(answer), expected)

        return test

    spark_conf_test_case_list = [["spark_master_webui_port_simple",
                                  "SPARK_MASTER_WEBUI_PORT",
                                  "export SPARK_MASTER_WEBUI_PORT=8880",
                                  "8080"],
                                 ["spark_master_webui_port_single_quotes",
                                  "SPARK_MASTER_WEBUI_PORT",
                                  "export SPARK_MASTER_WEBUI_PORT=\'8880\'",
                                  "8080"],
                                 ["spark_master_webui_port_double_quotes",
                                  "SPARK_MASTER_WEBUI_PORT",
                                  "export SPARK_MASTER_WEBUI_PORT=\"8880\"",
                                  "8080"],
                                 ]

    for i in range(len(spark_conf_test_case_list)):
        test_name = 'test_%s' % spark_conf_test_case_list[i][0]
        test = test_probe_spark_conf_value_generator(i)
        setattr(ProbeSparkConfValueTestCase, test_name, test)
    print_hint_seperator("Test probe spark conf value")
    run_test(ProbeSparkConfValueTestCase)


def test_probe_java_opts():
    print_hint_seperator("Test probe java opts")
    run_test(ProbeJavaOptsTestCase)


def test_probe_masters_slaves_hostnames():
    print_hint_seperator("Test probe masters slaves hostnames")
    run_test(ProbeMastersSlavesHostnamesTestCase)


class ProbeHadoopExamplesTestCase(unittest.TestCase):

    def setUp(self):
        load_config.HibenchConf[
            "hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        load_config.HibenchConf["hibench.hadoop.examples.jar"] = ""


class ProbeHadoopTestExamplesTestCase(unittest.TestCase):

    def setUp(self):
        load_config.HibenchConf[
            "hibench.hadoop.home"] = "/tmp/test/hadoop_home"

    def tearDown(self):
        load_config.HibenchConf["hibench.hadoop.examples.test.jar"] = ""


class ProbeJavaBinTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_java_bin(self):
        load_config.probe_java_bin()
        answer = load_config.HibenchConf["java.bin"]
        expected = get_expected("java.bin")
        self.assertEqual(answer, expected)


class ProbeHadoopReleaseTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_hadoop_release(self):
        parse_conf_before_probe()
        load_config.probe_hadoop_release()
        answer = load_config.HibenchConf["hibench.hadoop.release"]
        expected = get_expected("hibench.hadoop.release")
        self.assertEqual(answer, expected)


class ProbeSparkVersionTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_spark_version(self):
        parse_conf_before_probe()
        load_config.probe_spark_version()
        answer = load_config.HibenchConf["hibench.spark.version"]
        expected = get_expected("hibench.spark.version")
        self.assertEqual(answer, expected)


class ProbeHadoopConfDirTestCase(unittest.TestCase):

    def expected_hadoop_conf_dir(self):
        if not load_config.HibenchConf.get("hibench.hadoop.configure.dir", ""):
            hadoop_conf_dir = os.path.join(
                load_config.HibenchConf["hibench.hadoop.home"], "etc", "hadoop")
        else:
            hadoop_conf_dir = load_config.HibenchConf[
                "hibench.hadoop.configure.dir"]
        return hadoop_conf_dir

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_hadoop_conf_dir(self):
        parse_conf_before_probe()
        load_config.probe_hadoop_configure_dir()
        answer = load_config.HibenchConf["hibench.hadoop.configure.dir"]
        expected = self.expected_hadoop_conf_dir()
        self.assertEqual(answer, expected)


class ProbeSparkConfValueTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_spark_conf_value_default_value(self):
        parse_conf_before_probe()
        conf_name = "Whatever~"
        default_value = "RIGHT_ANSWER"
        answer = load_config.probe_spark_conf_value(conf_name, default_value)
        expected = default_value
        self.assertEqual(answer, expected)


class ProbeJavaOptsTestCase(unittest.TestCase):

    def setUp(self):
        load_config.HibenchConf[
            "hibench.hadoop.home"] = "/tmp/test/hadoop_home"
        load_config.HibenchConf[
            "hibench.hadoop.configure.dir"] = "/tmp/test/hadoop_home/etc/hadoop"

    def tearDown(self):
        pass

    def test_probe_java_opts(self):

        mapred_site_path = load_config.HibenchConf[
            "hibench.hadoop.configure.dir"] + "/mapred-site.xml"

        mapred_site_content = "<property><name>mapreduce.map.java.opts</name><value>-Xmx1536M -DpreferIPv4Stack=true</value></property><property><name>mapreduce.reduce.java.opts</name><value>-Xmx1536M -DpreferIPv4Stack=true</value></property>"
        expected_map_java_opts = "-Xmx1536M -DpreferIPv4Stack=true"
        expected_reduce_java_opts = "-Xmx1536M -DpreferIPv4Stack=true"

        def read_file_content_java_opts(filepath):
            if filepath == "/tmp/test/hadoop_home/etc/hadoop/mapred-site.xml":
                return [mapred_site_content]
            else:
                return []

        mock_read_file_content_java_opts = mock.Mock(
            side_effect=read_file_content_java_opts)
        with mock.patch("load_config.read_file_content",
                        mock_read_file_content_java_opts):
            answer = ""

            try:
                from load_config import probe_java_opts
                probe_java_opts()
                answer = ""
            except:
                pass

            answer_map_java_opts = load_config.HibenchConf[
                'hibench.dfsioe.map.java_opts']
            answer_red_java_opts = load_config.HibenchConf[
                'hibench.dfsioe.red.java_opts']
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


class ProbeMastersSlavesHostnamesTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_probe_masters_slaves_hostnames(self):
        load_config.probe_masters_slaves_hostnames()
        answer_masters_hostnames = load_config.HibenchConf[
            'hibench.masters.hostnames']
        answer_slaves_hostnames = load_config.HibenchConf[
            'hibench.slaves.hostnames']
        expected_masters_hostnames = get_expected("hibench.masters.hostnames")
        expected_slaves_hostnames = get_expected("hibench.slaves.hostnames")
        self.assertEqual(
            answer_masters_hostnames.strip("\'"),
            expected_masters_hostnames)
        self.assertEqual(
            answer_slaves_hostnames.strip("\'"),
            expected_slaves_hostnames)

if __name__ == '__main__':
    test_probe_hadoop_examples_jars()
    test_probe_hadoop_test_examples_jars()
    test_probe_java_bin()
    test_probe_hadoop_release()
    test_probe_spark_version()
    test_probe_hadoop_conf_dir()
    test_probe_spark_conf_value()
    test_probe_java_opts()
    test_probe_masters_slaves_hostnames()
