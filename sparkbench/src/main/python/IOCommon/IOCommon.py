# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os, re

class IOCommon(object):
    conf = {}
    def __init__(self, sc):
        self.sc = sc

    @classmethod
    def getProperty(cls, key, default = None):
        return cls.conf.get(key, default)

    @classmethod
    def getPropertiesFromFile(cls):
        def split(x):
            ret = re.split("\s", x.strip(), 1)
            if len(ret)<2: return (ret[0], '')
            return tuple(ret)
        prop_file = os.environ.get("SPARKBENCH_PROPERTIES_FILES", None)
        assert prop_file, "SPARKBENCH_PROPERTIES_FILES undefined!"
        with open(prop_file) as f:
            cls.conf = dict([split(x.strip()) for x in f.readlines() if x.strip() and x.strip()[0]!="#"])

    def load(self, filename, force_format=None):
        input_format = force_format if force_format else IOCommon.getProperty("sparkbench.inputformat", "Text")
        if input_format == "Text":
            return self.sc.textFile(filename)
        elif input_format == "Sequence":
            return self.sc.sequenceFile(filename, "org.apache.hadoop.io.NullWritable, org.apache.hadoop.io.Text")\
                .map(lambda x:x[1])
        else:
            raise Exception("Unknown input format: %s" % input_format)

    def save(selfself, filename, data, PropPrefix = "sparkbench.outputformat"):
        output_format = IOCommon.getProperty(PropPrefix, "Text")
        output_format_codec = IOCommon.getProperty(PropPrefix+".codec")

        if output_format == "Text":
            if not output_format_codec:   # isEmpty
                data.saveAsTextFile(filename)
            else:
                print "Warning, save as text file with a format codec is unsupported in python api"
                data.saveAsTextFile(filename)
                #data.saveAsTextFile(filename, output_format_codec)
        elif output_format == "Sequence":
            sequence_data = data.map(lambda x:(None, x))
            if not output_format_codec:   # isEmpty
                data.saveAsHadoopFile(filename, "org.apache.hadoop.mapred.SequenceFileOutputFormat",
                                      "org.apache.hadoop.io.NullWritable", "org.apache.hadoop.io.Text")
            else:
                data.saveAsHadoopFile(filename, "org.apache.hadoop.mapred.SequenceFileOutputFormat",
                                      "org.apache.hadoop.io.NullWritable", "org.apache.hadoop.io.Text",
                                      compressionCodecClass = output_format_codec)

IOCommon.getPropertiesFromFile()