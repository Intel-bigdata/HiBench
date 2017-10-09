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

from sys import argv

def gen_stream_sql(sql_index_list, throughtput_test_resource_dir, throughput_test_bin_dir, throughput_scale):

    sql_index_list = sql_index_list.split(",")
    sql_list = []
    end_sign = ";\n"

    bin_template_beeline = """
        SUBMIT_CMD=\"${BEELINE_CMD} ${BEELINE_GLOBAL_OPTS} -f %s\"
        execute_withlog ${SUBMIT_CMD}
    """.strip().replace("\t", "")

    for i in sql_index_list:
        path = throughtput_test_resource_dir + "/q" + str(i) + ".sql"
        single_sql = open(path, "r")
        sql_list.append(single_sql.read())


    for i in range(0, int(throughput_scale)):
        path_sql = throughtput_test_resource_dir + "/stream" + str(i) + ".sql"
        sum_sql = open(path_sql, "w")
        for j in range(0, len(sql_index_list)):
            sum_sql.write(sql_list[(j + i) % len(sql_index_list)])
            sum_sql.write(end_sign)
        sum_sql.close()
        path_bin = throughput_test_bin_dir + "/stream" + str(i) + ".sh"
        file_bin = open(path_bin, "w")
        file_bin.write(bin_template_beeline % path_sql)

if __name__ == "__main__":
    if len(argv) < 4:
        raise Exception(
            "Please supply <sql_index_list> <throughtput_test_resource_dir> <throughput_test_bin_dir> <throughput_scale>")
    sql_index_list, throughtput_test_resource_dir, throughput_test_bin_dir, throughput_scale = argv[1], argv[2], argv[3], argv[4]
    gen_stream_sql(sql_index_list, throughtput_test_resource_dir, throughput_test_bin_dir, throughput_scale)