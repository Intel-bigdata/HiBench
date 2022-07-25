#!/usr/bin/env python3
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
import fnmatch
import os
import re
import subprocess
import sys
from time import sleep
from time import time

from terminalsize import get_terminal_size


def load_colors():
    color_script_fn = os.path.join(
        os.path.dirname(__file__),
        "color.enabled.sh",
    )
    with open(color_script_fn) as f:
        return {
            k: v.split("'")[1].replace(r"\e[", "\033[")
            for k, v in [
                x.strip().split("=")
                for x in f.readlines()
                if x.strip() and not x.strip().startswith("#")
            ]
        }  # noqa: E501


Color = load_colors()
if int(os.environ.get("HIBENCH_PRINTFULLLOG", 0)):
    Color["ret"] = os.linesep
else:
    Color["ret"] = "\r"

tab_matcher = re.compile("\t")
tabstop = 8


def replace_tab_to_space(s):
    def tab_replacer(match):
        pos = match.start()
        length = pos % tabstop
        if not length:
            length += tabstop
        return " " * length

    return tab_matcher.sub(tab_replacer, s)


class _Matcher:
    hadoop = re.compile(r"^.*map\s*=\s*(\d+)%,\s*reduce\s*=\s*(\d+)%.*$")
    hadoop2 = re.compile(r"^.*map\s+\s*(\d+)%\s+reduce\s+\s*(\d+)%.*$")
    spark = re.compile(
        r"^.*finished task \S+ in stage \S+ \(tid \S+\) in.*on.*\((\d+)/(\d+)\)\s*$",  # noqa: E501
    )

    def match(self, line):
        for p in [self.hadoop, self.hadoop2]:
            m = p.match(line)
            if m:
                return (float(m.groups()[0]) + float(m.groups()[1])) / 2

        for p in [self.spark]:
            m = p.match(line)
            if m:
                return float(m.groups()[0]) / float(m.groups()[1]) * 100


matcher = _Matcher()


def show_with_progress_bar(line, progress, line_width):
    """
    Show text with progress bar.

    @progress:0-100
    @line: text to show
    @line_width: width of screen
    """
    pos = int(line_width * progress / 100)
    if len(line) < line_width:
        line = line + " " * (line_width - len(line))
    line = "{On_Yellow}{line_seg1}{On_Blue}{line_seg2}{Color_Off}{ret}".format(
        line_seg1=line[:pos],
        line_seg2=line[pos:],
        **Color,
    )
    sys.stdout.write(line)


def execute(workload_result_file, command_lines):
    proc = subprocess.Popen(
        " ".join(command_lines),
        shell=True,
        bufsize=1,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    count = 100
    last_time = 0
    log_file = open(workload_result_file, "w")
    # see http://stackoverflow.com/a/4417735/1442961
    lines_iterator = iter(proc.stdout.readline, b"")
    for line in lines_iterator:
        count += 1
        if (
            count > 100 or time() - last_time > 1
        ):  # refresh terminal size for 100 lines or each seconds
            count, last_time = 0, time()
            width, height = get_terminal_size()
            width -= 1

        try:
            line = line.rstrip()
            log_file.write(str(line) + "\n")
            log_file.flush()
        except KeyboardInterrupt:
            proc.terminate()
            break
        line = line.decode("utf-8")
        line = replace_tab_to_space(line)
        # print("{Red}log=>{Color_Off}".format(**Color), line)
        lline = line.lower()

        def table_not_found_in_log(line):
            table_not_found_pattern = "*Table * not found*"
            regex = fnmatch.translate(table_not_found_pattern)
            reobj = re.compile(regex)
            if reobj.match(line):
                return True
            else:
                return False

        def database_default_exist_in_log(line):
            database_default_already_exist = "Database default already exists"
            if database_default_already_exist in line:
                return True
            else:
                return False

        def uri_with_key_not_found_in_log(line):
            uri_with_key_not_found = (
                "Could not find uri with key [dfs.encryption.key.provider.uri]"
            )
            if uri_with_key_not_found in line:
                return True
            else:
                return False

        if ("error" in lline) and lline.lstrip() == lline:
            # Bypass hive 'error's and KeyProviderCache error
            bypass_error_condition = (
                table_not_found_in_log
                or database_default_exist_in_log(
                    lline,
                )
                or uri_with_key_not_found_in_log(lline)
            )
            if not bypass_error_condition:
                COLOR = "Red"
                sys.stdout.write(
                    ("{%s}{line}{Color_Off}{ClearEnd}\n" % COLOR)
                    .format(
                        line=line,
                        **Color,
                    )
                    .encode("utf-8"),
                )

        else:
            if len(line) >= width:
                line = line[: width - 4] + "..."
            progress = matcher.match(lline)
            if progress is not None:
                show_with_progress_bar(line, progress, width)
            else:
                sys.stdout.write(
                    "{line}{ClearEnd}{ret}".format(
                        line=line,
                        **Color,
                    ).encode("utf-8"),
                )
        sys.stdout.flush()
    print()
    log_file.close()
    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.kill()
        return 1
    return proc.returncode


def test_progress_bar():
    for i in range(101):
        show_with_progress_bar("test progress : %d" % i, i, 80)
        sys.stdout.flush()

        sleep(0.05)


if __name__ == "__main__":
    sys.exit(
        execute(
            workload_result_file=sys.argv[1],
            command_lines=sys.argv[2:],
        ),
    )
#    test_progress_bar()
