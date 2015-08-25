/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.streambench.samza.micro;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.lang.Deprecated;
import java.lang.Override;
import java.util.Random;

public class Sample implements StreamTask {
    private final SystemStream OUTPUT_STREAM = new SystemStream("kafka", "sample");
    private ThreadLocal<Random> rand = null;


    public Sample(){
        rand = threadRandom(1);
    }

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        String message = (String) envelope.getMessage();
        double randVal = rand.get().nextDouble();
        if (randVal <= CommonArg.getProb()) {
            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, message));
        }
    }

    public static ThreadLocal<Random> threadRandom(final long seed) {
        return new ThreadLocal<Random>() {
            @Override
            protected Random initialValue() {
                return new Random(seed);
            }
        };
    }
}
