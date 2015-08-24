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
