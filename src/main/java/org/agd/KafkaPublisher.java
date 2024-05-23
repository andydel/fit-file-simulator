package org.agd;

import org.agd.fitfile.avro.FitSample;

import java.time.Instant;
import java.util.ArrayDeque;

public class KafkaPublisher implements Runnable {

    ArrayDeque<FitSample> messageQueue = new ArrayDeque<>();

    public void publishMessage(FitSample fitSample) {
        messageQueue.add(fitSample);
    }

    @Override
    public void run() {
        Instant lastTime = null;
        while (true) {
            if (!messageQueue.isEmpty()) {
                var fitSample = messageQueue.poll();
                System.out.println(fitSample);
                var currentTime = fitSample.getTimestamp();
                if (lastTime != null) {
                    var timeDiff = currentTime.toEpochMilli() - lastTime.toEpochMilli();
                    if (timeDiff > 0) {
                        try {
                            Thread.sleep(timeDiff);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                lastTime = currentTime;
            }
        }
    }
}
