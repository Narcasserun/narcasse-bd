package com.narcasse.kafka.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogProducer {

    private static final Logger logger = LoggerFactory.getLogger(LogProducer.class);

    public static void main(String args[]) {

        int counter = 1;
        while (counter <= 20) {

            logger.error("Log4jToKafka test" + counter);

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            counter++;
        }
    }
}
