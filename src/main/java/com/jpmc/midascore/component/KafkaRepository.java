package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaRepository { // You can name this class whatever you want, e.g., MidasKafkaListener
    private static final Logger logger = LoggerFactory.getLogger(KafkaRepository.class);

    // The "${general.kafka-topic}" tells Spring to go to application.yml 
    // and find the value for "general.kafka-topic"
    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        logger.info("Received Transaction: {}", transaction);
    }
}