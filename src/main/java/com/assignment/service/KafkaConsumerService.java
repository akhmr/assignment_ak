package com.assignment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

	@KafkaListener(topics = "evenTopic", groupId = "my-group")
    public void listenToEvenAge(String message) {
		logger.info("Msg recived at evenTopic {} ",message);
        System.out.println("Received message from test topic: " + message);
    }
	
	@KafkaListener(topics = "oddTopic", groupId = "my-group")
    public void listenToOddAge(String message) {
		logger.info("Msg recived at oddTopic {} ",message);
    }

}
