package com.assignment.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.assignment.service.KafkaProducerService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RestController
public class DataController {
	
	private static final Logger logger = LoggerFactory.getLogger(DataController.class);
	
	@Autowired
	private KafkaProducerService kafkaProducerService;
	
	private final ObjectMapper objectMapper = new ObjectMapper();
	
	
	
	@PostMapping("/send")
    public String sendMessage(@RequestBody Person person) throws JsonProcessingException {
		logger.info("Person {} ",person);
		kafkaProducerService.sendToKafka("personTopic", objectMapper.writeValueAsString(person));
        return "Message sent successfully!";
    }

	@GetMapping("/health")
	public String heatlh() {
		return "ok";
	}
	
	

}
