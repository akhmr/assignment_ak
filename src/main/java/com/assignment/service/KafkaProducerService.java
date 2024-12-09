package com.assignment.service;

import java.io.Serializable;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService  implements Serializable {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void sendToKafka(String topic,String msg) {
		kafkaTemplate.send(topic, msg);
		//return "Message sent successfully!";
	}


	
//	 private void sendToKafka(String message) {
//	        Properties props = new Properties();
//	        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
//	        props.put("key.serializer", StringSerializer.class.getName());
//	        props.put("value.serializer", StringSerializer.class.getName());
//
//	        Producer<String, String> producer = new KafkaProducer<>(props);
//	        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, message));
//	        producer.close();
//	    }

}
