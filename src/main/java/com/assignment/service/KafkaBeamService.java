package com.assignment.service;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.assignment.controller.Person;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.annotation.PostConstruct;

@Service
public class KafkaBeamService {

	private static final Logger logger = LoggerFactory.getLogger(KafkaBeamService.class);

	@Value("${spring.kafka.topic.input}")
	private String inputTopic;

	@Value("${spring.kafka.topic.even}")
	private String evenTopic;

	@Value("${spring.kafka.topic.odd}")
	private String oddTopic;

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@PostConstruct
	public void runPipeline() {
		PipelineOptions options = PipelineOptionsFactory.create();
		options.setRunner(DirectRunner.class);
		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> messages = pipeline
				.apply(KafkaIO.<String, String>read()
						.withBootstrapServers(bootstrapServers)
						.withTopic(inputTopic)
						.withKeyDeserializer(StringDeserializer.class)
						.withValueDeserializer(StringDeserializer.class)
						.withoutMetadata())
				.apply(MapElements.into(TypeDescriptor.of(String.class)).via((kafkaRecord) -> kafkaRecord.getValue()));
		
		PCollection<KV<String, String>> processedMessages = messages.apply("ProcessedMessagesByAge",
				ParDo.of(new ProcessedMessageFn()));
		PCollection<KV<String, Iterable<String>>> groupedMessages = processedMessages.apply("GroupByAgeType",
				GroupByKey.create());

		groupedMessages.apply("WriteEvenMessagesToKafka", ParDo.of(new WriteToKafkaFn(evenTopic, oddTopic)));

		new Thread(() -> pipeline.run().waitUntilFinish()).start();
	}

	public static class ProcessedMessageFn extends DoFn<String, KV<String, String>> {
		@ProcessElement
		public void processElement(@Element String message, OutputReceiver<KV<String, String>> out) {
			ObjectMapper objectMapper = new ObjectMapper();
			try {
				Person person = objectMapper.readValue(message, Person.class);
				int age = calculateAge(person.getDateOfBirth());

				if (age != -1) {
					if (age % 2 == 0) {
						out.output(KV.of("even", "Age is even and age is =" + age));
					} else {
						out.output(KV.of("odd", "Age is odd and age is =" + age));
					}
				}
			} catch (Exception e) {
				logger.error("Error occurred while processing message: {}", e.getMessage());
				out.output(KV.of("error", "exception occurred"));
			}
		}
	}

	public static class WriteToKafkaFn extends DoFn<KV<String, Iterable<String>>, Void> {

		private final String evenTopic;
		private final String oddTopic;

		public WriteToKafkaFn(String evenTopic, String oddTopic) {
			this.evenTopic = evenTopic;
			this.oddTopic = oddTopic;
		}

		@ProcessElement
		public void processElement(@Element KV<String, Iterable<String>> groupedMessages, OutputReceiver<Void> out) {
			String key = groupedMessages.getKey();
			Iterable<String> messages = groupedMessages.getValue();
			String topic = (key.equals("even")) ? evenTopic : (key.equals("odd")) ? oddTopic : null;
			if (topic != null) {
				for (String message : messages) {
					logger.info("Sending message to {} topic: {}", topic, message);
				}
			} else {
				logger.error("Unknown key for grouping: {}", key);
			}
		}
	}

	private static int calculateAge(String dateOfBirth) {
		try {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
			LocalDate dob = LocalDate.parse(dateOfBirth, formatter);
			return Period.between(dob, LocalDate.now()).getYears();
		} catch (Exception e) {
			logger.error("Error {}", e);
			return -1; // Return -1 if parsing fails
		}
	}

}