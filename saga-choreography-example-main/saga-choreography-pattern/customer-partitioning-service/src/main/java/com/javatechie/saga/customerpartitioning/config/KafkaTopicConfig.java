package com.javatechie.saga.customerpartitioning.config;

import com.javatechie.saga.customerpartitioning.dto.CustomerEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

	@Value("${spring.kafka.bootstrap-servers:localhost:9092}")
	private String bootstrapServers;

	@Value("${app.kafka.customer-topic:customer-events}")
	private String topic;

	@Value("${app.kafka.partition-count:6}")
	private int partitionCount;

	@Bean
	public ProducerFactory<String, CustomerEvent> producerFactory() {
		Map<String, Object> props = new HashMap<String, Object>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		return new DefaultKafkaProducerFactory<String, CustomerEvent>(props);
	}

	@Bean
	public KafkaTemplate<String, CustomerEvent> kafkaTemplate() {
		return new KafkaTemplate<String, CustomerEvent>(producerFactory());
	}

	@Bean
	public NewTopic customerEventsTopic() {
		return TopicBuilder.name(topic)
				.partitions(partitionCount)
				.replicas(1)
				.build();
	}
}

