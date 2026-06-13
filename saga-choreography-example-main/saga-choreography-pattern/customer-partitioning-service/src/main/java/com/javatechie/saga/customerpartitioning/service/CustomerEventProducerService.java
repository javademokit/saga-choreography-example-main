package com.javatechie.saga.customerpartitioning.service;

import com.javatechie.saga.customerpartitioning.dto.CustomerEvent;
import com.javatechie.saga.customerpartitioning.dto.CustomerEventRequest;
import com.javatechie.saga.customerpartitioning.dto.PublishResult;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class CustomerEventProducerService {

    private final KafkaTemplate<String, CustomerEvent> kafkaTemplate;

    @Value("${app.kafka.customer-topic:customer-events}")
    private String topic;

    public CustomerEventProducerService(KafkaTemplate<String, CustomerEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public PublishResult publish(CustomerEventRequest request) {
        CustomerEvent event = CustomerEvent.fromRequest(request);
        String key = String.valueOf(request.getCustomerId());

        try {
            SendResult<String, CustomerEvent> sendResult =
                    kafkaTemplate.send(topic, key, event).get(10, TimeUnit.SECONDS);

            return new PublishResult(
                    sendResult.getRecordMetadata().topic(),
                    sendResult.getRecordMetadata().partition(),
                    sendResult.getRecordMetadata().offset(),
                    key,
                    event.getEventId()
            );
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish customer event", e);
        }
    }
}

