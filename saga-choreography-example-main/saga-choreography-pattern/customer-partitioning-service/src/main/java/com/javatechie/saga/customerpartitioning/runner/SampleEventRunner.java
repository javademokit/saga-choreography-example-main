package com.javatechie.saga.customerpartitioning.runner;

import com.javatechie.saga.customerpartitioning.dto.CustomerEventRequest;
import com.javatechie.saga.customerpartitioning.service.CustomerEventProducerService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collections;

@Component
@ConditionalOnProperty(name = "app.sample-runner.enabled", havingValue = "true")
public class SampleEventRunner implements CommandLineRunner {

    private final CustomerEventProducerService producerService;

    public SampleEventRunner(CustomerEventProducerService producerService) {
        this.producerService = producerService;
    }

    @Override
    public void run(String... args) {
        CustomerEventRequest request = new CustomerEventRequest();
        request.setCustomerId(1001L);
        request.setEventType("Customer Registration");
        request.setBusinessDomain("Banking");
        request.setPayload(Collections.singletonMap("source", "sample-runner"));

        producerService.publish(request);
    }
}

