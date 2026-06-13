package com.javatechie.saga.customerpartitioning.controller;

import com.javatechie.saga.customerpartitioning.dto.CustomerEventRequest;
import com.javatechie.saga.customerpartitioning.dto.PublishResult;
import com.javatechie.saga.customerpartitioning.service.CustomerEventProducerService;
import com.javatechie.saga.customerpartitioning.service.PartitionCalculator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@Validated
public class CustomerEventController {

    private final CustomerEventProducerService producerService;
    private final PartitionCalculator partitionCalculator;

    @Value("${app.kafka.partition-count:6}")
    private int defaultPartitions;

    public CustomerEventController(CustomerEventProducerService producerService,
                                   PartitionCalculator partitionCalculator) {
        this.producerService = producerService;
        this.partitionCalculator = partitionCalculator;
    }

    @PostMapping("/api/customer-events")
    public PublishResult publish(@Valid @RequestBody CustomerEventRequest request) {
        return producerService.publish(request);
    }

    @GetMapping("/api/customer-events/partition/{customerId}")
    public Map<String, Object> calculatePartition(@PathVariable long customerId,
                                                  @RequestParam(required = false) Integer partitions) {
        int partitionCount = partitions != null ? partitions : defaultPartitions;
        int partition = partitionCalculator.partitionForCustomerId(customerId, partitionCount);

        Map<String, Object> result = new LinkedHashMap<String, Object>();
        result.put("customerId", customerId);
        result.put("partitions", partitionCount);
        result.put("formula", "partition = hash(customerId) % numberOfPartitions");
        result.put("partition", partition);
        return result;
    }
}

