package com.javatechie.saga.customerpartitioning.service;

import org.apache.kafka.common.utils.Utils;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class PartitionCalculator {

    public int partitionForCustomerId(long customerId, int partitions) {
        if (partitions <= 0) {
            throw new IllegalArgumentException("Partition count must be greater than zero");
        }

        byte[] keyBytes = String.valueOf(customerId).getBytes(StandardCharsets.UTF_8);
        return Utils.toPositive(Utils.murmur2(keyBytes)) % partitions;
    }
}

