package com.javatechie.saga.customerpartitioning.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PartitionCalculatorTest {

    private final PartitionCalculator partitionCalculator = new PartitionCalculator();

    @Test
    void shouldRouteSameCustomerToSamePartition() {
        int first = partitionCalculator.partitionForCustomerId(1001L, 6);
        int second = partitionCalculator.partitionForCustomerId(1001L, 6);

        Assertions.assertEquals(first, second);
    }

    @Test
    void shouldKeepPartitionWithinRange() {
        int partition = partitionCalculator.partitionForCustomerId(2001L, 6);

        Assertions.assertTrue(partition >= 0 && partition < 6);
    }

    @Test
    void shouldRejectInvalidPartitionCount() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> partitionCalculator.partitionForCustomerId(1001L, 0));
    }
}

