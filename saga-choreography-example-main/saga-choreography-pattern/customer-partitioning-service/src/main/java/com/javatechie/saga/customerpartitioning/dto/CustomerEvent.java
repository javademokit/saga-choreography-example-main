package com.javatechie.saga.customerpartitioning.dto;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public class CustomerEvent {

    private String eventId;
    private Long customerId;
    private String eventType;
    private String businessDomain;
    private Instant eventTime;
    private Map<String, Object> payload;

    public static CustomerEvent fromRequest(CustomerEventRequest request) {
        CustomerEvent event = new CustomerEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setCustomerId(request.getCustomerId());
        event.setEventType(request.getEventType());
        event.setBusinessDomain(request.getBusinessDomain());
        event.setEventTime(Instant.now());
        event.setPayload(request.getPayload());
        return event;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getBusinessDomain() {
        return businessDomain;
    }

    public void setBusinessDomain(String businessDomain) {
        this.businessDomain = businessDomain;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }
}

