package com.javatechie.saga.customerpartitioning.dto;

public class PublishResult {

    private String topic;
    private int partition;
    private long offset;
    private String key;
    private String eventId;

    public PublishResult() {
    }

    public PublishResult(String topic, int partition, long offset, String key, String eventId) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.eventId = eventId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
}

