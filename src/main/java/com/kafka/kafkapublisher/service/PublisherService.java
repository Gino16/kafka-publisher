package com.kafka.kafkapublisher.service;

public interface PublisherService {
  public void sendMessage(String topicName, String key, String message);
}
