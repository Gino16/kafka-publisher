package com.kafka.kafkapublisher.service;

import com.kafka.message.AvroMessage;

public interface PublisherService {
  public void sendMessage(String topicName, String key, AvroMessage message);
}
