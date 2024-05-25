package com.kafka.kafkapublisher.service.impl;

import com.kafka.kafkapublisher.service.PublisherService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class PublisherServiceImpl implements PublisherService {

  private final KafkaTemplate<String, String> publisher;

  @Override
  public void sendMessage(String topicName, String key, String message) {
    publisher.send(topicName, key, message)
        .whenComplete((result, ex) -> {
          if (ex == null) {
            log.info("Send message {} with offset {}", message,
                result.getRecordMetadata().offset());
          } else {
            log.error("Unable to send message {} due to {}", message, ex.getMessage());
          }
        });
  }
}
