package com.kafka.kafkapublisher;

import com.kafka.kafkapublisher.service.PublisherService;
import com.kafka.message.AvroMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class KafkaPublisherApplication implements CommandLineRunner {

  private final PublisherService publisherService;

  public static void main(String[] args) {
    SpringApplication.run(KafkaPublisherApplication.class, args);
  }

  @Override
  public void run(String... args) {
    AvroMessage avroMessage = AvroMessage.newBuilder()
        .setId(0)
        .setName("Gino")
        .build();
    for (int i = 0; i < 100; i++) {
      avroMessage.setId(i);
      publisherService.sendMessage("topic-example", "a1", avroMessage);
    }
  }
}
