package com.kafka.kafkapublisher.config;

import com.kafka.kafkapublisher.model.KafkaConfigData;
import com.kafka.kafkapublisher.model.KafkaProducerConfigData;
import com.kafka.message.AvroMessage;
import java.io.Serializable;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@RequiredArgsConstructor
public class KafkaPublisherConfig <K extends Serializable, V extends SpecificRecordBase> {

  private final KafkaConfigData kafkaConfigData;
  private final KafkaProducerConfigData kafkaProducerConfigData;

  public Map<String, Object> producerConfig(){
    return Map.ofEntries(
        Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigData.getBootstrapServers()),
        Map.entry(kafkaConfigData.getSchemaRegistryUrlKey(),
            kafkaConfigData.getSchemaRegistryUrl()),
        Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            kafkaProducerConfigData.getKeySerializerClass()),
        Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            kafkaProducerConfigData.getValueSerializerClass()),
        Map.entry(ProducerConfig.BATCH_SIZE_CONFIG,
            kafkaProducerConfigData.getBatchSize()
                * kafkaProducerConfigData.getBatchSizeBoostFactor()),
        Map.entry(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerConfigData.getLingerMs()),
        Map.entry(ProducerConfig.COMPRESSION_TYPE_CONFIG,
            kafkaProducerConfigData.getCompressionType()),
        Map.entry(ProducerConfig.ACKS_CONFIG, kafkaProducerConfigData.getAcks()),
        Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
            kafkaProducerConfigData.getRequestTimeoutMs()),
        Map.entry(ProducerConfig.RETRIES_CONFIG, kafkaProducerConfigData.getRetryCount())
    );
  }

  @Bean
  public ProducerFactory<K, V> producerFactory() {
    DefaultKafkaProducerFactory<K, V> producerFactory = new DefaultKafkaProducerFactory<>(producerConfig());
    producerFactory.setValueSerializer(new AvroMessageSerializer());
    return producerFactory;
  }

  @Bean
  public KafkaTemplate<K, V> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }
}
