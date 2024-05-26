package com.kafka.kafkapublisher.config;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

public class AvroMessageSerializer<V extends SpecificRecordBase> implements Serializer<V> {

  @Override
  public byte[] serialize(String topic, V data) {

    byte[] arr = new byte[100000];
    try {
      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        GenericDatumWriter<V> writer = new GenericDatumWriter<>(data.getSchema());
        writer.write(data, binaryEncoder);
        binaryEncoder.flush();
        arr = outputStream.toByteArray();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return arr;
  }
}
