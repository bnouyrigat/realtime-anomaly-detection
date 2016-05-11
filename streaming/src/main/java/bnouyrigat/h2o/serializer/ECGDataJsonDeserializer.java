package bnouyrigat.h2o.serializer;

import bnouyrigat.h2o.processor.ECGData;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class ECGDataJsonDeserializer<T> implements Deserializer<ECGData> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
//        objectMapper = new ObjectMapper();
    }

    @Override
    public ECGData deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, ECGData.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {

    }
}
