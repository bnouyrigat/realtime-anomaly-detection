package bnouyrigat.h2o;

import io.confluent.examples.streams.IntegrationTestUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerTestDriver {

    private final Properties producerConfig;

    public KafkaProducerTestDriver(Properties producerConfig) {
        Properties properties = configuration();
        properties.putAll(producerConfig);
        this.producerConfig = properties;
    }

    public <V> void produceValuesSynchronously(String inputTopic, Collection<V> inputValues) throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);
    }

    private static Properties configuration() {
        Properties producerConfig = new Properties();
//        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerConfig;
    }

    public <V> void produceValue(String topic, V data) throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceValuesSynchronously(topic, Collections.singletonList(data), producerConfig);
    }
}
