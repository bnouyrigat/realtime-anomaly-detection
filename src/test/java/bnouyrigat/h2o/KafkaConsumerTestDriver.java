package bnouyrigat.h2o;

import io.confluent.examples.streams.IntegrationTestUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;

import java.util.List;
import java.util.Properties;

public class KafkaConsumerTestDriver {

    private final Properties consumerConfig;

    public KafkaConsumerTestDriver(Properties consumerConfig) {
        Properties properties = defaultConfig();
        properties.putAll(consumerConfig);
        this.consumerConfig = properties;
    }

    private Properties defaultConfig() {
        Properties consumerConfig = new Properties();
//        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-lambda-integration-test-standard-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }

    public <K, V> List<KeyValue<K, V>> readKeyValues(String topic) {
        return IntegrationTestUtils.readKeyValues(topic, consumerConfig);
    }

    public <V> List<V> readValues(String topic, int maxMessages) {
        return IntegrationTestUtils.readValues(topic, consumerConfig, maxMessages);
    }
}
