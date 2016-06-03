package bnouyrigat.h2o;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerTestDriver {

    private final Properties consumerConfig;
    private final long waitTime;

    public KafkaConsumerTestDriver(Properties consumerConfig, long waitTime) {
        Properties properties = defaultConfig();
        properties.putAll(consumerConfig);
        this.consumerConfig = properties;
        this.waitTime = waitTime;
    }

    public KafkaConsumerTestDriver(Properties consumerConfig) {
        this(consumerConfig, 10_000);
    }

    private Properties defaultConfig() {
        Properties consumerConfig = new Properties();
//        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-test-driver");
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

    /**
     * Wait until enough data (value records) has been consumed.
     *
     * @param topic              Topic to consume from
     * @param expectedNumRecords Minimum number of expected records
     * @return All the records consumed, or null if no records are consumed
     * @throws InterruptedException
     * @throws AssertionError       if the given wait time elapses
     */
    public <V> List<V> waitUntilMinValuesRecordsReceived(String topic,
                                                         int expectedNumRecords) throws InterruptedException {
        List<V> accumData = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        while (true) {
            List<V> readData = readValues(topic, expectedNumRecords);
            accumData.addAll(readData);
            if (accumData.size() >= expectedNumRecords)
                return accumData;
            if (System.currentTimeMillis() > startTime + waitTime)
                throw new AssertionError("Expected " + expectedNumRecords +
                        " but received only " + accumData.size() +
                        " records before timeout " + waitTime + " ms");
            Thread.sleep(Math.min(waitTime, 100L));
        }
    }

}
