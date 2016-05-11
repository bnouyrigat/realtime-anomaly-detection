package bnouyrigat.h2o;

import bnouyrigat.h2o.processor.ECGData;
import bnouyrigat.h2o.serializer.ECGDataJsonDeserializer;
import bnouyrigat.h2o.serializer.ECGDataJsonSerializer;
import bnouyrigat.h2o.streams.IntegrationTestUtils;
import bnouyrigat.h2o.streams.kafka.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

public class AnomalyDetectionStreamingTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static KafkaProducerTestDriver producerTestDriver;
    private static KafkaConsumerTestDriver consumerTestDriver;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic("src-ecg-data");
        CLUSTER.createTopic("anomaly-ecg-data");
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ECGDataJsonSerializer.class);
        producerTestDriver = new KafkaProducerTestDriver(producerConfig);

        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ECGDataJsonDeserializer.class);
        consumerTestDriver = new KafkaConsumerTestDriver(consumerConfig);
    }

    private AnomalyDetectionStreaming streamingUnderTest = new AnomalyDetectionStreaming();

    @Before
    public void setUp() throws IOException {
        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(testStreamingConfiguration());
    }

    @Test
    public void shouldDetectAnomaly() throws InterruptedException, ExecutionException {
        streamingUnderTest.start(testStreamingConfiguration());

        producerTestDriver.produceValue("src-ecg-data", new ECGData(ECGDataTestValues.ANOMALY_DOUBLE_ARRAY));


        List<ECGData> actualAnomalyKeyValueList = consumerTestDriver.readValues("anomaly-ecg-data", 1);
        assertThat(actualAnomalyKeyValueList).containsExactlyElementsOf(singletonList(new ECGData(ECGDataTestValues.ANOMALY_DOUBLE_ARRAY)));

        streamingUnderTest.stop();
    }

    @Test
    public void shouldNoDetectAnomaly() throws InterruptedException, ExecutionException {
        streamingUnderTest.start(testStreamingConfiguration());

        producerTestDriver.produceValue("src-ecg-data", new ECGData(ECGDataTestValues.CONFORME_DOUBLE_ARRAY));

        List<ECGData> actualAnomalyKeyValueList = consumerTestDriver.readValues("anomaly-ecg-data", 1);
        assertThat(actualAnomalyKeyValueList).isEmpty();

        streamingUnderTest.stop();
    }

    private static Properties testStreamingConfiguration() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "anomaly-detection-integration-test");
        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "anomaly-detection-job-1");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
        // Explicitly place the state directory under /tmp so that we can remove it via
        // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
        // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
        // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
        // accordingly.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return streamsConfiguration;
    }

}