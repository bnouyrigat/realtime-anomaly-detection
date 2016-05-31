package bnouyrigat.h2o;

import bnouyrigat.h2o.processor.AnomalyDetectionProcessor;
import bnouyrigat.h2o.processor.ECGData;
import bnouyrigat.h2o.serializer.ECGDataJsonDeserializer;
import bnouyrigat.h2o.serializer.JsonSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class AnomalyDetectionStreaming {

    private KafkaStreams kafkaStreams;

    private TopologyBuilder createTopologyBuilder() {
        StringDeserializer stringDeserializer = new StringDeserializer();
        StringSerializer stringSerializer = new StringSerializer();
        JsonSerializer<ECGData> ecgDataJsonSerializer = new JsonSerializer<>();
        ECGDataJsonDeserializer ecgDataJsonDeserializer = new ECGDataJsonDeserializer();
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.addSource("SOURCE", stringDeserializer, ecgDataJsonDeserializer, "src-ecg-data")
                .addProcessor("PROCESS", AnomalyDetectionProcessor::new, "SOURCE")
                .addSink("SINK3", "anomaly-ecg-data", stringSerializer, ecgDataJsonSerializer, "PROCESS");

        return topologyBuilder;
    }

    public void start(Properties properties) throws InterruptedException {
        Properties streamingConfiguration = streamingConfiguration();
        streamingConfiguration.putAll(properties);
        kafkaStreams = new KafkaStreams(createTopologyBuilder(), streamingConfiguration);
        kafkaStreams.start();
        // Wait briefly for the topology to be fully up and running (otherwise it might miss some or all
        // of the input data we produce below).
        // Note: The sleep times are relatively high to support running the build on Travis CI.
        Thread.sleep(5000);
    }

    public void stop() {
        if (null == kafkaStreams) {
            throw new IllegalStateException();
        }
        kafkaStreams.close();
    }

    public static Properties streamingConfiguration() {
        Properties streamsConfiguration = new Properties();
//        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "anomaly-detection-streaming");
//        streamsConfiguration.put(StreamsConfig.JOB_ID_CONFIG, "anomaly-detection-job-1");
//        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
//        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return streamsConfiguration;
    }

}
