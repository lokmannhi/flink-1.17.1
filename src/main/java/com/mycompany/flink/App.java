package com.mycompany.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class App {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost"); // placeholder
        properties.setProperty("group.id", "flink_kafka_consumer");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanisms", "PLAIN");
        properties.setProperty("sasl.username", "0987654321"); // placeholder
        properties.setProperty("sasl.password", "1234567890"); // placeholder


        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "ROUTER.INCOMING_MESSAGE",       // topic
                new SimpleStringSchema(),        // deserialization schema
                properties                       // Kafka properties
        );

        // Add the source to the environment
        DataStream<String> kafkaStream = env.addSource(kafkaConsumer);

        // Process the stream and print to the console (for now)
        kafkaStream.print();

        // Execute the job
        env.execute("Kafka Monitoring Job");
    }
}
