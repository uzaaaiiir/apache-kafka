package kafkademo.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer.");

        // create Producer Properties
        Properties properties = new Properties();

        // Connect to Localhost
        // properties.setProperty("bootstrap.servers", "localhost:9092");

        // Connect to Confluence Cluster
        properties.setProperty("bootstrap.servers", "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='ZAWT4CQKOHYZQPIE' password='2QNxf7UAmTBPo1ZjB7tyD5vRQujGL1xZth0JsQH1JtPVA6lZX+128x0W+MidfI5V';");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id " + i;
                String value = "Event " + i;

                // Create Producer Record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // Send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record success sent or an exception is thrown
                        if (exception == null) {
                            // record successfully sent
                            log.info("Key: " + key + "| Partition: " + metadata.partition());
                        } else {
                            log.error("Error while sending event: " + exception);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Tell the Producer to send all the data and block until done -- synchronous
        producer.flush();

        // Flush and close Producer
        producer.close();
    }
}
