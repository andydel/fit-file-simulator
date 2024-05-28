package org.agd;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.agd.fitfile.avro.FitSample;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class KafkaPublisher implements Runnable {

    private final ArrayDeque<FitSample> messageQueue = new ArrayDeque<>();
    private static final String SCHEMA_REGISTRY_URL = "https://psrc-571d82.europe-west2.gcp.confluent.cloud/";
    private final KafkaProducer<String, FitSample> producer;

    public KafkaPublisher() {
        try {
            producer = initialiseProducer();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        new Thread(this).start();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close, "Shutdown-thread"));
    }

    public void publishMessage(FitSample fitSample) {
        messageQueue.add(fitSample);
    }


    @Override
    public void run() {
        Instant lastTime = null;
        while (true) {
            if (!messageQueue.isEmpty()) {
                var fitSample = messageQueue.poll();
                try {
                    publish(fitSample);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    producer.flush();
                }
                var currentTime = fitSample.getTimestamp();
                if (lastTime != null) {
                    var timeDiff = currentTime.toEpochMilli() - lastTime.toEpochMilli();
                    if (timeDiff > 0) {
                        try {
                            Thread.sleep(timeDiff);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                lastTime = currentTime;
            }
        }
    }

    private void publish(FitSample fitSample) throws ExecutionException, InterruptedException, TimeoutException {
        var ack = producer.send(new ProducerRecord<>("fit-sample", fitSample.getTimestamp().toString(), fitSample));
        var md = ack.get(30, TimeUnit.SECONDS);
        if (md.hasOffset()) {
            System.out.printf("Sent with offset %d%n", md.offset());
        } else if (ack.exceptionNow() != null) {
            throw new RuntimeException(ack.exceptionNow());
        }
        //producer.flush();

    }

    private KafkaProducer<String, FitSample> initialiseProducer() throws IOException {
        Properties props = readConfig();
        //setup credentials
        var creds = String.format("org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';", System.getenv().get("KAFKA_USER"), System.getenv().get("KAFKA_PASSWORD"));
        props.put("sasl.jaas.config", creds);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("schema.registry.basic.auth.user.info", String.format("%s:%s", System.getenv().get("schema.registry.api.key"), System.getenv().get("schema.registry.api.secret")));

        return new KafkaProducer<>(props);
    }

    private static Properties readConfig() throws IOException {

        final Properties config = new Properties();
        try (InputStream inputStream = KafkaPublisher.class.getClassLoader().getResourceAsStream("client.properties")) {
            config.load(inputStream);
        }

        return config;
    }
}
