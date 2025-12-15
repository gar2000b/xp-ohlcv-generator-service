package ca.digilogue.xp.config;

import ca.digilogue.xp.generator.OhlcvCandle;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for publishing OHLCV candles collection.
 * Publishes the entire Map<String, OhlcvCandle> as a single JSON message.
 * Uses Jackson 3's JacksonJsonSerializer (replaces deprecated JsonSerializer).
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, Map<String, OhlcvCandle>> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        
        // Producer reliability settings
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Map<String, OhlcvCandle>> kafkaTemplate(ProducerFactory<String, Map<String, OhlcvCandle>> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }
}
