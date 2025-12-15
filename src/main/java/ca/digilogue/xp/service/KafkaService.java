package ca.digilogue.xp.service;

import ca.digilogue.xp.generator.OhlcvCandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Service layer for Kafka operations.
 * Provides business logic for publishing OHLCV candles collection to Kafka topics.
 */
@Service
public class KafkaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    private final KafkaTemplate<String, Map<String, OhlcvCandle>> kafkaTemplate;
    private final String ohlcvTopic;

    @Autowired
    public KafkaService(
            KafkaTemplate<String, Map<String, OhlcvCandle>> kafkaTemplate,
            @Value("${spring.kafka.topic.ohlcv:ohlcv-topic}") String ohlcvTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ohlcvTopic = ohlcvTopic;
    }

    /**
     * Publishes the entire collection of OHLCV candles to the Kafka topic as a single message.
     * The message value will be a JSON representation of the Map<String, OhlcvCandle>.
     * 
     * @param candles Map of symbol to OHLCV candle (the entire collection)
     */
    public void publishCandlesCollection(Map<String, OhlcvCandle> candles) {
        if (candles == null || candles.isEmpty()) {
            log.debug("No candles to publish, skipping");
            return;
        }
        
        try {
            // Use a fixed key for the collection message (or could use timestamp-based key)
            String key = "ohlcv-collection";
            CompletableFuture<SendResult<String, Map<String, OhlcvCandle>>> future = 
                kafkaTemplate.send(ohlcvTopic, key, candles);
            
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.debug("Successfully published candles collection ({} symbols) to topic: {}", 
                        candles.size(), ohlcvTopic);
                } else {
                    log.error("Failed to publish candles collection to topic: {}", 
                        ohlcvTopic, exception);
                }
            });
        } catch (Exception e) {
            log.error("Error publishing candles collection to topic: {}", 
                ohlcvTopic, e);
            // Don't throw - allow collector to continue even if one publish fails
        }
    }
}
