package ca.digilogue.xp.service;

import ca.digilogue.xp.generator.OhlcvCandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Service layer for Kafka operations.
 * Provides business logic for publishing OHLCV candles to Kafka topics.
 */
@Service
public class KafkaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);

    private final KafkaTemplate<String, OhlcvCandle> kafkaTemplate;
    private final String ohlcvTopic;

    @Autowired
    public KafkaService(
            KafkaTemplate<String, OhlcvCandle> kafkaTemplate,
            @Value("${spring.kafka.topic.ohlcv:ohlcv-topic}") String ohlcvTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ohlcvTopic = ohlcvTopic;
    }

    /**
     * Publishes an OHLCV candle to the Kafka topic.
     * Uses the symbol as the message key for partitioning.
     * 
     * @param candle The OHLCV candle to publish
     */
    public void publishCandle(OhlcvCandle candle) {
        try {
            String key = candle.getSymbol();
            CompletableFuture<SendResult<String, OhlcvCandle>> future = 
                kafkaTemplate.send(ohlcvTopic, key, candle);
            
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.debug("Successfully published candle for symbol: {} to topic: {}", 
                        candle.getSymbol(), ohlcvTopic);
                } else {
                    log.error("Failed to publish candle for symbol: {} to topic: {}", 
                        candle.getSymbol(), ohlcvTopic, exception);
                }
            });
        } catch (Exception e) {
            log.error("Error publishing candle for symbol: {} to topic: {}", 
                candle.getSymbol(), ohlcvTopic, e);
            // Don't throw - allow collector to continue even if one publish fails
        }
    }

    /**
     * Publishes multiple candles to the Kafka topic.
     * 
     * @param candles Map of symbol to OHLCV candle
     */
    public void publishCandles(java.util.Map<String, OhlcvCandle> candles) {
        for (OhlcvCandle candle : candles.values()) {
            publishCandle(candle);
        }
    }
}
