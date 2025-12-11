package ca.digilogue.xp.service;

import ca.digilogue.xp.generator.OhlcvCandle;
import ca.digilogue.xp.repository.InfluxDbRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Service layer for InfluxDB operations.
 * Provides business logic for writing OHLCV candles.
 */
@Service
public class InfluxDbService {

    private static final Logger log = LoggerFactory.getLogger(InfluxDbService.class);

    private final InfluxDbRepository influxDbRepository;

    @Autowired
    public InfluxDbService(InfluxDbRepository influxDbRepository) {
        this.influxDbRepository = influxDbRepository;
    }

    /**
     * Writes an OHLCV candle to InfluxDB.
     * 
     * @param candle The OHLCV candle to write
     */
    public void writeCandle(OhlcvCandle candle) {
        try {
            influxDbRepository.writeCandle(candle);
            log.debug("Successfully wrote candle for symbol: {}", candle.getSymbol());
        } catch (Exception e) {
            log.error("Failed to write candle for symbol: {}", candle.getSymbol(), e);
            // Don't throw - allow generator to continue even if one write fails
        }
    }

    /**
     * Flushes any pending writes to InfluxDB.
     */
    public void flush() {
        influxDbRepository.flush();
    }
}

