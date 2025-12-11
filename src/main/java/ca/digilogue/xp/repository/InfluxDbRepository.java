package ca.digilogue.xp.repository;

import ca.digilogue.xp.generator.OhlcvCandle;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

/**
 * Repository for InfluxDB operations.
 * Handles low-level InfluxDB client interactions.
 */
@Repository
public class InfluxDbRepository {

    private static final Logger log = LoggerFactory.getLogger(InfluxDbRepository.class);

    private final InfluxDBClient influxDBClient;
    private final String bucket;
    private final String org;
    private WriteApi writeApi;

    @Autowired
    public InfluxDbRepository(InfluxDBClient influxDBClient, 
                              @Value("${influxdb.bucket}") String bucket,
                              @Value("${influxdb.org}") String org) {
        this.influxDBClient = influxDBClient;
        this.bucket = bucket;
        this.org = org;
        this.writeApi = influxDBClient.getWriteApi();
    }

    /**
     * Writes an OHLCV candle to InfluxDB.
     * 
     * @param candle The OHLCV candle to write
     */
    public void writeCandle(OhlcvCandle candle) {
        try {
            Point point = Point.measurement("ohlcv_candles")
                .addTag("symbol", candle.getSymbol())
                .addField("open", candle.getOpen())
                .addField("high", candle.getHigh())
                .addField("low", candle.getLow())
                .addField("close", candle.getClose())
                .addField("volume", candle.getVolume())
                .time(candle.getTimestamp(), WritePrecision.NS);

            writeApi.writePoint(bucket, org, point);
            
            log.debug("Written OHLCV candle to InfluxDB: {}", candle);
        } catch (Exception e) {
            log.error("Error writing OHLCV candle to InfluxDB: {}", candle, e);
            throw new RuntimeException("Failed to write candle to InfluxDB", e);
        }
    }

    /**
     * Flushes any pending writes to InfluxDB.
     */
    public void flush() {
        try {
            writeApi.flush();
        } catch (Exception e) {
            log.error("Error flushing writes to InfluxDB", e);
        }
    }

    /**
     * Closes the write API (should be called on shutdown).
     */
    public void close() {
        if (writeApi != null) {
            writeApi.close();
        }
    }
}

