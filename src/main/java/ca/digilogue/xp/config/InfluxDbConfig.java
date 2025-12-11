package ca.digilogue.xp.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxDbConfig {

    private static final Logger log = LoggerFactory.getLogger(InfluxDbConfig.class);

    @Value("${influxdb.url}")
    private String influxDbUrl;

    @Value("${influxdb.token}")
    private String influxDbToken;

    @Value("${influxdb.org}")
    private String influxDbOrg;

    @Bean
    public InfluxDBClient influxDBClient() {
        log.info("Creating InfluxDB client for URL: {}, Org: {}", influxDbUrl, influxDbOrg);
        
        InfluxDBClient client = InfluxDBClientFactory.create(
            influxDbUrl,
            influxDbToken.toCharArray(),
            influxDbOrg
        );
        
        log.info("InfluxDB client created successfully");
        return client;
    }

    @Bean
    public String influxDbBucket(@Value("${influxdb.bucket}") String bucket) {
        return bucket;
    }
}

