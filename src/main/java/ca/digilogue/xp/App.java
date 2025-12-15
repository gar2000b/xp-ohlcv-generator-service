package ca.digilogue.xp;

import ca.digilogue.xp.generator.OhlcvGenerator;
import ca.digilogue.xp.service.InfluxDbService;
import ca.digilogue.xp.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import ca.digilogue.xp.generator.OhlcvCandle;

@SpringBootApplication
public class App {

    public static String version;
    public static String instanceId;

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static ExecutorService executorService;
    private static final List<OhlcvGenerator> generators = new ArrayList<>();
    private static ConfigurableApplicationContext applicationContext;
    
    // Collection to store latest candles from all generators (keyed by symbol)
    private static final Map<String, OhlcvCandle> latestCandles = new ConcurrentHashMap<>();
    private static Thread candleCollectorThread;
    private static final AtomicBoolean candleCollectorRunning = new AtomicBoolean(false);
    
    /**
     * Symbol configuration: symbol name, base price, volatility
     */
    private record SymbolConfig(String symbol, double basePrice, double volatility) {}

    public static void main(String[] args) {
        applicationContext = SpringApplication.run(App.class, args);

        version = resolveVersion(applicationContext);
        instanceId = resolveInstanceId();

        log.info("xp-ohlcv-generator-service is running @ version: {}, instanceId: {}", version, instanceId);
        
        // Start OHLCV generators
        startGenerators();

        // Start candle collector thread
        startCandleCollector();
        
        // Register shutdown hook to stop generators gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down OHLCV generators...");
            stopCandleCollector();
            stopGenerators();
        }));
    }
    
    private static void startGenerators() {
        // Get InfluxDB service from Spring context
        InfluxDbService influxDbService = applicationContext.getBean(InfluxDbService.class);
        
        // Define symbols to generate
        List<SymbolConfig> symbolConfigs = List.of(
            new SymbolConfig("MEGA-USD", 100.0, 2.0),
            new SymbolConfig("HELIO-USD", 75.0, 1.5),
            new SymbolConfig("RUCKS-USD", 50.0, 3.0)
        );
        
        // Create thread pool with one thread per generator
        executorService = Executors.newFixedThreadPool(symbolConfigs.size(), r -> {
            Thread t = new Thread(r);
            t.setDaemon(true); // Allow JVM to exit even if threads are running
            return t;
        });
        
        // Create and start generators
        for (SymbolConfig config : symbolConfigs) {
            OhlcvGenerator generator = new OhlcvGenerator(
                config.symbol(),
                config.basePrice(),
                config.volatility(),
                influxDbService
            );
            generators.add(generator);
            executorService.submit(generator);
            log.info("Started OHLCV generator for symbol: {} (basePrice: {}, volatility: {})", 
                config.symbol(), config.basePrice(), config.volatility());
        }
        
        log.info("All OHLCV generators started ({} generators running)", generators.size());
    }
    
    /**
     * Starts a background thread that collects candles from all generators every second
     * and publishes them to Kafka.
     */
    private static void startCandleCollector() {
        // Get KafkaService from Spring context
        KafkaService kafkaService = applicationContext.getBean(KafkaService.class);
        
        candleCollectorRunning.set(true);
        candleCollectorThread = new Thread(() -> {
            log.info("Candle collector thread started");
            while (candleCollectorRunning.get()) {
                try {
                    // Collect latest candles from all generators
                    for (OhlcvGenerator generator : generators) {
                        OhlcvCandle candle = generator.getLatestCandle();
                        if (candle != null) {
                            latestCandles.put(candle.getSymbol(), candle);
                            
                            // Publish to Kafka
                            kafkaService.publishCandle(candle);
                        }
                    }
                    
                    // Sleep for 1 second
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Candle collector thread interrupted");
                    break;
                } catch (Exception e) {
                    log.error("Error collecting/publishing candles", e);
                }
            }
            log.info("Candle collector thread stopped");
        }, "candle-collector");
        candleCollectorThread.setDaemon(true);
        candleCollectorThread.start();
        log.info("Candle collector thread started");
    }
    
    /**
     * Stops the candle collector thread gracefully.
     */
    private static void stopCandleCollector() {
        candleCollectorRunning.set(false);
        if (candleCollectorThread != null) {
            candleCollectorThread.interrupt();
            try {
                candleCollectorThread.join(2000); // Wait up to 2 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted while waiting for candle collector thread to stop");
            }
        }
        log.info("Candle collector stopped");
    }
    
    private static void stopGenerators() {
        // Stop all generators
        for (OhlcvGenerator generator : generators) {
            generator.stop();
        }
        
        // Shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    log.warn("Forced shutdown of executor service");
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        log.info("All OHLCV generators stopped");
    }

    private static String resolveVersion(ConfigurableApplicationContext ctx) {
        // Preferred: use build-info (packaged JAR / Docker)
        try {
            BuildProperties buildProperties = ctx.getBean(BuildProperties.class);
            return buildProperties.getVersion();
        } catch (NoSuchBeanDefinitionException ignored) {
            // Fall back to parsing pom.xml (e.g., when running from IDE)
        }

        return readVersionFromPom();
    }

    private static String readVersionFromPom() {
        try {
            List<String> lines = Files.readAllLines(Paths.get("pom.xml"));

            boolean inParent = false;

            for (String line : lines) {
                String trimmed = line.trim();

                // Track whether we're inside the <parent>...</parent> block
                if (trimmed.startsWith("<parent>")) {
                    inParent = true;
                } else if (trimmed.startsWith("</parent>")) {
                    inParent = false;
                }

                // We only care about <version> tags OUTSIDE the <parent> section
                if (!inParent &&
                        trimmed.startsWith("<version>") &&
                        trimmed.endsWith("</version>")) {

                    return trimmed
                            .replace("<version>", "")
                            .replace("</version>", "")
                            .trim();
                }
            }
        } catch (Exception ignored) {
            // If something goes wrong reading pom.xml, fall through
        }

        // Last-resort fallback for local dev
        return "DEV";
    }

    private static String resolveInstanceId() {
        // First, try environment variable (set in docker-compose)
        String envInstanceId = System.getenv("INSTANCE_ID");
        if (envInstanceId != null && !envInstanceId.isEmpty()) {
            return envInstanceId;
        }

        // Fallback to hostname (Docker container name)
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }

        // Last resort: generate a UUID
        return java.util.UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Gets the generator for a specific symbol.
     * 
     * @param symbol The trading symbol (e.g., "MEGA-USD")
     * @return The OhlcvGenerator for the symbol, or null if not found
     */
        public static OhlcvGenerator getGenerator(String symbol) {
            return generators.stream()
                .filter(g -> g.getSymbol().equals(symbol))
                .findFirst()
                .orElse(null);
        }
        
        /**
         * Gets all active generators.
         * 
         * @return A copy of the generators list (to prevent external modification)
         */
        public static List<OhlcvGenerator> getGenerators() {
            return new ArrayList<>(generators);
        }
        
        /**
         * Gets the latest candle for a specific symbol from the collection.
         * 
         * @param symbol The trading symbol (e.g., "MEGA-USD")
         * @return The latest OHLCV candle for the symbol, or null if not found
         */
        public static OhlcvCandle getLatestCandle(String symbol) {
            return latestCandles.get(symbol);
        }
        
        /**
         * Gets all latest candles from all generators.
         * 
         * @return A copy of the latest candles map (to prevent external modification)
         */
        public static Map<String, OhlcvCandle> getAllLatestCandles() {
            return new ConcurrentHashMap<>(latestCandles);
        }
}
