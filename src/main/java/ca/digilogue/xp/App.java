package ca.digilogue.xp;

import ca.digilogue.xp.generator.OhlcvGenerator;
import ca.digilogue.xp.service.InfluxDbService;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class App {

    public static String version;

    private static final Logger log = LoggerFactory.getLogger(App.class);
    private static ExecutorService executorService;
    private static final List<OhlcvGenerator> generators = new ArrayList<>();
    private static ConfigurableApplicationContext applicationContext;
    
    /**
     * Symbol configuration: symbol name, base price, volatility
     */
    private record SymbolConfig(String symbol, double basePrice, double volatility) {}

    public static void main(String[] args) {
        applicationContext = SpringApplication.run(App.class, args);

        version = resolveVersion(applicationContext);

        log.info("xp-ohlcv-generator-service is running @ version: {}", version);
        
        // Start OHLCV generators
        startGenerators();
        
        // Register shutdown hook to stop generators gracefully
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down OHLCV generators...");
            stopGenerators();
        }));
    }
    
    private static void startGenerators() {
        // Get InfluxDB service from Spring context
        InfluxDbService influxDbService = applicationContext.getBean(InfluxDbService.class);
        
        // Define symbols to generate
        List<SymbolConfig> symbolConfigs = List.of(
            new SymbolConfig("MEGA/USD", 100.0, 2.0),
            new SymbolConfig("HELIO/USD", 75.0, 1.5),
            new SymbolConfig("RUCKS/USD", 50.0, 3.0)
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
}
