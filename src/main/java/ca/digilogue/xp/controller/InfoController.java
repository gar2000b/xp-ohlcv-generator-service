package ca.digilogue.xp.controller;

import ca.digilogue.xp.App;
import ca.digilogue.xp.model.Info;
import ca.digilogue.xp.model.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class InfoController {

    private static final Logger log = LoggerFactory.getLogger(InfoController.class);

    @GetMapping("/test")
    public ResponseEntity<Test> getTestMessage() {
        Test response = new Test("XP OHLCV Generator Service is alive!");

        // Log JSON-style representation
        log.info("GET /test JSON Response: {{\"message\":\"{}\"}}", response.getMessage());

        return ResponseEntity.ok(response); // 200 OK
    }

    @GetMapping("/info")
    public ResponseEntity<Info> getServiceInfo() {
        Info response = new Info("XP OHLCV Generator Service", App.version);

        // Log JSON-style representation
        log.info(
                "GET /info JSON Response: {{\"appName\":\"{}\",\"version\":\"{}\"}}",
                response.getAppName(),
                response.getVersion()
        );

        return ResponseEntity.ok(response); // 200 OK
    }
}