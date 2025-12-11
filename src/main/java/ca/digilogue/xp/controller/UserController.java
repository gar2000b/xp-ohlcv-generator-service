package ca.digilogue.xp.controller;

import ca.digilogue.xp.model.User;
import ca.digilogue.xp.service.UserService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class UserController {

    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @GetMapping("/users")
    public ResponseEntity<List<User>> getAllUsers() {
        log.info("Received GET /users");

        List<User> users = userService.getAllUsers();

        log.info("GET /users → {} users returned", users.size());

        return ResponseEntity.ok(users); // 200 OK with JSON array of User
    }

    @GetMapping("/users/{id}")
    public ResponseEntity<User> getUserById(@PathVariable String id) {
        log.info("Received GET /users/{}", id);

        User user = userService.getUserById(id);

        if (user != null) {
            log.info("GET /users/{} → Found: {{\"username\":\"{}\"}}", id, user.getUsername());
            return ResponseEntity.ok(user);
        }

        log.warn("GET /users/{} → Not Found", id);
        return ResponseEntity.notFound().build();
    }
}
