package ifsc.edu.programacaodistribuidaeconcorrente;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("/register")
    public ResponseEntity<String> register(@RequestBody Map<String, String> body) {
        String userId = body.get("userId");
        String name = body.get("name");

        User user = userService.registerUser(userId, name);

        return ResponseEntity.ok("User registered: " + user.getUserId());
    }
}