package com.cicd.beginner;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/webhook/user")
public class Controller {
    @PostMapping
    public ResponseEntity<String> sendMessage() {
        System.out.println("Sending a message");
        return ResponseEntity.ok(HttpStatus.OK.toString());
    }
}
