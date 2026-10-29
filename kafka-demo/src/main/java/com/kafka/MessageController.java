package com.kafka;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("api/kafka")
public class MessageController {

    private final ProducerService producerService;
    private final ConsumerService consumerService;

    public MessageController(ProducerService producerService, ConsumerService consumerService) {
        this.producerService = producerService;
        this.consumerService = consumerService;
    }

    @PostMapping("/publish")
    public ResponseEntity<String> publish(@RequestParam String message) {
        producerService.sendMessage(message);

        return ResponseEntity.ok("Message sent to kafka successfully.");
    }

    @GetMapping("/messages")
    public ResponseEntity<List<String>> getMessages() {
        return ResponseEntity.ok(consumerService.getMessages());
    }

    @GetMapping("/search")
    public ResponseEntity<List<String>> searchMessages(@RequestParam String keyword) {
        return ResponseEntity.ok(consumerService.searchMessages(keyword));
    }

    @GetMapping("/count")
    public ResponseEntity<Integer> countMessages() {
        return ResponseEntity.ok(consumerService.getMessages().size());
    }

    @DeleteMapping("/clear")
    public ResponseEntity<String> clearMessages() {
        consumerService.clearMessages();
        return ResponseEntity.ok("Message list cleared.");
    }
}
