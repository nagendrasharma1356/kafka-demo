package com.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ConsumerService {

    private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "kafka-topic", groupId = "kafka_group")
    public void consume(String message) {
        messages.add(message);
        System.out.println("Consumed message: " + message);
    }

    public List<String> getMessages() {
        return new ArrayList<String>(messages);
    }

    public void clearMessages() {
        messages.clear();
    }

    public List<String> searchMessages(String keyword) {
        return messages.stream()
                .filter(m -> m.contains(keyword))
                .collect(Collectors.toList());
    }

}
