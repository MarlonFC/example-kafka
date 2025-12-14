package com.example.kafka.controller;

import com.example.kafka.producer.VendaProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/vendas")
public class VendaController {

    private final VendaProducer producer;

    public VendaController(VendaProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<Void> post(@RequestParam(required = false) String key,
                                     @RequestBody String body) {
        producer.send(key, body);
        return ResponseEntity.accepted().build();
    }
}