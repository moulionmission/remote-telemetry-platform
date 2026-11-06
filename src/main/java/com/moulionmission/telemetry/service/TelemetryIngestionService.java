package com.moulionmission.telemetry.service;

import com.moulionmission.telemetry.model.TelemetryDTO;
import lombok.RequiredEncoder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class TelemetryIngestionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${telemetry.topic.events:telemetry.events.primary}")
    private String primaryTopic;

    /**
     * Partition-aware event routing:
     * Using deviceId as the partition key ensures all events from a given device
     * route to the exact same Kafka partition, guaranteeing strict chronological ordering.
     */
    public CompletableFuture<SendResult<String, Object>> ingestEvent(TelemetryDTO dto) {
        if (dto.getIdempotencyKey() == null || dto.getIdempotencyKey().isBlank()) {
            dto.setIdempotencyKey(UUID.randomUUID().toString());
        }
        if (dto.getTimestamp() == null) {
            dto.setTimestamp(Instant.now());
        }

        String partitionKey = dto.getDeviceId();
        log.debug("Routing telemetry event for device [{}] with key [{}]", partitionKey, dto.getIdempotencyKey());

        return kafkaTemplate.send(primaryTopic, partitionKey, dto);
    }
}

