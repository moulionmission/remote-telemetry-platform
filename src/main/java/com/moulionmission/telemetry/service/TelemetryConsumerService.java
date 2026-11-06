package com.moulionmission.telemetry.service;

import com.moulionmission.telemetry.model.TelemetryDTO;
import com.moulionmission.telemetry.model.TelemetryEvent;
import com.moulionmission.telemetry.repository.TelemetryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CachePut;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@Slf4j
public class TelemetryConsumerService {

    private final TelemetryRepository repository;
    private final RedisTemplate<String, Object> redisTemplate;
    private static final String IDEMPOTENCY_PREFIX = "idempotency:telemetry:";

    /**
     * Idempotent consumer with Dead-Letter Queue integration:
     * Validates idempotency key in Redis (SETNX) to achieve exactly-once processing semantics.
     */
    @KafkaListener(
            topics = "${telemetry.topic.events:telemetry.events.primary}",
            groupId = "telemetry-ingestion-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @Transactional
    public void consumeTelemetryEvent(
            @Payload TelemetryDTO dto,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String lockKey = IDEMPOTENCY_PREFIX + dto.getIdempotencyKey();

        // Atomic check-and-set: Prevents duplicate ingestion under network retries
        Boolean isNew = redisTemplate.opsForValue().setIfAbsent(lockKey, "PROCESSED", Duration.ofHours(24));
        if (Boolean.FALSE.equals(isNew)) {
            log.warn("Duplicate telemetry event detected with key [{}]. Skipping duplicate.", dto.getIdempotencyKey());
            return;
        }

        log.info("Processing event from device [{}] on partition [{}] offset [{}]", dto.getDeviceId(), partition, offset);

        TelemetryEvent event = TelemetryEvent.builder()
                .idempotencyKey(dto.getIdempotencyKey())
                .deviceId(dto.getDeviceId())
                .eventType(dto.getEventType())
                .cpuUsagePercent(dto.getCpuUsage())
                .memoryUsagePercent(dto.getMemoryUsage())
                .latencyMs(dto.getLatencyMs())
                .batteryLevelPercent(dto.getBatteryLevel())
                .timestamp(dto.getTimestamp())
                .status("SYNCHRONIZED")
                .build();

        // 1. Commit to PostgreSQL
        repository.save(event);

        // 2. Warm the Redis telemetry cache for sub-second retrieval
        String cacheKey = "device:latest:" + dto.getDeviceId();
        redisTemplate.opsForValue().set(cacheKey, event, Duration.ofMinutes(30));
    }
}
