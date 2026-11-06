package com.moulionmission.telemetry;

import com.moulionmission.telemetry.model.TelemetryDTO;
import com.moulionmission.telemetry.service.TelemetryIngestionService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TelemetryPipelineTests {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private TelemetryIngestionService ingestionService;

    @Test
    void testPartitionAwareRouting() {
        TelemetryDTO dto = TelemetryDTO.builder()
                .deviceId("device-cluster-alpha-001")
                .eventType("DEVICE_HEALTH")
                .cpuUsage(42.5)
                .memoryUsage(68.2)
                .latencyMs(12.4)
                .batteryLevel(98.0)
                .idempotencyKey(UUID.randomUUID().toString())
                .timestamp(Instant.now())
                .build();

        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(any(), eq("device-cluster-alpha-001"), eq(dto))).thenReturn(future);

        CompletableFuture<SendResult<String, Object>> result = ingestionService.ingestEvent(dto);

        assertNotNull(result);
        verify(kafkaTemplate).send(any(), eq("device-cluster-alpha-001"), eq(dto));
    }
}

