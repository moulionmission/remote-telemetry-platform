package com.moulionmission.telemetry.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryDTO {
    private String deviceId;
    private String eventType;
    private double cpuUsage;
    private double memoryUsage;
    private double latencyMs;
    private double batteryLevel;
    private Instant timestamp;
    private String idempotencyKey;
}

