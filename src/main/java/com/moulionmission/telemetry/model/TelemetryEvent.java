package com.moulionmission.telemetry.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;

@Entity
@Table(name = "telemetry_events")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TelemetryEvent implements Serializable {

    @Id
    private String idempotencyKey;

    private String deviceId;
    private String eventType;
    private double cpuUsagePercent;
    private double memoryUsagePercent;
    private double latencyMs;
    private double batteryLevelPercent;
    private Instant timestamp;
    private String status;
}
