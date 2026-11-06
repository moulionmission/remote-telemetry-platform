package com.moulionmission.telemetry.repository;

import com.moulionmission.telemetry.model.TelemetryEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TelemetryRepository extends JpaRepository<TelemetryEvent, String> {
    List<TelemetryEvent> findByDeviceIdOrderByTimestampDesc(String deviceId);
    Optional<TelemetryEvent> findTopByDeviceIdOrderByTimestampDesc(String deviceId);
}
