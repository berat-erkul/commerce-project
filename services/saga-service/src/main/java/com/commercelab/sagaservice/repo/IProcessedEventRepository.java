package com.commercelab.sagaservice.repo;

import com.commercelab.sagaservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.UUID;

public interface IProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * Postgres-native idempotent INSERT. Race veya retry'da exception fırlatmaz —
     * UNIQUE çakışması olursa hiçbir şey yapmaz, transaction 'E' state'ine girmez.
     * <p>
     * JPA save() / saveAndFlush() bu garantiyi vermez: çakışma DataIntegrityViolation
     * fırlatır, Postgres connection 'E' (aborted) olur, transaction'ın geri kalanı
     * yapılamaz. ON CONFLICT DO NOTHING bu Postgres-spesifik tuzağı bypass eder.
     */
    @Modifying
    @Query(value = """
            INSERT INTO processed_events (event_id, consumer_name, topic, processed_at)
            VALUES (:eventId, :consumerName, :topic, :processedAt)
            ON CONFLICT (event_id) DO NOTHING
            """, nativeQuery = true)
    int insertIfAbsent(
            @Param("eventId") UUID eventId,
            @Param("consumerName") String consumerName,
            @Param("topic") String topic,
            @Param("processedAt") OffsetDateTime processedAt
    );
}
