package com.commercelab.inventoryservice.repo;

import com.commercelab.inventoryservice.entity.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.UUID;

public interface IProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {

    /**
     * Postgres-native idempotent INSERT — UNIQUE çakışmasında transaction
     * 'aborted' state'ine düşmez. JPA save() bu garantiyi vermez.
     * [[feedback_postgres_aborted_tx]]
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
