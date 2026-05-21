package com.commercelab.paymentservice.repo;

import com.commercelab.paymentservice.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface IOutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /**
     * Multi-instance claim: PENDING satırları kilitle, kilitliyi atla (SKIP LOCKED).
     * Çağıran tx içinde bu satırları IN_FLIGHT yapar → başka poller PENDING sorgusunda görmez.
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT :limit
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> lockPendingBatch(@Param("limit") int limit);

    /**
     * Crash recovery: claim'leyip publish edemeden ölen poller'ın IN_FLIGHT satırlarını
     * PENDING'e geri al (threshold'dan eski claimed_at).
     */
    @Modifying
    @Query(value = """
            UPDATE outbox_events
            SET status = 'PENDING', claimed_at = NULL
            WHERE status = 'IN_FLIGHT' AND claimed_at < :threshold
            """, nativeQuery = true)
    int reclaimStale(@Param("threshold") OffsetDateTime threshold);
}
