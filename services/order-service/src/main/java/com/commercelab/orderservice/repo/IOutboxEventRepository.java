package com.commercelab.orderservice.repo;

import com.commercelab.orderservice.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface IOutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    // SKIP LOCKED: çoklu poller instance'ı aynı satıra takılmasın, kilitli olanı atlayıp ilerlesin.
    // Composite index (status, created_at) bu sorgu için tasarlandı.
    @Query(
            value = "SELECT * FROM outbox_events " +
                    "WHERE status = 'PENDING' " +
                    "ORDER BY created_at ASC " +
                    "LIMIT 100 " +
                    "FOR UPDATE SKIP LOCKED",
            nativeQuery = true
    )
    List<OutboxEvent> selectTop100PublishedFalseEvent();


}
