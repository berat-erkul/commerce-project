package com.commercelab.paymentservice.repo;

import com.commercelab.paymentservice.entity.OutboxEvent;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

public interface IOutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("select o from OutboxEvent o where o.status = 'PENDING' order by o.createdAt asc")
    List<OutboxEvent> findPendingBatch(Pageable pageable);
}
