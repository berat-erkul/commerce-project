package com.commercelab.sagaservice.repo;

import com.commercelab.sagaservice.entity.SagaInstance;
import com.commercelab.sagaservice.entity.enums.SagaStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ISagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findBySagaTypeAndOrderId(String sagaType, UUID orderId);

    List<SagaInstance> findAllByStatusInAndNextRetryAtBefore(
            List<SagaStatus> statuses,
            OffsetDateTime threshold
    );

    /**
     * Pessimistic write lock — concurrent advance/fail race koruması.
     * Kafka rebalance veya duplicate redelivery sonrası iki thread paralel
     * advance yaparsa lost-update + double command emission engellenir.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select s from SagaInstance s where s.id = :id")
    Optional<SagaInstance> findByIdForUpdate(@Param("id") UUID id);
}
