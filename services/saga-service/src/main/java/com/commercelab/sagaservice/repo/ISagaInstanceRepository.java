package com.commercelab.sagaservice.repo;

import com.commercelab.sagaservice.entity.SagaInstance;
import com.commercelab.sagaservice.entity.enums.SagaStatus;
import org.springframework.data.jpa.repository.JpaRepository;

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
}
