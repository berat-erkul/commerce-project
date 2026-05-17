package com.commercelab.sagaservice.repo;

import com.commercelab.sagaservice.entity.SagaStepHistory;
import com.commercelab.sagaservice.entity.enums.StepName;
import com.commercelab.sagaservice.entity.enums.StepStatus;
import com.commercelab.sagaservice.entity.enums.StepType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface ISagaStepHistoryRepository extends JpaRepository<SagaStepHistory, UUID> {

    List<SagaStepHistory> findAllBySagaInstanceIdOrderByStartedAtAsc(UUID sagaInstanceId);

    /**
     * LIFO compensation için: tamamlanmış FORWARD step'leri ters sırada getir.
     */
    List<SagaStepHistory> findAllBySagaInstanceIdAndStepTypeAndStatusOrderByStartedAtDesc(
            UUID sagaInstanceId,
            StepType stepType,
            StepStatus status
    );

    /**
     * Advance/fail persister'lar bu saga için aktif (STARTED) forward step'i bulur,
     * COMPLETED veya FAILED işaretlemek için.
     */
    Optional<SagaStepHistory> findFirstBySagaInstanceIdAndStepNameAndStepTypeAndStatusOrderByStartedAtDesc(
            UUID sagaInstanceId,
            StepName stepName,
            StepType stepType,
            StepStatus status
    );
}
