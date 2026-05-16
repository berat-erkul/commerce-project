package com.commercelab.sagaservice.service;

import com.commercelab.sagaservice.repo.IOutboxEventRepository;
import com.commercelab.sagaservice.repo.IProcessedEventRepository;
import com.commercelab.sagaservice.repo.ISagaInstanceRepository;
import com.commercelab.sagaservice.repo.ISagaStepHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Saga başlatma için tek atomik DB bloğu.
 * <p>
 * Postgres notu: try/catch içinde DataIntegrityViolationException YUTMUYORUZ.
 * Postgres'te SQL hatası transaction'ı 'E' (aborted) state'ine alır — hata
 * yutulsa bile transaction'ın geri kalanı çalışmaz. Bu yüzden:
 *   - processed_events için ON CONFLICT DO NOTHING (Postgres-native idempotent INSERT)
 *   - saga_instances UNIQUE race için exception YUKARI propagate; orchestrator
 *     yakalayıp Result.SKIPPED_RACE'e çevirir → temiz rollback.
 */
@Slf4j
@Component
@RequiredArgsConstructor
class SagaStartPersister {

    static final String CONSUMER_NAME = "saga-service-order";
    static final String TOPIC_ORDER_EVENTS = "order.events";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;

    enum Result { CREATED, SKIPPED_DUPLICATE_EVENT, SKIPPED_EXISTING_SAGA }

    @Transactional
    Result persist(UUID eventId, SagaStartFactory.SagaStartAggregate agg) {
        // K1
        if (processedRepo.findById(eventId).isPresent()) {
            return Result.SKIPPED_DUPLICATE_EVENT;
        }

        // K2 fast-path
        if (sagaRepo.findBySagaTypeAndOrderId(SagaStartFactory.SAGA_TYPE, agg.saga().getOrderId()).isPresent()) {
            recordProcessed(eventId);
            return Result.SKIPPED_EXISTING_SAGA;
        }

        // K2 authoritative — race olursa DataIntegrityViolation YUKARI çıkar.
        // Spring temiz rollback yapar, orchestrator SKIPPED_RACE'e çevirir.
        sagaRepo.save(agg.saga());
        stepRepo.save(agg.step());
        outboxRepo.save(agg.processPaymentOutbox());
        recordProcessed(eventId);
        return Result.CREATED;
    }

    private void recordProcessed(UUID eventId) {
        // Postgres ON CONFLICT DO NOTHING — exception fırlatmaz, transaction 'E' olmaz.
        processedRepo.insertIfAbsent(eventId, CONSUMER_NAME, TOPIC_ORDER_EVENTS, OffsetDateTime.now());
    }
}
