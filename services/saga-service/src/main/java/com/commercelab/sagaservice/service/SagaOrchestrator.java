package com.commercelab.sagaservice.service;

import com.commercelab.events.OrderEvents;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Saga state machine'in giriş noktası. NO @Transactional — koordinasyon katmanı.
 * <p>
 * Hata-translasyon burada: Postgres UNIQUE race'i (DataIntegrityViolation)
 * persister'dan dışarı çıkar, burada SKIPPED_RACE log'una çevrilir.
 * Persister içinde catch yok → temiz @Transactional rollback garantisi.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final SagaStartFactory startFactory;
    private final SagaStartPersister startPersister;

    public void startOrderPlacementSaga(OrderEvents.OrderCreated event) {
        SagaStartFactory.SagaStartAggregate agg = startFactory.build(event);

        try {
            SagaStartPersister.Result result = startPersister.persist(event.eventId(), agg);
            switch (result) {
                case CREATED -> log.info("Saga started sagaId={} orderId={} → ProcessPayment queued",
                        agg.saga().getId(), event.orderId());
                case SKIPPED_DUPLICATE_EVENT -> log.info("OrderCreated already processed eventId={}", event.eventId());
                case SKIPPED_EXISTING_SAGA -> log.info("Saga already exists for orderId={}, skipping", event.orderId());
            }
        } catch (DataIntegrityViolationException ex) {
            // K2 race: paralel thread saga'yı yarattı. Persister @Transactional temiz rollback etti.
            log.info("Race lost on saga creation orderId={}, other thread won", event.orderId());
        }
    }
}
