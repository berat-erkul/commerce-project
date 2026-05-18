package com.commercelab.sagaservice.service;

import com.commercelab.events.InventoryEvents;
import com.commercelab.events.OrderEvents;
import com.commercelab.events.PaymentEvents;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Saga state machine giriş noktası. NO @Transactional — koordinasyon katmanı.
 * Heavy work factory'lerde (CPU) ve persister'larda (DB).
 * Hata translasyonu burada: Postgres UNIQUE race'leri persister'lardan dışarı çıkar,
 * burada anlamlı log'a çevrilir.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final SagaStartFactory startFactory;
    private final SagaStartPersister startPersister;
    private final SagaAdvancePersister advancePersister;
    private final SagaFailPersister failPersister;
    private final SagaCompletePersister completePersister;
    private final SagaCompensatePersister compensatePersister;

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
            log.info("Race lost on saga creation orderId={}, other thread won", event.orderId());
        }
    }

    public void onPaymentCompleted(PaymentEvents.PaymentCompleted event) {
        try {
            SagaAdvancePersister.Result result = advancePersister.persistAdvanceToReserveStock(event.eventId(), event);
            switch (result) {
                case ADVANCED -> log.info("Saga {} advanced AWAITING_PAYMENT → AWAITING_STOCK, ReserveStock queued",
                        event.sagaInstanceId());
                case SKIPPED_DUPLICATE_EVENT -> log.info("PaymentCompleted duplicate eventId={}", event.eventId());
                case SKIPPED_UNKNOWN_SAGA -> log.warn("PaymentCompleted for unknown saga={}", event.sagaInstanceId());
                case SKIPPED_INVALID_STATE -> log.info("Saga {} not in AWAITING_PAYMENT, skipped", event.sagaInstanceId());
                case SKIPPED_NO_ACTIVE_STEP -> log.warn("No active PROCESS_PAYMENT step for saga {}", event.sagaInstanceId());
            }
        } catch (DataIntegrityViolationException ex) {
            log.info("Race lost on saga advance saga={}", event.sagaInstanceId());
        }
    }

    public void onPaymentFailed(PaymentEvents.PaymentFailed event) {
        try {
            SagaFailPersister.Result result = failPersister.failOnPaymentFailed(event.eventId(), event);
            switch (result) {
                case TERMINATED_FAILED -> log.info("Saga {} TERMINATED FAILED (payment), OrderCancelled queued",
                        event.sagaInstanceId());
                case SKIPPED_DUPLICATE_EVENT -> log.info("PaymentFailed duplicate eventId={}", event.eventId());
                case SKIPPED_UNKNOWN_SAGA -> log.warn("PaymentFailed for unknown saga={}", event.sagaInstanceId());
                case SKIPPED_INVALID_STATE -> log.info("Saga {} not in AWAITING_PAYMENT for fail, skipped", event.sagaInstanceId());
            }
        } catch (DataIntegrityViolationException ex) {
            log.info("Race lost on saga fail saga={}", event.sagaInstanceId());
        }
    }

    public void onStockReserved(InventoryEvents.StockReserved event) {
        try {
            SagaCompletePersister.Result result = completePersister.completeOnStockReserved(event.eventId(), event);
            switch (result) {
                case COMPLETED -> log.info("Saga {} COMPLETED (stock reserved), OrderCompleted queued",
                        event.sagaInstanceId());
                case SKIPPED_DUPLICATE_EVENT -> log.info("StockReserved duplicate eventId={}", event.eventId());
                case SKIPPED_UNKNOWN_SAGA -> log.warn("StockReserved for unknown saga={}", event.sagaInstanceId());
                case SKIPPED_INVALID_STATE -> log.info("Saga {} not in AWAITING_STOCK, skipped", event.sagaInstanceId());
                case SKIPPED_NO_ACTIVE_STEP -> log.warn("No active RESERVE_STOCK step for saga {}", event.sagaInstanceId());
            }
        } catch (DataIntegrityViolationException ex) {
            log.info("Race lost on saga complete saga={}", event.sagaInstanceId());
        }
    }

    public void onStockReservationFailed(InventoryEvents.StockReservationFailed event) {
        try {
            SagaCompensatePersister.Result result = compensatePersister.compensateOnStockReservationFailed(event.eventId(), event);
            switch (result) {
                case COMPENSATION_STARTED -> log.info("Saga {} COMPENSATING (stock failed), RefundPayment queued",
                        event.sagaInstanceId());
                case SKIPPED_DUPLICATE_EVENT -> log.info("StockReservationFailed duplicate eventId={}", event.eventId());
                case SKIPPED_UNKNOWN_SAGA -> log.warn("StockReservationFailed for unknown saga={}", event.sagaInstanceId());
                case SKIPPED_INVALID_STATE -> log.info("Saga {} not in AWAITING_STOCK for compensation, skipped", event.sagaInstanceId());
                case SKIPPED_MISSING_PAYMENT_ID -> log.error("Saga {} MANUAL_INTERVENTION — paymentId missing, manual refund required",
                        event.sagaInstanceId());
            }
        } catch (DataIntegrityViolationException ex) {
            log.info("Race lost on saga compensate saga={}", event.sagaInstanceId());
        }
    }
}
