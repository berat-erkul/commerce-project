package com.commercelab.sagaservice.service;

import com.commercelab.events.OrderEvents;
import com.commercelab.events.PaymentEvents;
import com.commercelab.sagaservice.dto.SagaPayload;
import com.commercelab.sagaservice.entity.*;
import com.commercelab.sagaservice.entity.enums.SagaStatus;
import com.commercelab.sagaservice.entity.enums.StepName;
import com.commercelab.sagaservice.entity.enums.StepStatus;
import com.commercelab.sagaservice.entity.enums.StepType;
import com.commercelab.sagaservice.repo.IOutboxEventRepository;
import com.commercelab.sagaservice.repo.IProcessedEventRepository;
import com.commercelab.sagaservice.repo.ISagaInstanceRepository;
import com.commercelab.sagaservice.repo.ISagaStepHistoryRepository;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Saga state machine'in single source of truth'u.
 * <p>
 * Tüm metodlar @Transactional — saga state transition + outbox INSERT atomik.
 * Listener'lar sadece JSON parse + delegation yapar; iş kararları burada.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private static final String SAGA_TYPE = "ORDER_PLACEMENT";
    private static final String CONSUMER_ORDER = "saga-service-order";

    private static final String TOPIC_PAYMENT_CMD = "payment.commands";

    private final ISagaInstanceRepository sagaRepo;
    private final ISagaStepHistoryRepository stepRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;
    private final EntityManager entityManager;

    /**
     * `order.events.OrderCreated` consume edildiğinde çağrılır.
     * <p>
     * İdempotency 2 katman:
     *   K1 — processed_events INSERT (UNIQUE eventId)
     *   K2 — saga_instances UNIQUE (saga_type, order_id)
     * <p>
     * Yan etki: ProcessPayment command'ı outbox'a yazılır → payment.commands.
     */
    @Transactional
    public void startOrderPlacementSaga(OrderEvents.OrderCreated event) {
        // K1 — event-level idempotency
        if (alreadyProcessed(event.eventId(), CONSUMER_ORDER, "order.events")) {
            log.info("OrderCreated already processed eventId={}", event.eventId());
            return;
        }

        // K2 — saga-level domain idempotency (uq_saga_order)
        if (sagaRepo.findBySagaTypeAndOrderId(SAGA_TYPE, event.orderId()).isPresent()) {
            log.info("Saga already exists for orderId={}, skipping", event.orderId());
            recordProcessed(event.eventId(), CONSUMER_ORDER, "order.events");
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();
        UUID sagaId = UUID.randomUUID();

        SagaPayload payload = SagaPayload.builder()
                .orderId(event.orderId())
                .customerId(event.customerId())
                .amount(event.totalAmount())
                .currency(event.currency())
                .items(event.items().stream()
                        .map(i -> SagaPayload.OrderItemSnapshot.builder()
                                .productId(i.productId())
                                .quantity(i.quantity())
                                .unitPrice(i.unitPrice())
                                .build())
                        .toList())
                .build();

        SagaInstance saga = SagaInstance.builder()
                .id(sagaId)
                .sagaType(SAGA_TYPE)
                .orderId(event.orderId())
                .status(SagaStatus.AWAITING_PAYMENT)
                .currentStep(StepName.PROCESS_PAYMENT)
                .payload(payload)
                .startedAt(now)
                .updatedAt(now)
                .retryCount(0)
                .build();

        try {
            sagaRepo.saveAndFlush(saga);
        } catch (DataIntegrityViolationException ex) {
            // Race: aynı OrderCreated'i iki listener thread'i paralel işledi.
            // K2 UNIQUE patladı → diğer thread saga'yı yarattı, biz sessizce çık.
            log.info("Race lost on saga creation orderId={}, other thread won", event.orderId());
            return;
        }

        UUID stepId = UUID.randomUUID();
        SagaStepHistory step = SagaStepHistory.builder()
                .id(stepId)
                .sagaInstanceId(sagaId)
                .stepName(StepName.PROCESS_PAYMENT)
                .stepType(StepType.FORWARD)
                .status(StepStatus.STARTED)
                .startedAt(now)
                .build();
        stepRepo.save(step);

        UUID commandId = UUID.randomUUID();
        PaymentEvents.ProcessPayment cmd = new PaymentEvents.ProcessPayment(
                commandId,
                sagaId,
                event.orderId(),
                event.totalAmount(),
                event.currency(),
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                commandId,
                "Saga",
                sagaId,
                "ProcessPayment",
                TOPIC_PAYMENT_CMD,
                cmd,
                sagaId
        );
        outboxRepo.save(outbox);

        recordProcessed(event.eventId(), CONSUMER_ORDER, "order.events");

        log.info("Saga started sagaId={} orderId={} → ProcessPayment queued", sagaId, event.orderId());
    }

    private boolean alreadyProcessed(UUID eventId, String consumerName, String topic) {
        return processedRepo.findById(eventId).isPresent();
    }

    private void recordProcessed(UUID eventId, String consumerName, String topic) {
        ProcessedEvent pe = ProcessedEvent.builder()
                .eventId(eventId)
                .consumerName(consumerName)
                .topic(topic)
                .processedAt(OffsetDateTime.now())
                .build();
        try {
            processedRepo.saveAndFlush(pe);
        } catch (DataIntegrityViolationException ignored) {
            // Race: paralel thread aynı event'i işliyor olabilir; OK.
        }
    }
}
