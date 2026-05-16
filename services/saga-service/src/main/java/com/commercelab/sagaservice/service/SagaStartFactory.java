package com.commercelab.sagaservice.service;

import com.commercelab.events.OrderEvents;
import com.commercelab.events.PaymentEvents;
import com.commercelab.sagaservice.dto.SagaPayload;
import com.commercelab.sagaservice.entity.OutboxEvent;
import com.commercelab.sagaservice.entity.SagaInstance;
import com.commercelab.sagaservice.entity.SagaStepHistory;
import com.commercelab.sagaservice.entity.enums.SagaStatus;
import com.commercelab.sagaservice.entity.enums.StepName;
import com.commercelab.sagaservice.entity.enums.StepStatus;
import com.commercelab.sagaservice.entity.enums.StepType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Saga başlatma için entity graph kurar. NO DB, NO @Transactional — pure CPU.
 * <p>
 * Tüm UUID üretimi, payload denormalizasyonu, Jackson serialize iş ı burada.
 * Persister bu aggregate'i alıp tek atomik blokta kaydeder.
 */
@Component
@RequiredArgsConstructor
class SagaStartFactory {

    static final String SAGA_TYPE = "ORDER_PLACEMENT";
    static final String TOPIC_PAYMENT_CMD = "payment.commands";

    private final OutboxFactory outboxFactory;

    record SagaStartAggregate(
            SagaInstance saga,
            SagaStepHistory step,
            OutboxEvent processPaymentOutbox
    ) {}

    SagaStartAggregate build(OrderEvents.OrderCreated event) {
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

        SagaStepHistory step = SagaStepHistory.builder()
                .id(UUID.randomUUID())
                .sagaInstanceId(sagaId)
                .stepName(StepName.PROCESS_PAYMENT)
                .stepType(StepType.FORWARD)
                .status(StepStatus.STARTED)
                .startedAt(now)
                .build();

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

        return new SagaStartAggregate(saga, step, outbox);
    }
}
