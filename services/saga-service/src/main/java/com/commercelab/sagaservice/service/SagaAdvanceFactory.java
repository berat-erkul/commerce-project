package com.commercelab.sagaservice.service;

import com.commercelab.events.InventoryEvents;
import com.commercelab.events.OrderItem;
import com.commercelab.sagaservice.entity.OutboxEvent;
import com.commercelab.sagaservice.entity.SagaInstance;
import com.commercelab.sagaservice.entity.SagaStepHistory;
import com.commercelab.sagaservice.entity.enums.StepName;
import com.commercelab.sagaservice.entity.enums.StepStatus;
import com.commercelab.sagaservice.entity.enums.StepType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Saga forward-advance için entity graph kurar. NO DB, NO @Transactional — pure CPU.
 * Persister mevcut SagaInstance'ı tx içinde okuyup buraya iletir.
 */
@Component
@RequiredArgsConstructor
class SagaAdvanceFactory {

    static final String TOPIC_INVENTORY_CMD = "inventory.commands";

    private final OutboxFactory outboxFactory;

    record AdvanceAggregate(
            SagaStepHistory newStep,
            OutboxEvent commandOutbox
    ) {}

    /**
     * PaymentCompleted → ReserveStock advance.
     * Saga payload'ından items okunup ReserveStock command'ı kurulur.
     */
    AdvanceAggregate buildAdvanceToReserveStock(SagaInstance saga) {
        OffsetDateTime now = OffsetDateTime.now();

        SagaStepHistory newStep = SagaStepHistory.builder()
                .id(UUID.randomUUID())
                .sagaInstanceId(saga.getId())
                .stepName(StepName.RESERVE_STOCK)
                .stepType(StepType.FORWARD)
                .status(StepStatus.STARTED)
                .startedAt(now)
                .build();

        UUID commandId = UUID.randomUUID();
        InventoryEvents.ReserveStock cmd = new InventoryEvents.ReserveStock(
                commandId,
                saga.getId(),
                saga.getOrderId(),
                saga.getPayload().getItems().stream()
                        .map(i -> new OrderItem(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                        .toList(),
                Instant.now()
        );

        OutboxEvent outbox = outboxFactory.build(
                commandId,
                "Saga",
                saga.getId(),
                "ReserveStock",
                TOPIC_INVENTORY_CMD,
                cmd,
                saga.getId()
        );

        return new AdvanceAggregate(newStep, outbox);
    }
}
