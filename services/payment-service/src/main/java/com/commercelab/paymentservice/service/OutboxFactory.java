package com.commercelab.paymentservice.service;

import com.commercelab.paymentservice.entity.OutboxEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * Typed event record'larını OutboxEvent satırına çevirir.
 * Header standardı: saga-instance-id + event-type (saga/inventory ile aynı).
 */
@Component
@RequiredArgsConstructor
public class OutboxFactory {

    private final ObjectMapper objectMapper;

    public OutboxEvent build(
            UUID rowId,
            String aggregateType,
            UUID aggregateId,
            String eventType,
            String topic,
            Object payloadRecord,
            UUID sagaInstanceId
    ) {
        Map<String, Object> payload = objectMapper.convertValue(payloadRecord, new TypeReference<>() {});
        Map<String, Object> headers = Map.of(
                "saga-instance-id", sagaInstanceId.toString(),
                "event-type", eventType
        );

        return OutboxEvent.builder()
                .id(rowId)
                .aggregateType(aggregateType)
                .aggregateId(aggregateId)
                .eventType(eventType)
                .topic(topic)
                .payload(payload)
                .headers(headers)
                .status("PENDING")
                .createdAt(OffsetDateTime.now())
                .retryCount(0)
                .build();
    }
}
