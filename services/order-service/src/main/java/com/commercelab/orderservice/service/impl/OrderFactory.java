package com.commercelab.orderservice.service.impl;

import com.commercelab.events.OrderEvents;
import com.commercelab.events.OrderItem;
import com.commercelab.orderservice.dto.CreateOrderRequest;
import com.commercelab.orderservice.dto.OrderItemRequest;
import com.commercelab.orderservice.entity.Order;
import com.commercelab.orderservice.entity.OutboxEvent;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Component
@RequiredArgsConstructor
class OrderFactory {

    static final String ORDER_TOPIC = "order.events";
    static final String DEFAULT_CURRENCY = "USD";

    private final ObjectMapper objectMapper;

    record OrderAggregate(Order order, OutboxEvent outbox) {}

    OrderAggregate build(CreateOrderRequest request) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String currency = request.getCurrency() == null ? DEFAULT_CURRENCY : request.getCurrency();

        BigDecimal total = request.getItems().stream()
            .map(i -> i.getUnitPrice().multiply(BigDecimal.valueOf(i.getQuantity())))
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        Order order = Order.builder()
            .id(UUID.randomUUID())
            .customerId(request.getCustomerId())
            .status("PENDING")
            .totalAmount(total)
            .currency(currency)
            .createdAt(now)
            .updatedAt(now)
            .build();

        for (OrderItemRequest itemReq : request.getItems()) {
            order.addItem(com.commercelab.orderservice.entity.OrderItem.builder()
                .id(UUID.randomUUID())
                .productId(itemReq.getProductId())
                .quantity(itemReq.getQuantity())
                .unitPrice(itemReq.getUnitPrice())
                .build());
        }

        UUID eventId = UUID.randomUUID();
        List<OrderItem> eventItems = request.getItems().stream()
            .map(i -> new OrderItem(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
            .toList();
        OrderEvents.OrderCreated event = new OrderEvents.OrderCreated(
            eventId, order.getId(), order.getCustomerId(),
            total, currency, eventItems, Instant.now()
        );
        Map<String, Object> payload = objectMapper.convertValue(event, new TypeReference<>() {});

        OutboxEvent outbox = OutboxEvent.builder()
            .id(eventId)
            .aggregateType("Order")
            .aggregateId(order.getId())
            .eventType("OrderCreated")
            .topic(ORDER_TOPIC)
            .payload(payload)
            .status("PENDING")
            .createdAt(now)
            .retryCount(0)
            .build();

        return new OrderAggregate(order, outbox);
    }
}
