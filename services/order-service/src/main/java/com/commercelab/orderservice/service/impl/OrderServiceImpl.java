package com.commercelab.orderservice.service.impl;

import com.commercelab.events.OrderEvents;
import com.commercelab.events.OrderItem;
import com.commercelab.orderservice.dto.CreateOrderRequest;
import com.commercelab.orderservice.dto.CreateOrderResponse;
import com.commercelab.orderservice.dto.OrderItemRequest;
import com.commercelab.orderservice.entity.Order;
import com.commercelab.orderservice.entity.OutboxEvent;
import com.commercelab.orderservice.repo.OrderRepository;
import com.commercelab.orderservice.repo.OutboxEventRepository;
import com.commercelab.orderservice.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private static final String ORDER_TOPIC = "order.events";
    private static final String DEFAULT_CURRENCY = "USD";

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Override
    @Transactional
    public CreateOrderResponse createOrder(CreateOrderRequest request) {
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

        orderRepository.save(order);

        List<OrderItem> eventItems = request.getItems().stream()
            .map(i -> new OrderItem(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
            .toList();

        UUID eventId = UUID.randomUUID();
        OrderEvents.OrderCreated event = new OrderEvents.OrderCreated(
            eventId, order.getId(), order.getCustomerId(),
            total, currency, eventItems, Instant.now()
        );

        Map<String, Object> payload = objectMapper.convertValue(event, new com.fasterxml.jackson.core.type.TypeReference<>() {});

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

        outboxRepository.save(outbox);

        return CreateOrderResponse.builder()
            .orderId(order.getId())
            .status(order.getStatus())
            .totalAmount(total)
            .currency(currency)
            .build();
    }
}
