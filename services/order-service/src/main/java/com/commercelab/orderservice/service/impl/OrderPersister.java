package com.commercelab.orderservice.service.impl;

import com.commercelab.orderservice.repo.OrderRepository;
import com.commercelab.orderservice.repo.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
class OrderPersister {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;

    @Transactional
    public void persist(OrderFactory.OrderAggregate agg) {
        orderRepository.save(agg.order());
        outboxRepository.save(agg.outbox());
    }
}
