package com.commercelab.orderservice.service.impl;

import com.commercelab.orderservice.repo.IOrderRepository;
import com.commercelab.orderservice.repo.IOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@RequiredArgsConstructor
class OrderPersister {

    private final IOrderRepository orderRepository;
    private final IOutboxEventRepository outboxRepository;

    @Transactional
    public void persist(OrderFactory.OrderAggregate agg) {
        orderRepository.save(agg.order());
        outboxRepository.save(agg.outbox());
    }
}
