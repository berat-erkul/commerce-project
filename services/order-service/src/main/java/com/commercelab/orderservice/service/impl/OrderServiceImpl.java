package com.commercelab.orderservice.service.impl;

import com.commercelab.orderservice.dto.CreateOrderRequest;
import com.commercelab.orderservice.dto.CreateOrderResponse;
import com.commercelab.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderFactory orderFactory;
    private final OrderPersister orderPersister;

    @Override
    public CreateOrderResponse createOrder(CreateOrderRequest request) {
        OrderFactory.OrderAggregate agg = orderFactory.build(request);
        orderPersister.persist(agg);
        return CreateOrderResponse.builder()
            .orderId(agg.order().getId())
            .status(agg.order().getStatus())
            .totalAmount(agg.order().getTotalAmount())
            .currency(agg.order().getCurrency())
            .build();
    }
}
