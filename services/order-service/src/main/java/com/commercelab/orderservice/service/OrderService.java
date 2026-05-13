package com.commercelab.orderservice.service;

import com.commercelab.orderservice.dto.CreateOrderRequest;
import com.commercelab.orderservice.dto.CreateOrderResponse;

public interface OrderService {
    CreateOrderResponse createOrder(CreateOrderRequest request);
}
