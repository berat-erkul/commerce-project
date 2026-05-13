package com.commercelab.orderservice.controller;

import com.commercelab.orderservice.dto.CreateOrderRequest;
import com.commercelab.orderservice.dto.CreateOrderResponse;
import com.commercelab.orderservice.service.OrderService;
import com.commercelab.orderservice.wrapper.ResponseWrapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @PostMapping
    public ResponseEntity<ResponseWrapper<CreateOrderResponse>> create(@Valid @RequestBody CreateOrderRequest request) {
        CreateOrderResponse response = orderService.createOrder(request);
        return ResponseEntity
            .status(HttpStatus.CREATED)
            .body(ResponseWrapper.<CreateOrderResponse>builder()
                .success(true)
                .message("Order created")
                .data(response)
                .build());
    }
}
