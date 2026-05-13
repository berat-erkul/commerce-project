package com.commercelab.orderservice.dto;

import lombok.*;

import java.math.BigDecimal;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CreateOrderResponse {
    private UUID orderId;
    private String status;
    private BigDecimal totalAmount;
    private String currency;
}
