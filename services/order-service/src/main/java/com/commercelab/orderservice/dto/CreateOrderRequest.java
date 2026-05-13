package com.commercelab.orderservice.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.*;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {

    @NotNull
    private UUID customerId;

    private String currency;

    @NotEmpty
    @Size(max = 100)
    @Valid
    private List<OrderItemRequest> items;
}
