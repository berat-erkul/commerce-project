package com.commercelab.sagaservice.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Saga'nın yaşam döngüsü boyunca biriken durum verisi.
 * JSONB olarak saga_instances.payload'a serialize edilir.
 * Forward adımlar ilerledikçe alanlar dolar (paymentIntentId, reservationId).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SagaPayload {

    private UUID orderId;
    private UUID customerId;
    private BigDecimal amount;
    private String currency;

    @Builder.Default
    private List<OrderItemSnapshot> items = new ArrayList<>();

    private String paymentIntentId;
    private UUID reservationId;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class OrderItemSnapshot {
        private UUID productId;
        private Integer quantity;
        private BigDecimal unitPrice;
    }
}
