package com.commercelab.sagaservice.entity.enums;

public enum SagaStatus {
    PENDING,
    AWAITING_PAYMENT,
    AWAITING_STOCK,
    AWAITING_CAPTURE,
    COMPLETED,
    FAILED,
    COMPENSATING,
    CANCELLED,
    MANUAL_INTERVENTION
}
