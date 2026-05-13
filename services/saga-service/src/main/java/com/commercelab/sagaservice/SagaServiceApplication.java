package com.commercelab.sagaservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Saga orchestrator.
 *
 * Sorumlulukları (ADR-001):
 *  - OrderCreated event'ini consume eder → saga_instance oluşturur
 *  - State machine: PENDING → AWAITING_PAYMENT → AWAITING_STOCK → COMPLETED
 *  - Sad path: STOCK_RESERVATION_FAILED → COMPENSATING → CANCELLED
 *  - 4 katman compensation fail handling (retry / DLQ / MANUAL_INTERVENTION / reconciliation)
 *
 * @EnableScheduling: saga-watcher + outbox poller scheduled job'ları için
 */
@SpringBootApplication
@EnableScheduling
public class SagaServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(SagaServiceApplication.class, args);
    }
}
