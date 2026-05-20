package com.commercelab.paymentservice.service;

import com.commercelab.events.PaymentEvents;
import com.commercelab.paymentservice.entity.OutboxEvent;
import com.commercelab.paymentservice.entity.Payment;
import com.commercelab.paymentservice.repo.IOutboxEventRepository;
import com.commercelab.paymentservice.repo.IPaymentRepository;
import com.commercelab.paymentservice.repo.IProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * ProcessPayment komutunu işleyen business katmanı — Gün 1 STUB.
 * <ul>
 *   <li>Gün 1: gerçek Stripe YOK. Her ödeme anında başarılı (status=CAPTURED).</li>
 *   <li>Idempotency K1: processed_events ON CONFLICT DO NOTHING (Postgres aborted-tx trap'ten kaçınma).</li>
 *   <li>Idempotency K2: payments UNIQUE(saga_instance_id) — findBySagaInstanceId guard.</li>
 *   <li>Fail path yok (karar #1): STUB her zaman PaymentCompleted üretir.</li>
 * </ul>
 * Gün 2 (Task #8) gerçek Stripe Seçenek B (authorize+capture) buraya gelecek.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProcessingService {

    static final String CONSUMER_NAME = "payment-service-commands";
    static final String TOPIC_PAYMENT_COMMANDS = "payment.commands";
    static final String TOPIC_PAYMENT_EVENTS = "payment.events";

    private final IPaymentRepository paymentRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    public enum Result { COMPLETED, SKIPPED_DUPLICATE, SKIPPED_ALREADY_PAID }

    /**
     * Process flow — tek tx. Gün 1 STUB: her zaman başarılı.
     */
    @Transactional
    public Result process(PaymentEvents.ProcessPayment cmd) {
        OffsetDateTime now = OffsetDateTime.now();

        // K1 — processed_events guard. 0 = zaten işlenmiş, sessizce çık.
        int inserted = processedRepo.insertIfAbsent(
                cmd.commandId(), CONSUMER_NAME, TOPIC_PAYMENT_COMMANDS, now);
        if (inserted == 0) {
            log.info("K1 hit: commandId={} already processed, skipping", cmd.commandId());
            return Result.SKIPPED_DUPLICATE;
        }

        // K2 — bu saga için payment zaten var mı? Varsa re-emit yok (inventory kalıbı).
        Optional<Payment> existing = paymentRepo.findBySagaInstanceId(cmd.sagaInstanceId());
        if (existing.isPresent()) {
            log.warn("K2 hit: saga={} already has payment, no PaymentCompleted re-emitted",
                    cmd.sagaInstanceId());
            return Result.SKIPPED_ALREADY_PAID;
        }

        // STUB: anında başarılı ödeme. paymentId service-side üretilir (karar #3).
        UUID paymentId = UUID.randomUUID();
        Payment payment = Payment.builder()
                .id(paymentId)
                .orderId(cmd.orderId())
                .sagaInstanceId(cmd.sagaInstanceId())
                .amount(cmd.amount())
                .currency(cmd.currency())
                .stripePaymentIntentId(null)   // Gün 2 gerçek Stripe doldurur
                .status("CAPTURED")            // Seçenek B: authorize+capture beraber
                .createdAt(now)
                .capturedAt(now)
                .build();
        paymentRepo.save(payment);

        PaymentEvents.PaymentCompleted event = new PaymentEvents.PaymentCompleted(
                UUID.randomUUID(),
                cmd.sagaInstanceId(),
                cmd.orderId(),
                paymentId,
                cmd.amount(),
                null,                          // stripePaymentIntentId — Gün 2
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                event.eventId(),
                "Payment",
                cmd.sagaInstanceId(),
                "PaymentCompleted",
                TOPIC_PAYMENT_EVENTS,
                event,
                cmd.sagaInstanceId()
        );
        outboxRepo.save(outbox);

        log.info("STUB payment captured paymentId={} saga={} amount={}",
                paymentId, cmd.sagaInstanceId(), cmd.amount());
        return Result.COMPLETED;
    }
}
