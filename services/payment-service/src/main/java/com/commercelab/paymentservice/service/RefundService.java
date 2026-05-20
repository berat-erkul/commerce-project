package com.commercelab.paymentservice.service;

import com.commercelab.events.PaymentEvents;
import com.commercelab.paymentservice.entity.OutboxEvent;
import com.commercelab.paymentservice.entity.Payment;
import com.commercelab.paymentservice.entity.Refund;
import com.commercelab.paymentservice.repo.IOutboxEventRepository;
import com.commercelab.paymentservice.repo.IPaymentRepository;
import com.commercelab.paymentservice.repo.IProcessedEventRepository;
import com.commercelab.paymentservice.repo.IRefundRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;

/**
 * RefundPayment komutunu işleyen compensation katmanı — STUB.
 * <ul>
 *   <li>Gerçek Stripe refund KASITLI atlandı — "iade alınmış gibi" işlenir.</li>
 *   <li>K1: processed_events ON CONFLICT (commandId).</li>
 *   <li>K2: refunds UNIQUE(payment_id) — findByPaymentId guard.</li>
 *   <li>Çıktı: PaymentRefunded → saga COMPENSATING'i CANCELLED'a düşürür.</li>
 * </ul>
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RefundService {

    static final String CONSUMER_NAME = "payment-service-commands";
    static final String TOPIC_PAYMENT_COMMANDS = "payment.commands";
    static final String TOPIC_PAYMENT_EVENTS = "payment.events";

    private final IPaymentRepository paymentRepo;
    private final IRefundRepository refundRepo;
    private final IOutboxEventRepository outboxRepo;
    private final IProcessedEventRepository processedRepo;
    private final OutboxFactory outboxFactory;

    public enum Result { REFUNDED, SKIPPED_DUPLICATE, SKIPPED_ALREADY_REFUNDED, SKIPPED_UNKNOWN_PAYMENT }

    @Transactional
    public Result refund(PaymentEvents.RefundPayment cmd) {
        OffsetDateTime now = OffsetDateTime.now();

        // K1 — processed_events guard.
        int inserted = processedRepo.insertIfAbsent(
                cmd.commandId(), CONSUMER_NAME, TOPIC_PAYMENT_COMMANDS, now);
        if (inserted == 0) {
            log.info("K1 hit: refund commandId={} already processed, skipping", cmd.commandId());
            return Result.SKIPPED_DUPLICATE;
        }

        // Payment gerçekten var mı?
        Optional<Payment> paymentOpt = paymentRepo.findById(cmd.paymentId());
        if (paymentOpt.isEmpty()) {
            log.warn("RefundPayment for unknown paymentId={}, saga={}", cmd.paymentId(), cmd.sagaInstanceId());
            return Result.SKIPPED_UNKNOWN_PAYMENT;
        }

        // K2 — bu payment için refund zaten var mı?
        if (refundRepo.findByPaymentId(cmd.paymentId()).isPresent()) {
            log.warn("K2 hit: payment={} already refunded, no PaymentRefunded re-emitted", cmd.paymentId());
            return Result.SKIPPED_ALREADY_REFUNDED;
        }

        // STUB iade: gerçek Stripe çağrısı yok, "iade alınmış gibi" COMPLETED.
        Refund refund = Refund.builder()
                .id(cmd.refundId())            // saga-side idempotency key = PK
                .paymentId(cmd.paymentId())
                .refundIdExternal(null)        // re_xxx — gerçek Stripe ayrı haftada
                .amount(cmd.amount())
                .status("COMPLETED")
                .createdAt(now)
                .completedAt(now)
                .build();
        refundRepo.save(refund);

        PaymentEvents.PaymentRefunded event = new PaymentEvents.PaymentRefunded(
                UUID.randomUUID(),
                cmd.sagaInstanceId(),
                cmd.paymentId(),
                cmd.refundId(),
                cmd.amount(),
                Instant.now()
        );
        OutboxEvent outbox = outboxFactory.build(
                event.eventId(),
                "Payment",
                cmd.sagaInstanceId(),
                "PaymentRefunded",
                TOPIC_PAYMENT_EVENTS,
                event,
                cmd.sagaInstanceId()
        );
        outboxRepo.save(outbox);

        log.info("STUB refund completed refundId={} payment={} saga={}",
                cmd.refundId(), cmd.paymentId(), cmd.sagaInstanceId());
        return Result.REFUNDED;
    }
}
