package com.commercelab.orderservice.worker;

import com.commercelab.orderservice.entity.OutboxEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * order-service outbox poller — claim-then-send.
 * <p>
 * 1. store.claimBatch() KISA tx: SKIP LOCKED ile PENDING → IN_FLIGHT (multi-instance güvenli).
 * 2. tx DIŞINDA Kafka'ya bas (connection I/O boyunca tutulmaz).
 * 3. her satırı ayrı mini-tx'te işaretle (batch ortası crash'te toplu duplicate yok).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final OutboxStore store;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 1000)
    public void publishPendingEvents() {
        List<OutboxEvent> batch = store.claimBatch();
        for (OutboxEvent row : batch) {
            try {
                String payloadJson = objectMapper.writeValueAsString(row.getPayload());
                kafkaTemplate.send(row.getTopic(), row.getAggregateId().toString(), payloadJson)
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                store.markPublished(row.getId());
            } catch (Exception ex) {
                log.warn("Outbox publish failed id={} err={}", row.getId(), ex.getMessage());
                store.markFailed(row.getId(), truncate(ex.getMessage(), 1000));
            }
        }
    }

    @Scheduled(fixedDelay = 30000)
    public void reclaimStaleInFlight() {
        store.reclaimStale();
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
