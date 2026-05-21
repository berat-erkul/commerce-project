package com.commercelab.paymentservice.worker;

import com.commercelab.paymentservice.entity.OutboxEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * payment-service outbox poller — claim-then-send.
 * <p>
 * 1. store.claimBatch() KISA tx: SKIP LOCKED ile PENDING → IN_FLIGHT (multi-instance güvenli).
 * 2. tx DIŞINDA Kafka'ya bas (connection I/O boyunca tutulmaz).
 * 3. her satırı ayrı mini-tx'te işaretle (batch ortası crash'te toplu duplicate yok).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final OutboxStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    public void publishPendingEvents() {
        List<OutboxEvent> batch = store.claimBatch();
        for (OutboxEvent row : batch) {
            try {
                String payloadJson = objectMapper.writeValueAsString(row.getPayload());
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        row.getTopic(), null, row.getAggregateId().toString(), payloadJson);
                attachHeaders(record, row.getHeaders());
                kafkaTemplate.send(record).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                store.markPublished(row.getId());
            } catch (Exception ex) {
                log.warn("Outbox publish failed id={} topic={} err={}",
                        row.getId(), row.getTopic(), ex.getMessage());
                store.markFailed(row.getId(), truncate(ex.getMessage(), 1000));
            }
        }
    }

    @Scheduled(fixedDelayString = "${outbox.reclaim-interval-ms:30000}")
    public void reclaimStaleInFlight() {
        store.reclaimStale();
    }

    private static void attachHeaders(ProducerRecord<String, String> record, Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) return;
        headers.forEach((k, v) -> {
            if (v == null) return;
            record.headers().add(k, v.toString().getBytes(StandardCharsets.UTF_8));
        });
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
