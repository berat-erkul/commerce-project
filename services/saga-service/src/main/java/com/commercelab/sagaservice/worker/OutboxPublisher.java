package com.commercelab.sagaservice.worker;

import com.commercelab.sagaservice.entity.OutboxEvent;
import com.commercelab.sagaservice.repo.IOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Saga-service'in transactional outbox poller'ı.
 * <p>
 * PENDING satırları batch'le çeker, Kafka'ya basar, status=PUBLISHED yapar.
 * Başarısızlık olursa retry_count++ ve PENDING kalır — sonraki tick yeniden dener.
 * <p>
 * Order-service'ten fark: headers JSONB kolonu desteklenir → Kafka header olarak basılır
 * (saga_instance_id correlation id altyapısı için).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final IOutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${outbox.batch-size:100}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> batch = outboxRepo.findPendingBatch(PageRequest.of(0, batchSize));
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent row : batch) {
            try {
                String payloadJson = objectMapper.writeValueAsString(row.getPayload());

                ProducerRecord<String, String> record = new ProducerRecord<>(
                        row.getTopic(),
                        null,
                        row.getAggregateId().toString(),
                        payloadJson
                );
                attachHeaders(record, row.getHeaders());

                CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
                future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                row.setStatus("PUBLISHED");
                row.setPublishedAt(OffsetDateTime.now());
            } catch (Exception ex) {
                row.setRetryCount(row.getRetryCount() + 1);
                row.setLastError(truncate(ex.getMessage(), 1000));
                log.warn("Outbox publish failed id={} topic={} retry={} err={}",
                        row.getId(), row.getTopic(), row.getRetryCount(), ex.getMessage());
            }
        }
    }

    private static void attachHeaders(ProducerRecord<String, String> record, Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return;
        }
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
