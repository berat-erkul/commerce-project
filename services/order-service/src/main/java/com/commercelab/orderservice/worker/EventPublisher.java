package com.commercelab.orderservice.worker;

import com.commercelab.orderservice.entity.OutboxEvent;
import com.commercelab.orderservice.repo.IOutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 5L;

    private final IOutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 1000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> batch = outboxRepo.selectTop100PublishedFalseEvent();
        if (batch.isEmpty()) {
            return;
        }

        for (OutboxEvent row : batch) {
            try {
                String payloadJson = objectMapper.writeValueAsString(row.getPayload());
                CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(
                        row.getTopic(),
                        row.getAggregateId().toString(),
                        payloadJson
                );
                future.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                row.setStatus("PUBLISHED");
                row.setPublishedAt(OffsetDateTime.now());
            } catch (Exception ex) {
                row.setRetryCount(row.getRetryCount() + 1);
                row.setLastError(truncate(ex.getMessage(), 1000));
                log.warn("Outbox publish failed id={} retry={} err={}",
                        row.getId(), row.getRetryCount(), ex.getMessage());
            }
        }
    }

    private static String truncate(String s, int max) {
        if (s == null) return null;
        return s.length() <= max ? s : s.substring(0, max);
    }
}
