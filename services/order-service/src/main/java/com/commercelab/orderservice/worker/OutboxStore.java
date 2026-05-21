package com.commercelab.orderservice.worker;

import com.commercelab.orderservice.entity.OutboxEvent;
import com.commercelab.orderservice.repo.IOutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Outbox publisher'ın DB transaction sınırları. Kafka I/O KASITLI olarak burada DEĞİL
 * (EventPublisher orchestrate eder) — connection ağ çağrısı boyunca tutulmaz (L3).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxStore {

    private final IOutboxEventRepository outboxRepo;

    @Value("${outbox.batch-size:100}")
    private int batchSize;

    @Value("${outbox.inflight-timeout-ms:60000}")
    private long inflightTimeoutMs;

    /** SKIP LOCKED ile PENDING claim → IN_FLIGHT. Liste tx kapanınca detach olur ama veri yüklü. */
    @Transactional
    public List<OutboxEvent> claimBatch() {
        List<OutboxEvent> rows = outboxRepo.lockPendingBatch(batchSize);
        OffsetDateTime now = OffsetDateTime.now();
        for (OutboxEvent row : rows) {
            row.setStatus("IN_FLIGHT");
            row.setClaimedAt(now);
        }
        return rows;  // dirty-checking commit'te flush eder
    }

    @Transactional
    public void markPublished(UUID id) {
        outboxRepo.findById(id).ifPresent(row -> {
            row.setStatus("PUBLISHED");
            row.setPublishedAt(OffsetDateTime.now());
            row.setClaimedAt(null);
        });
    }

    @Transactional
    public void markFailed(UUID id, String error) {
        outboxRepo.findById(id).ifPresent(row -> {
            row.setStatus("PENDING");   // geri PENDING → sonraki tick yeniden dener
            row.setClaimedAt(null);
            row.setRetryCount(row.getRetryCount() + 1);
            row.setLastError(error);
        });
    }

    @Transactional
    public void reclaimStale() {
        OffsetDateTime threshold = OffsetDateTime.now().minusNanos(inflightTimeoutMs * 1_000_000L);
        int n = outboxRepo.reclaimStale(threshold);
        if (n > 0) {
            log.warn("Reclaimed {} stale IN_FLIGHT outbox rows (poller crash recovery)", n);
        }
    }
}
