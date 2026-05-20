package com.commercelab.sagaservice.listener;

import com.commercelab.events.PaymentEvents;
import com.commercelab.sagaservice.service.SagaOrchestrator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * payment.events listener.
 * Discriminator: "event-type" Kafka header (payment-service'i biz yazıyoruz,
 * OutboxFactory header standardını uyguluyor).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentEventsListener {

    private final ObjectMapper objectMapper;
    private final SagaOrchestrator orchestrator;

    @KafkaListener(topics = "payment.events", groupId = "saga-service")
    public void onPaymentEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String type = readHeader(record, "event-type");
            if (type == null) {
                log.warn("payment.events message without event-type header, offset={}", record.offset());
                ack.acknowledge();
                return;
            }

            switch (type) {
                case "PaymentCompleted" -> orchestrator.onPaymentCompleted(parse(record.value(), PaymentEvents.PaymentCompleted.class));
                case "PaymentFailed"    -> orchestrator.onPaymentFailed(parse(record.value(), PaymentEvents.PaymentFailed.class));
                case "PaymentRefunded"  -> orchestrator.onPaymentRefunded(parse(record.value(), PaymentEvents.PaymentRefunded.class));
                default                 -> log.debug("Ignoring unknown event-type={} on payment.events", type);
            }

            ack.acknowledge();
        } catch (Exception ex) {
            log.error("PaymentEventsListener failed offset={} err={}", record.offset(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private <T> T parse(String json, Class<T> type) throws Exception {
        return objectMapper.copy()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(json, type);
    }

    private String readHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
