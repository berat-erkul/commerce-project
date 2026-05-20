package com.commercelab.orderservice.listener;

import com.commercelab.events.OrderEvents;
import com.commercelab.orderservice.service.OrderStatusService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * order.events consumer — saga'nın bastığı terminal event'leri tüketir.
 * order.events'te kendi ürettiği OrderCreated de akar; event-type header'ı ile
 * sadece OrderCompleted / OrderCancelled işlenir, OrderCreated ack'lenip atlanır.
 * Manual ack only-after-success.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventsListener {

    private static final String EVENT_TYPE_HEADER = "event-type";

    private final OrderStatusService statusService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "order.events", containerFactory = "kafkaListenerContainerFactory")
    public void onEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String eventType = readHeader(record, EVENT_TYPE_HEADER);
        if (eventType == null) {
            // OrderCreated'ı order-service kendi basıyor (Gün 3'e kadar header'sız) — kendi event'i, atla.
            log.debug("order.events message without event-type header (own OrderCreated?), offset={}",
                    record.offset());
            ack.acknowledge();
            return;
        }

        try {
            switch (eventType) {
                case "OrderCompleted" -> handleCompleted(record);
                case "OrderCancelled" -> handleCancelled(record);
                case "OrderCreated" -> log.debug("Own OrderCreated echoed back, ignoring offset={}",
                        record.offset());
                default -> log.warn("Unknown event-type='{}' on order.events offset={}",
                        eventType, record.offset());
            }
            ack.acknowledge();
        } catch (DataIntegrityViolationException race) {
            log.warn("Race lost on {} offset={}: {}", eventType, record.offset(), race.getMessage());
            ack.acknowledge();
        } catch (Exception ex) {
            // ack ETME + rethrow → DefaultErrorHandler retry (backoff) sonra <topic>.DLT.
            log.error("Failed to process {} offset={}: {}",
                    eventType, record.offset(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void handleCompleted(ConsumerRecord<String, String> record) throws JsonProcessingException {
        OrderEvents.OrderCompleted event = objectMapper.readValue(
                record.value(), OrderEvents.OrderCompleted.class);
        OrderStatusService.Result result = statusService.markCompleted(event.eventId(), event.orderId());
        log.info("OrderCompleted orderId={} result={}", event.orderId(), result);
    }

    private void handleCancelled(ConsumerRecord<String, String> record) throws JsonProcessingException {
        OrderEvents.OrderCancelled event = objectMapper.readValue(
                record.value(), OrderEvents.OrderCancelled.class);
        OrderStatusService.Result result = statusService.markCancelled(event.eventId(), event.orderId());
        log.info("OrderCancelled orderId={} reason={} result={}",
                event.orderId(), event.reason(), result);
    }

    private static String readHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
