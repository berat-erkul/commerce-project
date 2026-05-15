package com.commercelab.sagaservice.listener;

import com.commercelab.events.OrderEvents;
import com.commercelab.sagaservice.service.SagaOrchestrator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * order.events topic'ini dinler.
 * <p>
 * Şu an sadece OrderCreated ile ilgileniyoruz. order.events topic'inde gelecekte
 * saga-service'in kendi bastığı OrderCompleted/OrderCancelled de gezecek; onları
 * skip ediyoruz (alıcısı choreography fan-out tüketicileri — notification, analytics).
 * <p>
 * Discriminator: order-service'in publisher'ı şu an Kafka header eklemiyor, dolayısıyla
 * payload'ı OrderCreated record'una lenient deserialize edip null-check ile ayırıyoruz.
 * TODO Gün 3: order-service publisher'a event-type header eklenince bu hack kalkacak.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventsListener {

    private final ObjectMapper objectMapper;
    private final SagaOrchestrator orchestrator;

    @KafkaListener(topics = "order.events", groupId = "saga-service")
    public void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            OrderEvents.OrderCreated event = tryParseOrderCreated(record.value());
            if (event == null) {
                log.debug("Skipping non-OrderCreated message on order.events offset={}", record.offset());
                ack.acknowledge();
                return;
            }
            orchestrator.startOrderPlacementSaga(event);
            ack.acknowledge();
        } catch (Exception ex) {
            // ack atmıyoruz → in-memory error handler retry edecek (Gün 3'te DLQ stack gelecek).
            log.error("OrderEventsListener failed offset={} key={} err={}",
                    record.offset(), record.key(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Discriminator: gelen JSON'u OrderCreated'e lenient deserialize et.
     * customerId/totalAmount/items null ise OrderCreated değildir (OrderCompleted veya
     * OrderCancelled) — null döndür, listener skip + ack eder.
     */
    private OrderEvents.OrderCreated tryParseOrderCreated(String json) {
        try {
            ObjectMapper lenient = objectMapper.copy()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            OrderEvents.OrderCreated event = lenient.readValue(json, OrderEvents.OrderCreated.class);
            if (event.customerId() == null || event.totalAmount() == null || event.items() == null) {
                return null;
            }
            return event;
        } catch (Exception ex) {
            log.warn("Failed to deserialize as OrderCreated: {}", ex.getMessage());
            return null;
        }
    }
}
