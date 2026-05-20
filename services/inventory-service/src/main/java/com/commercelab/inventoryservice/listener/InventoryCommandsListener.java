package com.commercelab.inventoryservice.listener;

import com.commercelab.events.InventoryEvents;
import com.commercelab.inventoryservice.exception.InsufficientStockException;
import com.commercelab.inventoryservice.service.StockReservationService;
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
 * inventory.commands consumer. event-type header'ına göre dispatch eder.
 * Manual ack: sadece başarılı işlemden sonra commit. Beklenmedik hata = ack ETME,
 * Kafka redeliver yapar (K1/K2 idempotency duplicate'i yutar).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandsListener {

    private static final String EVENT_TYPE_HEADER = "event-type";

    private final StockReservationService reservationService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "inventory.commands", containerFactory = "kafkaListenerContainerFactory")
    public void onCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String eventType = readHeader(record, EVENT_TYPE_HEADER);
        if (eventType == null) {
            log.warn("inventory.commands message without event-type header, offset={} key={}",
                    record.offset(), record.key());
            ack.acknowledge();
            return;
        }

        try {
            switch (eventType) {
                case "ReserveStock" -> handleReserve(record);
                case "ReleaseStock" -> handleRelease(record);
                default -> log.warn("Unknown event-type='{}' on inventory.commands offset={}",
                        eventType, record.offset());
            }
            ack.acknowledge();
        } catch (DataIntegrityViolationException race) {
            // Concurrent saga aynı (saga, product) için aynı anda yazdı (defansif — normalde
            // olmaz çünkü saga-service tek instance Hafta 2'de). Başka taraf işledi, sessiz ack.
            log.warn("Race lost on {} offset={}: {}", eventType, record.offset(), race.getMessage());
            ack.acknowledge();
        } catch (Exception ex) {
            // ack ETME + rethrow → DefaultErrorHandler retry (backoff) sonra <topic>.DLT.
            // K1/K2 retry'da duplicate'i yutar.
            log.error("Failed to process {} offset={}: {}",
                    eventType, record.offset(), ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    private void handleReserve(ConsumerRecord<String, String> record) throws JsonProcessingException {
        InventoryEvents.ReserveStock cmd = objectMapper.readValue(
                record.value(), InventoryEvents.ReserveStock.class);
        try {
            StockReservationService.Result result = reservationService.reserve(cmd);
            log.info("ReserveStock commandId={} sagaId={} result={}",
                    cmd.commandId(), cmd.sagaInstanceId(), result);
        } catch (InsufficientStockException ex) {
            log.info("ReserveStock failed commandId={} sagaId={} reason={}",
                    cmd.commandId(), cmd.sagaInstanceId(), ex.getMessage());
            reservationService.emitFailed(cmd, ex.getMessage());
        }
    }

    private void handleRelease(ConsumerRecord<String, String> record) throws JsonProcessingException {
        InventoryEvents.ReleaseStock cmd = objectMapper.readValue(
                record.value(), InventoryEvents.ReleaseStock.class);
        StockReservationService.ReleaseResult result = reservationService.release(cmd);
        log.info("ReleaseStock commandId={} sagaId={} result={}",
                cmd.commandId(), cmd.sagaInstanceId(), result);
    }

    private static String readHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
