package com.commercelab.paymentservice.listener;

import com.commercelab.events.PaymentEvents;
import com.commercelab.paymentservice.service.PaymentProcessingService;
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
 * payment.commands consumer. event-type header'ına göre dispatch eder.
 * Manual ack: sadece başarılı işlemden sonra commit. Beklenmedik hata = ack ETME,
 * Kafka redeliver yapar (K1/K2 idempotency duplicate'i yutar).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCommandsListener {

    private static final String EVENT_TYPE_HEADER = "event-type";

    private final PaymentProcessingService processingService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "payment.commands", containerFactory = "kafkaListenerContainerFactory")
    public void onCommand(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String eventType = readHeader(record, EVENT_TYPE_HEADER);
        if (eventType == null) {
            log.warn("payment.commands message without event-type header, offset={} key={}",
                    record.offset(), record.key());
            ack.acknowledge();
            return;
        }

        try {
            switch (eventType) {
                case "ProcessPayment" -> handleProcess(record);
                case "RefundPayment" -> log.warn(
                        "RefundPayment received but is stub on Day 1, ignoring offset={}", record.offset());
                default -> log.warn("Unknown event-type='{}' on payment.commands offset={}",
                        eventType, record.offset());
            }
            ack.acknowledge();
        } catch (DataIntegrityViolationException race) {
            // Concurrent saga aynı saga için aynı anda payment yazdı (defansif — normalde
            // olmaz çünkü saga-service tek instance Hafta 2'de). Başka taraf işledi, sessiz ack.
            log.warn("Race lost on {} offset={}: {}", eventType, record.offset(), race.getMessage());
            ack.acknowledge();
        } catch (Exception ex) {
            // ack ETME → Kafka mesajı redeliver eder. K1 sonraki tur'da yakalar.
            log.error("Failed to process {} offset={}: {}",
                    eventType, record.offset(), ex.getMessage(), ex);
        }
    }

    private void handleProcess(ConsumerRecord<String, String> record) throws JsonProcessingException {
        PaymentEvents.ProcessPayment cmd = objectMapper.readValue(
                record.value(), PaymentEvents.ProcessPayment.class);
        PaymentProcessingService.Result result = processingService.process(cmd);
        log.info("ProcessPayment commandId={} sagaId={} result={}",
                cmd.commandId(), cmd.sagaInstanceId(), result);
    }

    private static String readHeader(ConsumerRecord<String, String> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
