package com.commercelab.paymentservice.repo;

import com.commercelab.paymentservice.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface IPaymentRepository extends JpaRepository<Payment, UUID> {

    /**
     * K2 idempotency check — bu saga için payment zaten var mı?
     * Var ise yeni INSERT yapılmaz (re-emit yok, inventory kalıbı).
     */
    Optional<Payment> findBySagaInstanceId(UUID sagaInstanceId);
}
