package com.commercelab.paymentservice.repo;

import com.commercelab.paymentservice.entity.Refund;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface IRefundRepository extends JpaRepository<Refund, UUID> {

    /** K2 idempotency — bu payment için refund zaten var mı? */
    Optional<Refund> findByPaymentId(UUID paymentId);
}
