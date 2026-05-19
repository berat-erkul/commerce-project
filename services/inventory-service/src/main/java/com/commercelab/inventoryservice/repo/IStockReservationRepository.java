package com.commercelab.inventoryservice.repo;

import com.commercelab.inventoryservice.entity.StockReservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface IStockReservationRepository extends JpaRepository<StockReservation, UUID> {

    /**
     * K2 idempotency check — aynı saga+product için reservation var mı?
     * Var ise mevcut reservation aynen geri döndürülür (yeni insert yapılmaz).
     */
    Optional<StockReservation> findBySagaInstanceIdAndProductId(UUID sagaInstanceId, UUID productId);

    /**
     * Saga'nın tüm reservation'ları (audit + compensation release için).
     */
    List<StockReservation> findBySagaInstanceId(UUID sagaInstanceId);
}
