package com.commercelab.inventoryservice.repo;

import com.commercelab.inventoryservice.entity.InventoryItem;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.Optional;
import java.util.UUID;

public interface IInventoryItemRepository extends JpaRepository<InventoryItem, UUID> {

    /**
     * Pessimistic write lock — Postgres'te SELECT ... FOR UPDATE üretir.
     * Aynı productId üzerinde concurrent reserve çağrıları sıraya girer,
     * race olmadan stok atomik düşer.
     * <p>
     * Transaction commit/rollback'te lock otomatik bırakılır.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select i from InventoryItem i where i.productId = :productId")
    Optional<InventoryItem> findByProductIdForUpdate(@Param("productId") UUID productId);
}
