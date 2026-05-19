-- Hafta 2 — saga başına çok kalemli sipariş desteği.
-- V1'deki UNIQUE(saga_instance_id) tek satır zorluyordu; çok kalemli order için
-- her ürün ayrı satır yazmak gerek. K2 idempotency (saga_instance_id, product_id)
-- çiftine taşınıyor.

ALTER TABLE stock_reservations
    DROP CONSTRAINT uq_reservations_saga;

ALTER TABLE stock_reservations
    ADD CONSTRAINT uq_reservations_saga_product UNIQUE (saga_instance_id, product_id);
