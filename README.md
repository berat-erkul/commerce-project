# commerce-lab

Mini e-ticaret + finans mikroservis vitrin projesi.

## Stack

- Java 21 (LTS)
- Spring Boot 3.4.1
- Spring Cloud 2024.0.0
- Maven 3.9+
- PostgreSQL 16, Redis 7, Kafka (KRaft mode), Elasticsearch 8

## Servisler

| Servis | Durum | Port | Açıklama |
|---|---|---|---|
| order-service | Hafta 2 (aktif) | 8081 | Saga orchestrator |
| payment-service | Hafta 2 (aktif) | 8082 | Ödeme + idempotency |
| inventory-service | Hafta 2 (aktif) | 8083 | Stok rezervasyonu |
| catalog-service | Hafta 3 | 8084 | Ürün CRUD |
| search-service | Hafta 3 | 8085 | Elasticsearch arama |
| cart-service | Hafta 5 | 8086 | Redis sepet |
| ledger-service | Hafta 6 | 8087 | CQRS + Event Sourcing |
| identity-service | Hafta 7 | 8088 | Keycloak integration |
| api-gateway | Hafta 5+ | 8080 | Spring Cloud Gateway |
| discovery-server | Hafta 5+ | 8761 | Eureka |
| config-server | Hafta 5+ | 8888 | Centralized config |

## Çalıştırma

```bash
# Altyapıyı ayağa kaldır (kafka, postgres, redis, kafka-ui)
docker compose up -d

# Bir servisi çalıştır
cd services/order-service
./mvnw spring-boot:run
```

## Yapı

```
commerce-project/
├── docker-compose.yml      # Kafka, Postgres, Redis, kafka-ui
├── infra/                  # Topic init, dashboard, realm export
├── services/               # Her servis bağımsız Spring Boot uygulaması
├── libs/common-events/     # Paylaşılan event şemaları
└── docs/adr/               # Architecture Decision Records
```

## Disiplin

- Tek tek servis ekle. Hafta 2'de 3 servisten fazlasını ayağa kaldırma.
- Her gün 1-3 commit.
- Önemli kararları `docs/adr/` altına 1 sayfa yaz.
