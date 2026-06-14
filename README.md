# SOA — Домашнее задание №4: Notification Platform

Централизованная платформа уведомлений онлайн-банка.  
Требования, ASR, архитектурное мышление, RFC по гарантированной доставке.

---

## 📄 Документы для сдачи

| Документ | Содержание | Пункты задания |
|----------|------------|----------------|
| **[SUBMISSION.md](./SUBMISSION.md)** | FR, NFR, ASR, арх. вопросы, последствия, риски, расчёты | **1–7** |
| **[RFC-critical-delivery-failover.md](./RFC-critical-delivery-failover.md)** | RFC: 2 архитектурных варианта, C4, sequence, sizing, cost, decision | **8** |

---

## 📊 Диаграммы (PlantUML)

| Файл | Описание |
|------|----------|
| [variant-a-c4-container.puml](./diagrams/variant-a-c4-container.puml) | C4 Container — Variant A (Orchestrator) |
| [variant-a-seq-happy-path.puml](./diagrams/variant-a-seq-happy-path.puml) | Sequence — happy path (push OK) |
| [variant-a-seq-failover.puml](./diagrams/variant-a-seq-failover.puml) | Sequence — failover push → SMS |
| [variant-b-c4-container.puml](./diagrams/variant-b-c4-container.puml) | C4 Container — Variant B (Choreography) |
| [variant-b-seq-happy-path.puml](./diagrams/variant-b-seq-happy-path.puml) | Sequence — happy path |
| [variant-b-seq-failover.puml](./diagrams/variant-b-seq-failover.puml) | Sequence — failover |

**PNG (уже отрендерены):** [`diagrams/png/`](./diagrams/png/)

| PNG | Описание |
|-----|----------|
| [variant_a_c4_container.png](./diagrams/png/variant_a_c4_container.png) | C4 — Variant A |
| [variant_a_seq_happy.png](./diagrams/png/variant_a_seq_happy.png) | Sequence happy path A |
| [variant_a_seq_failover.png](./diagrams/png/variant_a_seq_failover.png) | Sequence failover A |
| [variant_b_c4_container.png](./diagrams/png/variant_b_c4_container.png) | C4 — Variant B |
| [variant_b_seq_happy.png](./diagrams/png/variant_b_seq_happy.png) | Sequence happy path B |
| [variant_b_seq_failover.png](./diagrams/png/variant_b_seq_failover.png) | Sequence failover B |

Пересборка: `plantuml -tpng diagrams/*.puml -o png`

---

## ✅ Чеклист (10 баллов)

| § | Критерий | Статус |
|---|----------|--------|
| 1 | ≥ 5 FR, поведение, бизнес-связь | ✅ 8 FR |
| 2 | ≥ 5 NFR, измеримые | ✅ 8 NFR |
| 3 | ≥ 3 ASR + связи + влияние | ✅ 5 ASR |
| 4 | ≥ 3 арх. вопроса | ✅ 4 |
| 5 | Последствия каждого ASR | ✅ |
| 6 | ≥ 2 неподходящих решения | ✅ 4 |
| 7 | ≥ 2 неопределённости | ✅ 3 |
| 8 | RFC: 2 варианта, C4, sequence, расчёты, trade-off | ✅ |
| 9 | Диаграммы happy + failover | ✅ 6 .puml файлов |
| 10 | Обоснованный выбор + cost + sizing | ✅ RFC §9–10, §17 |

---

## Ключевые решения

- **Failover:** push (8s) → SMS (30s) → email (60s)
- **Транзакционные** нельзя отключить; маркетинговые — полностью
- **SLA:** P99 ≤ 3s (first attempt), ≥ 99.99% delivered
- **Архитектура:** Orchestrator-centric (Variant A) — Go + Kafka + PostgreSQL + Redis
