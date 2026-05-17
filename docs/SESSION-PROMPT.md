# Bootstrap Prompt — yeni Claude session'ı için

Bu dosya `SESSION-HANDOFF.md` ile birlikte güncellenmeli (her ikisi de senkron kalmalı). Aşağıdaki bloğu yeni session'da ilk mesaj olarak yapıştır.

---

```
~/Desktop/commerce-project — commerce-lab orchestrator saga (M3-extended), Faz 1/Hafta 2.

Sırayla yap:
1. SESSION-HANDOFF.md'yi oku — özellikle ⚡ NEXT, 🚫 DO NOT, 🎯 SCOPE DECISIONS, 🪤 KEY LEARNINGS bölümleri.
2. docs/adr/0001-*.md ve libs/common-events/{Order,Payment,Inventory}Events.java tara — wire kontratları otoriter.
3. saga-service'in mevcut dosya ağacını gör (handoff 📂 FILE INVENTORY).
4. TaskList'i çek; #3 in_progress olduğunu doğrula.

Sonra bana TEK MESAJDA şunları ver (3 cevap, 1 plan):
A) Postgres aborted-tx trap'i 1 cümle + commerce-project'teki bypass kuralımız.
B) Common-events Saf TCC mi Seçenek B mi destekliyor; saga forward chain hangi 3 step?
C) PaymentFailed → OrderCancelled neden Gün 1'de basılır (compensation değil de)?
PLAN: PaymentEventsListener task'ı için somut 5 maddelik uygulama planı (dosya isimleri + sıralama). Handoff ⚡ NEXT'tekiyle tutarlı olsun.

Cevabın hatalıysa düzelt; planın handoff ile çelişiyorsa benim açıklamamı bekle. Sonra Task #3'ü #3 ID üzerinden in_progress tutarak devam et — Claude yazar + anlatır modu.

Kurallar:
- Her task bitince commit mesajı öner (handoff'taki stil — kısa subject + 3-5 satır body).
- Her task bitince SESSION-HANDOFF.md'nin LIVING DOCUMENT bölümlerini güncelle.
- 🚫 DO NOT listesindeki şeyleri yapma; karar değişikliği gerekirse benden onay al.
- Berat'ın itirazlarına ciddiye al — geçmişte 2 mimari refactor onun yakaladığı sorunlardan çıktı.
```
