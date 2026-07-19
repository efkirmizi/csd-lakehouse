# CSD Data Lakehouse — Referans Kurulum

MinIO + Iceberg + Nessie + PySpark + ClickHouse ile uçtan uca, lokalde çalışan Lakehouse mimarisi.

Bir **menkul kıymet saklama kuruluşu** (CSD — *Central Securities Depository*)
analitik iş yükünü modelleyen uçtan uca bir referans mimarisi: alım-satım işlemleri,
gün sonu pozisyonlar, mutabakat ve düzenleyici saklama. Sermaye piyasası alan
bilgisi, bir e-ticaret demosundan daha ilginç bir problem uzayı sunduğu için
seçildi — ama **veri tamamen sentetiktir** (`random()` + `generate_series` ile
üretilir) ve şema, gerçek bir kurumun şeması değil, temsili bir modeldir.

> **Bağlam:** Bu proje bir mühendislik stajı sırasında, kendini geliştirme amaçlı
> geliştirildi. "CSD" jenerik bir sektör terimidir; herhangi bir kurumu temsil
> etmez. Gerçek veri, gerçek şema veya kuruma özel bilgi **içermez**.

> **Durum:** Bu bir lokal referans/kanıt kurulumudur. Üretime giderken gereken eksikler `docs/01-mimari.md` bölüm 8'de açıkça listelenmiştir.

---

## Hızlı başlangıç

```powershell
# 1. Spark imajını derle (JAR'lar imaja gömülür, ~3-5 dk, bir kez)
docker compose build

# 2. Tüm stack'i ayağa kaldır
docker compose up -d

# 3. Postgres seed'inin bitmesini bekle
#    Süre .env içindeki OLTP_ISLEM_ROWS ile orantılı: 2M ~1-2 dk, 20M ~10-15 dk
docker compose logs -f postgres
#    ">> OLTP kaynak hazir." satırını görünce Ctrl+C

# 4. Servisleri doğrula (satır sayısı + şema bütünlüğü)
.\run.ps1 status
```

> **Seed yarıda kesilirse** (Docker çökmesi, makine kapanması) Postgres bir
> daha init çalıştırmaz — veri dizini dolu olduğu için `Skipping initialization`
> der ve **yarım bir şema** bırakır: satırlar yerinde, indeksler/view/istatistikler
> eksik. `.\run.ps1 status` bunu yakalar, `.\run.ps1 repair-oltp` tamamlar.
> Bu tuzağa 20M testinde düşüldü; ayrıntı `sql/postgres/99_repair.sql` başlığında.

| Servis | Adres | Giriş |
|---|---|---|
| MinIO Konsol | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| Nessie API | http://localhost:19120/api/v2/config | — |
| Spark Master UI | http://localhost:8080 | — |
| ClickHouse Play | http://localhost:8123/play | `analytics` / `analytics_pass` |
| Jupyter *(ops.)* | http://localhost:8888 | token yok |

---

## Pipeline'ı çalıştır

```powershell
.\run.ps1 pipeline        # 01 → 02 → 03 sırayla
```

veya tek tek:

```powershell
.\run.ps1 job 01_oltp_to_bronze.py --full
.\run.ps1 job 02_bronze_to_silver.py
.\run.ps1 job 03_silver_to_gold.py
```

Sonra ClickHouse'u bağla:

```powershell
# Iceberg tablolarının GERÇEK yollarını çözüp ClickHouse görünümlerini üretir
.\run.ps1 job 90_generate_ch_views.py

# Üretilen DDL'i yükle -> lake.silver_islem, lake.gold_* ... hazır
docker compose exec -T clickhouse clickhouse-client `
  --user analytics --password analytics_pass --multiquery < out/lake_views.sql

# Sıcak veriyi MergeTree'ye materyalize et
.\run.ps1 sql 03_materialize_mergetree.sql
```

> **Neden ayrı bir üretim adımı var?** Nessie tablo dizinlerine UUID ekler
> (`silver/islem_57738cb1-.../`) ve bu UUID her kurulumda farklıdır. SQL'e elle
> yazılamaz. `90_generate_ch_views.py` yolları katalogdan okuyup DDL'i üretir.
> Şema değişince tekrar çalıştırın.

> **`01_catalog.sql` (DataLakeCatalog): keşif çalışıyor, okuma 25.6'da çalışmıyor.**
> Keşif (`SHOW TABLES`) **doğru base URL ile çalışır** (`/iceberg`, `/iceberg/main`
> değil — ref bir *prefix*'tir). `SELECT` ise 25.6'da metadata yol-birleşme
> hatasıyla patlar. Bu hata ClickHouse'ta **aktif düzeliyor**: 25.6 → 25.8 → 26.6
> ölçüldü; **26.6'da yol birleşmesi gitmiş**. Tam çalışır hale gelince (doğrulayın)
> katalog okumasına geçilebilir. Ölçüm ve kanıt: `sql/clickhouse/01_catalog.sql` başlığı.

---

## Regresyon kontrolü — her değişiklikten sonra

```powershell
.\run.ps1 verify-all      # saniyeler sürer, hata varsa sıfır olmayan çıkış kodu
```

Bu projedeki gerçek hataların **hepsi sessizdi**: job'lar yeşil bitiyor, rakam
yanlış çıkıyordu. Elle kontrol onları kaçırdı. `verify-all` o kontrolleri
otomatik ve sesli yapar:

| # | Kontrol | Neden var |
|---|---|---|
| 1–2 | Mutabakat: satır sayısı + **değer** farkı | Sayı tutup değerin tutmaması mümkün |
| 3 | `gold`'da `net_adet = 0` satır yok | 20M'de yakalanan gerçek hata; filtre silinirse yakalar |
| 4–5 | İki yönlü kayıp kayıt (anti-join) | "Sayı aynı ama kayıtlar farklı" durumu |
| 6 | `bronze = silver + karantina` | Karantina `.append()` hatası bu eşitliği bozuyordu |
| 7 | MergeTree kopyası = federe görünüm | Görünümler bayatlarsa (UUID değişimi) yakalar |
| 8 | `lake.*` görünüm sayısı | Görünüm üretimi kırılırsa yakalar |

**Doğrulandı:** temiz durumda 8/8 geçiyor (çıkış 0); kasten bir invaryant
bozulduğunda ilgili satır `KALDI` işaretleniyor, **diğer kontroller yine
çalışıyor** (ilk hatada durmuyor) ve çıkış kodu sıfır olmuyor.

> **Performans assert'i bilerek YOK.** "Gold sorgusu < 50 ms" gibi bir eşik
> makineye ve anlık yüke bağlıdır, arada bir boşuna kırmızı yanar — ve
> güvenilmeyen bir kontrol, bir süre sonra bakılmayan bir kontrole dönüşür.
> Performans ölçümü ayrı ve bilinçli: `.\run.ps1 sqltest 03_clickhouse_perf.sql`

---

## Orkestrasyon (opsiyonel) — Airflow

Çekirdek demo Airflow'a bağımlı **değil**; varsayılan `docker compose up` onu başlatmaz.

```powershell
docker compose --profile orchestration up -d --build
# -> http://localhost:8090   (admin / admin — .env içinden değiştirin)
```

`csd_lakehouse_pipeline` DAG'i tüm zinciri bağımlılıklarıyla çalıştırır:

```
01_oltp_to_bronze → 02_bronze_to_silver → 03_silver_to_gold
    → 04_refresh_ch_views → 05_materialize_mergetree → 06_mutabakat_kapısı
```

**En önemli görev sonuncusu.** Pipeline'ın "yeşil" bitmesi, ürettiği rakamın
*doğru* olduğu anlamına gelmez — bu projede tam olarak o sınıftan bir hata yaşandı
(gold, net-sıfır pozisyonları kaynaktan farklı ele alıyordu; 2M'de tesadüfen
tutuyor, 20M'de ayrışıyordu, hiçbir job hata vermiyordu). `06_mutabakat_kapısı`
lakehouse'un pozisyonlarını kaynak sistemin kendi kaydıyla karşılaştırır ve
fark varsa **DAG'ı kırmızıya düşürür**. Doğrulandı: fark yokken çıkış kodu 0,
fark varken sıfır değil → görev başarısız.

> **Güvenlik notu — bilerek yapılan taviz.** DAG, Spark'a iş göndermek için
> docker soketini kullanıyor (`docker exec lh-spark-master spark-submit ...`).
> Soket erişimi pratikte host'ta root yetkisine yakındır ve container bu yüzden
> root çalışıyor. Bu **yalnızca lokal referans kurulumu** için kabul edilebilir.
> Üretimde `KubernetesPodOperator` / Spark on K8s kullanın, soket paylaşmayın.
> Gerekçe: `dags/csd_lakehouse_pipeline.py` başlığı.

---

## Test senaryoları (yönetici sunumu için)

Hepsi bu ortamda **20.000.000 satır** üzerinde çalıştırıldı. Aşağıdaki rakamlar gerçek çıktıdan.

| # | Senaryo | Ölçülen sonuç | Komut |
|---|---|---|---|
| 1 | Time Travel + Rollback | 20M satır ×100 bozuldu → **5,49 sn**'de birebir geri geldi | `.\run.ps1 test 01_time_travel.py` |
| 2 | Nessie Branching (WAP) | Branch'te 50k bozuk satır, main **değişmedi**; DQ yakaladı, merge engellendi | `.\run.ps1 test 02_nessie_branching.py` |
| 3 | Performans (3 katman) | Postgres **15.414 ms** → Gold **10 ms** (**~1.541x**) | `.\run.ps1 sqltest 03_clickhouse_perf.sql` |
| 4 | Şema Evrimi | 3 sütun **0,43 sn**, **0 bayt** yeniden yazıldı | `.\run.ps1 test 04_schema_evolution.py` |
| 5 | **Mutabakat** (OLTP ↔ Lakehouse) | **10.384.619** pozisyon, **0 fark**, 0 kayıp kayıt | `.\run.ps1 sqltest 05_mutabakat.sql` |
| 6 | **ClickHouse branch izolasyonu** | Bir **sınır** buldu — aşağıya bakın | `.\run.ps1 test 06_clickhouse_branch_izolasyonu.py` |

Senaryo 1 ve 2 **kendi kopya tablolarında** çalışır (`silver.islem_timetravel_demo`, `silver.islem_wap_demo`) — üretim tablosuna dokunmazlar, istediğiniz sırada ve istediğiniz kadar çalıştırabilirsiniz.

> **Canlı demo planlıyorsanız süreleri bilin.** 20M satırda senaryo 1 ve 2, önce
> 20M satırlık bir kopya çıkarıp sonra copy-on-write güncelleme yaptıkları için
> **~7 dakika** sürer — toplantıda ekrana bakarak beklemek istemezsiniz.
> İki seçenek: (a) senaryoları toplantıdan **önce** çalıştırıp çıktıyı gösterin,
> (b) demo için `.env` içinde `OLTP_ISLEM_ROWS=2000000` ile çalışın — senaryolar
> saniyeler sürer, mimari iddialar birebir aynı kalır. Performans rakamlarını
> sunarken 20M ölçümlerini kullanın; onlar zaten bu dosyada kayıtlı.
> Senaryo 3, 4, 5 ve 6 her ölçekte hızlıdır.

### Performans — dürüst okuma (20M satır)

| Katman | Süre | Okunan satır |
|---|---|---|
| 1. PostgreSQL (bugünkü yol) | **15.414 ms** | 2,45 M |
| 2. ClickHouse → Iceberg (federe) | 198 ms | 1,38 M |
| 3. ClickHouse → MergeTree (detay) | 83 ms | 2,37 M |
| 4. ClickHouse → **Gold özet** | **10 ms** | 115 k |

**Ölçek testinin asıl bulgusu — makas gerçekten açılıyor:**

| | 2M satır | 20M satır | 10x veri ile |
|---|---|---|---|
| PostgreSQL | 1.034 ms | **15.414 ms** | **14,9x kötüleşti** |
| Gold özet | 8 ms | **10 ms** | 1,25x — neredeyse **sabit** |
| **Hızlanma** | 129x | **1.541x** | — |

PostgreSQL **superlineer** kötüleşiyor (10x veri → 15x süre); ön-toplanmış gold katmanı ise pratikte sabit kalıyor, çünkü özet tablo 24k→939k büyürken sorgunun okuduğu satır 115k'da kalıyor. Bu, mimarinin en güçlü tek argümanı — ve **ölçüldü, tahmin edilmedi**.

### Eşzamanlı yük (50 kullanıcı, 20M satır)

| | Iceberg (federe) | MergeTree (gold) | Fark |
|---|---|---|---|
| QPS | 49 | **211** | 4,3x |
| P50 | 918 ms | 160 ms | 5,7x |
| P95 | 1.374 ms | 343 ms | 4,0x |
| **P99** | **1.559 ms** | **426 ms** | 3,7x |
| P99.9 | 1.842 ms | 469 ms | 3,9x |

**Materyalizasyonun gerekçesi burada.** 20M + 50 eşzamanlı kullanıcıda federe yol **saniyeyi kırıyor** (P99 1,56 sn); MergeTree 426 ms'de kalıyor. 2M'de P99 farkı 2,8x idi, 20M'de 3,7x — eşzamanlılıkta da makas açılıyor.

**Sunumda dikkat:** "ClickHouse olmadan sorgular saniyeler sürer" **demeyin** — tek kullanıcıda federe okuma 198 ms, yani sub-second. Doğru cümle:

> *"Bugünkü PostgreSQL yolu 15 saniye; lakehouse'ta aynı soru 10 milisaniye. Veriyi kopyalamadan da 198 ms'de cevaplıyoruz — materyalizasyonu nokta atışı sorgular ve **eşzamanlılık altında taahhüt verebilmek** için yapıyoruz, temel hız için değil."*

### ⚠️ Bilinen sınır: federe görünümler branch izolasyonunu görmez

Senaryo 6 bunu ölçtü. `icebergS3()` Nessie katalogunu **atlar** ve tablo dizinindeki en yeni `metadata.json`'ı okur — bu bir ETL branch'inin commit'i olabilir:

- Spark: main 125.277 | branch 126.277 → **izole** ✅
- ClickHouse: 126.277 → **branch verisini gördü** ❌
- `DROP BRANCH` bunu **düzeltmez** (metadata S3'te kalır); main'e yeni commit gerekir

Başarılı ETL'de sorun kendini toplar (merge main'e yeni commit atar). **Asıl risk, kalite kontrolü patladığında** — yani tam da WAP'ın koruması gereken anda: merge edilmemiş bozuk branch federe görünümlerde görünür kalır.

**Sonuç:** SLA'lı iş ve dashboard'lar `csd.*` (MergeTree) tablolarından beslenmeli; `lake.*` keşif içindir. Kalıcı çözüm `DataLakeCatalog`'un düzelmesi — bu mimarideki en değerli tek iyileştirme.

---

## Proje yapısı

```
├── docker-compose.yml          Tüm stack
├── .env                        Sürüm matrisi + seed hacmi + kimlik bilgileri
├── docker/spark/Dockerfile     Iceberg+Nessie JAR'ları gömülü Spark imajı
├── conf/
│   ├── spark/                  spark-defaults (sırsız ayarlar)
│   ├── nessie/                 application.properties (S3 secret'ı buradan)
│   └── clickhouse/             named collections, S3 endpoint, listen_host
├── sql/
│   ├── postgres/               00 nessie db · 01 OLTP şeması + seed · 99 onarım
│   └── clickhouse/             01 katalog · 02 federe · 03 materyalizasyon
├── jobs/
│   ├── common/session.py       SparkSession fabrikası (katalog ayarları)
│   ├── 01_oltp_to_bronze.py    JDBC paralel okuma → Iceberg (WAP)
│   ├── 02_bronze_to_silver.py  temizleme + karantina
│   ├── 03_silver_to_gold.py    ön-toplama
│   ├── 90_generate_ch_views.py Iceberg yollarını çözüp CH görünümü üretir
│   └── 99_maintenance.py       compaction, manifest, Nessie GC yönlendirmesi
├── tests/                      6 kanıt senaryosu
└── docs/01-mimari.md           Mimari kararlar ve gerekçeler
```

---

## Mimarinin özeti

**Üç ayrı sorumluluk, üç ayrı bileşen:**

- **MinIO** — baytlar nerede? (Parquet)
- **Iceberg** — bu baytlar nasıl bir tablo? (metadata, snapshot, şema)
- **Nessie** — şu anki tablo hangi metadata'yı gösteriyor? (pointer + versiyon)

**Nessie'nin iki cephesi:**

```
Spark      ──►  :19120/api/v2           native API (branch/merge/log)
ClickHouse ──►  :19120/iceberg/<branch> Iceberg REST Catalog cephesi
```

**ClickHouse'un iki modu — en önemli mimari karar:**

| Mod | Tek sorgu | 50 eşzamanlı kullanıcı (P99) | Ne zaman |
|---|---|---|---|
| Federe (Iceberg üzerinden) | 17 – 229 ms | 877 ms | Keşif, ad-hoc, soğuk veri |
| Materyalize (MergeTree) | 4 – 51 ms | 310 ms | Dashboard, SLA'lı iş |

> Bu tablonun eski hâlinde federe okuma için **"0.3 – 3 sn"** yazıyordu. **Ölçtük, yanlıştı.**
> Federe okuma da bu ölçekte sub-second. Materyalizasyonun gerekçesi "saniyeden
> milisaniyeye inmek" değil; **nokta atışı erişim** (Iceberg'de indeks yok) ve
> **eşzamanlılık altında taahhüt verebilmek**. Ayrıntı ve ölçüm: yukarıdaki
> "Performans — dürüst okuma" bölümü.

İkisi alternatif değil **tamamlayıcıdır**. Iceberg = tek gerçek kaynak. ClickHouse = türetilmiş, silinebilir okuma kopyası. Detay: `docs/01-mimari.md` bölüm 4.

---

## Sık karşılaşılan sorunlar

**`docker compose build` sırasında 404**
Maven koordinatı değişmiş olabilir. `.env` içindeki `ICEBERG_VERSION` / `NESSIE_VERSION` değerlerini [Maven Central](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/)'dan doğrulayıp güncelleyin.

**MinIO imaj tag'i bulunamıyor**
`docker-compose.yml` içindeki `minio/minio:RELEASE...` tag'ini Docker Hub'daki güncel bir sürümle değiştirin.

**ClickHouse `Unknown setting allow_experimental_database_iceberg`**
Sürümünüzde stable'a alınmış. `SET` satırını atlayın.

**ClickHouse `DataLakeCatalog` çalışmıyor**
`sql/clickhouse/01_catalog.sql` içindeki **C) fallback** yolunu kullanın (`icebergS3()` tablo fonksiyonu). Test senaryolarının hiçbiri `DataLakeCatalog`'a bağımlı değil.

**Spark job'ı `NESSIE_URI tanimli degil` diyor**
Job'ı host'tan değil container içinden çalıştırın: `.\run.ps1 job ...` veya `docker compose exec spark-master ...`

**Postgres seed çok uzun sürüyor**
`.env` içinde `OLTP_ISLEM_ROWS` değerini düşürün, sonra `docker compose down -v; docker compose up -d`.

---

## Temizlik

```powershell
docker compose down          # container'ları durdur, veriyi koru
docker compose down -v       # veriyi de sil (seed baştan çalışır)
```
