<#
.SYNOPSIS
    CSD Data Lakehouse - yardimci komut sarmalayicisi

.DESCRIPTION
    Uzun docker compose komutlarini kisaltir.

.EXAMPLE
    .\run.ps1 up
    .\run.ps1 status
    .\run.ps1 pipeline
    .\run.ps1 job 01_oltp_to_bronze.py --full
    .\run.ps1 test 02_nessie_branching.py
    .\run.ps1 sql 01_catalog.sql
    .\run.ps1 shell
#>

param(
    [Parameter(Position = 0)]
    [string]$Command = "help",

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$Rest
)

$ErrorActionPreference = "Stop"

function Get-TailArgs {
    <#
      Ilk elemandan SONRAKI elemanlari dondurur.

      NEDEN AYRI BIR FONKSIYON? PowerShell'de $a[1..($a.Count-1)] tuzaktir:
      dizi TEK elemanliysa ifade $a[1..0] olur, '1..0' ise AZALAN bir aralik
      (@(1,0)) uretir ve dizi TERS cevrilerek $null + $a[0] doner. Sonuc:
      script adi komuta IKINCI KEZ arguman olarak eklenir --
          spark-submit /opt/spark/tests/04_x.py 04_x.py
      Bu tuzaga dustuk. Acik bir sayim kontrolu tek dogru cozum.
    #>
    param([string[]]$Items)
    if ($null -eq $Items -or $Items.Count -le 1) { return @() }
    return $Items[1..($Items.Count - 1)]
}

function Invoke-SparkSubmit {
    param([string]$ScriptPath, [string[]]$JobArgs)

    $argList = @(
        "compose", "exec", "-T", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        $ScriptPath
    )
    if ($JobArgs) { $argList += $JobArgs }

    Write-Host "-> spark-submit $ScriptPath $($JobArgs -join ' ')" -ForegroundColor Cyan

    # ---- STDERR TUZAGI -- native komut + $ErrorActionPreference='Stop' ----
    # Spark TUM INFO loglarini STDERR'e yazar (hata degil, normal cikti).
    # PS 5.1'de, cagiran taraf ciktiyi yonlendirirse (ornegin
    #     .\run.ps1 pipeline 2>$null
    #     .\run.ps1 pipeline > kayit.log 2>&1        )
    # her stderr satiri ErrorRecord'a donusur; dosyanin basindaki
    # $ErrorActionPreference='Stop' yuzunden script IS BASARILI OLSA BILE
    # tam burada durur. (Bu tuzaga dustuk: pipeline'i log'a yazdirmak
    # istedigimizde 01 job'i calisirken script oldu.)
    #
    # Cozum: native cagri suresince 'Continue'a dusup sonra geri aliyoruz.
    # Boylece run.ps1 CI'da veya log dosyasina yazarken de calisir.
    $eap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { & docker @argList } finally { $ErrorActionPreference = $eap }
}

function Invoke-ChSql {
    param([string]$FilePath)
    Write-Host "-> clickhouse-client < $FilePath" -ForegroundColor Cyan
    # Ayni stderr tuzagi (bkz. Invoke-SparkSubmit): clickhouse-client
    # ilerleme bilgisini stderr'e yazar.
    $eap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        Get-Content $FilePath -Raw | docker compose exec -T clickhouse `
            clickhouse-client --user analytics --password analytics_pass `
            --multiquery --progress
    } finally { $ErrorActionPreference = $eap }
}

function Invoke-RefreshViews {
    # ClickHouse federe gorunumlerini (lake.*) yeniden uretir.
    #
    # NEDEN AYRI KOMUT: Iceberg tablo yollari UUID'li ve tablo yeniden
    # olusturulursa UUID DEGISIR. ETL'i tek tek job olarak kosarsaniz
    # (ornegin sadece 'job 03_silver_to_gold.py'), eski gorunum sessizce
    # ESKI dizini okumaya devam eder -> "ETL kostu ama ClickHouse eski
    # veriyi gosteriyor". Bu komut yollari katalogdan yeniden cozer.
    # 'pipeline' bunu otomatik cagirir; tek job kosanlar elle cagirir.
    Write-Host "`n-> ClickHouse gorunumleri yeniden uretiliyor" -ForegroundColor Cyan
    Invoke-SparkSubmit "/opt/spark/jobs/90_generate_ch_views.py" @()
    if ($LASTEXITCODE -ne 0) {
        Write-Host "   90_generate_ch_views.py basarisiz." -ForegroundColor Red
        return $false
    }
    $viewFile = "$PSScriptRoot\out\lake_views.sql"
    if (-not (Test-Path $viewFile)) {
        Write-Host "   out/lake_views.sql uretilemedi." -ForegroundColor Red
        return $false
    }
    # stderr tuzagi -- bkz. Invoke-SparkSubmit icindeki not.
    $eap = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        Get-Content $viewFile -Raw | docker compose exec -T clickhouse `
            clickhouse-client --user analytics --password analytics_pass --multiquery
    } finally { $ErrorActionPreference = $eap }
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   lake.* gorunumleri guncellendi." -ForegroundColor Green
        return $true
    }
    Write-Host "   Gorunumler ClickHouse'a yuklenemedi." -ForegroundColor Red
    return $false
}

switch ($Command.ToLower()) {

    "build" {
        docker compose build
    }

    "up" {
        docker compose up -d
        Write-Host "`nPostgres seed'i (2M satir) arka planda calisiyor." -ForegroundColor Yellow
        Write-Host "Ilerlemeyi izlemek icin:  .\run.ps1 logs postgres" -ForegroundColor Yellow
    }

    "down" {
        docker compose down @Rest
    }

    "status" {
        Write-Host "`n=== Container durumu ===" -ForegroundColor Green
        docker compose ps

        Write-Host "`n=== Saglik kontrolleri ===" -ForegroundColor Green

        # Nessie
        try {
            $n = Invoke-RestMethod "http://localhost:19120/api/v2/config" -TimeoutSec 5
            Write-Host ("  [OK]   Nessie      -> varsayilan branch: {0}" -f $n.defaultBranch) -ForegroundColor Green
        } catch {
            Write-Host "  [HATA] Nessie      -> yanit yok (http://localhost:19120)" -ForegroundColor Red
        }

        # MinIO
        try {
            Invoke-WebRequest "http://localhost:9000/minio/health/live" -TimeoutSec 5 -UseBasicParsing | Out-Null
            Write-Host "  [OK]   MinIO       -> canli" -ForegroundColor Green
        } catch {
            Write-Host "  [HATA] MinIO       -> yanit yok (http://localhost:9000)" -ForegroundColor Red
        }

        # ClickHouse
        # Kimlik dogrulama SART: 'analytics' kullanicisi parolali. Basliksiz
        # istek 'default' kullanicisi olarak gider ve reddedilir -- saglikli
        # bir sunucuda bile YANLIS ALARM uretir. (Bu tuzaga dustuk.)
        try {
            $chHeaders = @{
                "X-ClickHouse-User" = "analytics"
                "X-ClickHouse-Key"  = "analytics_pass"
            }
            $c = Invoke-RestMethod "http://localhost:8123/?query=SELECT+version()" `
                    -Headers $chHeaders -TimeoutSec 5
            Write-Host ("  [OK]   ClickHouse  -> surum {0}" -f "$c".Trim()) -ForegroundColor Green
        } catch {
            Write-Host "  [HATA] ClickHouse  -> yanit yok (http://localhost:8123)" -ForegroundColor Red
        }

        # Spark
        try {
            Invoke-WebRequest "http://localhost:8080" -TimeoutSec 5 -UseBasicParsing | Out-Null
            Write-Host "  [OK]   Spark       -> master UI ayakta" -ForegroundColor Green
        } catch {
            Write-Host "  [HATA] Spark       -> yanit yok (http://localhost:8080)" -ForegroundColor Red
        }

        # Postgres seed
        try {
            $r = docker compose exec -T postgres psql -U csd -d csd_oltp -tAc `
                 "SELECT count(*) FROM csd.islem" 2>$null
            if ($r) {
                Write-Host ("  [OK]   Postgres    -> csd.islem: {0} satir" -f $r.Trim()) -ForegroundColor Green
            }
        } catch {
            Write-Host "  [BEKLE] Postgres   -> seed devam ediyor olabilir" -ForegroundColor Yellow
        }

        # Sema BUTUNLUGU -- satir saymak yetmez.
        # Seed yarida kesilirse satirlar yerinde durur ama indeksler,
        # v_islem_sinirlari ve ANALYZE istatistikleri eksik kalir; Postgres
        # yine de "healthy" der. Eksik indeks = benchmark'ta Postgres'i
        # haksiz yere yavas gostermek demektir. Onu burada yakaliyoruz.
        try {
            $eksik = docker compose exec -T postgres psql -U csd -d csd_oltp -tAc @"
SELECT (4 - (SELECT count(*) FROM pg_indexes WHERE schemaname='csd'
              AND indexname IN ('idx_islem_yatirimci','idx_islem_menkul',
                                'idx_islem_guncelleme','idx_bakiye_valor')))
     + (1 - (SELECT count(*) FROM pg_views WHERE schemaname='csd'
              AND viewname='v_islem_sinirlari'))
     + (SELECT count(*) FROM pg_stat_user_tables WHERE schemaname='csd'
              AND COALESCE(last_analyze, last_autoanalyze) IS NULL)
"@ 2>$null
            if ($eksik -and $eksik.Trim() -eq "0") {
                Write-Host "  [OK]   OLTP semasi -> indeks + view + istatistik tam" -ForegroundColor Green
            } elseif ($eksik) {
                Write-Host ("  [HATA] OLTP semasi -> {0} eksik nesne. Cozum: .\run.ps1 repair-oltp" -f $eksik.Trim()) -ForegroundColor Red
            }
        } catch { }
        Write-Host ""
    }

    "logs" {
        if ($Rest) { docker compose logs -f @Rest } else { docker compose logs -f }
    }

    "repair-oltp" {
        # Postgres init scriptleri SADECE veri dizini bossa calisir. Seed
        # yarida kesilirse (Docker cokmesi, makine kapanmasi) container
        # yeniden basladiginda init ATLANIR ve veritabani yarim kalir:
        # satirlar yerinde, indeksler/view/istatistikler EKSIK. "healthy"
        # gorunur ama degildir. Bu projede birebir yasandi.
        #
        # Bu komut idempotenttir: eksigi tamamlar, varsa dokunmaz.
        Write-Host "`n=== OLTP semasi onariliyor / dogrulaniyor ===" -ForegroundColor Green
        Write-Host "20M satirda indeks kurulumu birkac dakika surebilir.`n" -ForegroundColor Yellow
        docker compose exec -T postgres psql -U csd -d csd_oltp `
            -v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/99_repair.sql
    }

    "job" {
        if (-not $Rest) { Write-Host "Kullanim: .\run.ps1 job <script.py> [args]" -ForegroundColor Red; exit 1 }
        Invoke-SparkSubmit "/opt/spark/jobs/$($Rest[0])" (Get-TailArgs $Rest)
    }

    "test" {
        if (-not $Rest) { Write-Host "Kullanim: .\run.ps1 test <script.py>" -ForegroundColor Red; exit 1 }
        Invoke-SparkSubmit "/opt/spark/tests/$($Rest[0])" (Get-TailArgs $Rest)
    }

    "sql" {
        if (-not $Rest) { Write-Host "Kullanim: .\run.ps1 sql <dosya.sql>" -ForegroundColor Red; exit 1 }
        Invoke-ChSql "$PSScriptRoot\sql\clickhouse\$($Rest[0])"
    }

    "sqltest" {
        if (-not $Rest) { Write-Host "Kullanim: .\run.ps1 sqltest <dosya.sql>" -ForegroundColor Red; exit 1 }
        Invoke-ChSql "$PSScriptRoot\tests\$($Rest[0])"
    }

    "pipeline" {
        Write-Host "`n=== ETL Pipeline: OLTP -> Bronze -> Silver -> Gold ===`n" -ForegroundColor Green
        Invoke-SparkSubmit "/opt/spark/jobs/01_oltp_to_bronze.py" @("--full")
        if ($LASTEXITCODE -ne 0) { Write-Host "01 basarisiz - duruyorum." -ForegroundColor Red; exit 1 }
        Invoke-SparkSubmit "/opt/spark/jobs/02_bronze_to_silver.py" @()
        if ($LASTEXITCODE -ne 0) { Write-Host "02 basarisiz - duruyorum." -ForegroundColor Red; exit 1 }
        Invoke-SparkSubmit "/opt/spark/jobs/03_silver_to_gold.py" @()
        if ($LASTEXITCODE -ne 0) { Write-Host "03 basarisiz - duruyorum." -ForegroundColor Red; exit 1 }

        # Gorunumleri BURADA yeniliyoruz (tek kaynak: Invoke-RefreshViews).
        # ETL'i tek tek job olarak kosanlar 'refresh-views' komutunu elle
        # cagirmali; pipeline otomatik yapiyor.
        [void](Invoke-RefreshViews)

        Write-Host "`nPipeline tamam. Simdi:  .\run.ps1 sql 03_materialize_mergetree.sql" -ForegroundColor Green
    }

    "refresh-views" {
        # ETL'i tek tek kostuysaniz federe gorunumleri guncellemek icin.
        [void](Invoke-RefreshViews)
    }

    "verify-all" {
        # ---------------------------------------------------------------
        #  REGRESYON KOSUM TAKIMI -- her degisiklikten sonra calistirin.
        #
        #  Bu projedeki gercek hatalarin hepsi SESSIZDI: job'lar yesil
        #  bitiyor, rakam yanlis cikiyordu. Elle kontrol onlari kacirdi.
        #  Bu komut o kontrolleri saniyeler icinde ve SESLI yapar.
        #
        #  Kapsam: veri DOGRULUGU invaryantlari (mutabakat, net-sifir,
        #  bronze=silver+karantina, gorunum tazeligi). Performans BILEREK
        #  disarida -- makineye bagli esikler arada bir bosuna kirmizi
        #  yanar ve guvenilmeyen kontrol, bakilmayan kontrole donusur.
        #  Performans icin:  .\run.ps1 sqltest 03_clickhouse_perf.sql
        # ---------------------------------------------------------------
        Write-Host "`n=== Regresyon dogrulamasi ===" -ForegroundColor Green

        Get-Content "$PSScriptRoot\tests\07_dogrulama.sql" -Raw |
            docker compose exec -T clickhouse clickhouse-client `
                --user analytics --password analytics_pass --multiquery
        $rc = $LASTEXITCODE

        Write-Host ""
        if ($rc -eq 0) {
            Write-Host "  TUM KONTROLLER GECTI." -ForegroundColor Green
        } else {
            Write-Host "  REGRESYON VAR -- yukarida 'KALDI' isaretli satirlara bakin." -ForegroundColor Red
            Write-Host "  (clickhouse cikis kodu: $rc)" -ForegroundColor Red
        }
        Write-Host ""
        exit $rc
    }

    "shell" {
        docker compose exec spark-master /opt/spark/bin/spark-sql `
            --master "spark://spark-master:7077" `
            --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" `
            --conf "spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog" `
            --conf "spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" `
            --conf "spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2" `
            --conf "spark.sql.catalog.nessie.client-api-version=2" `
            --conf "spark.sql.catalog.nessie.ref=main" `
            --conf "spark.sql.catalog.nessie.authentication.type=NONE" `
            --conf "spark.sql.catalog.nessie.warehouse=s3://lakehouse/warehouse" `
            --conf "spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO" `
            --conf "spark.sql.catalog.nessie.s3.endpoint=http://minio:9000" `
            --conf "spark.sql.catalog.nessie.s3.path-style-access=true" `
            --conf "spark.sql.defaultCatalog=nessie"
    }

    "gc" {
        # Nessie GC -- Iceberg'in expire_snapshots'inin NESSIE'DEKI KARSILIGI.
        # Neden expire_snapshots kullanilamadigini jobs/99_maintenance.py
        # icindeki [4/4] notu anlatiyor (kisaca: gc.enabled=false, cunku ayni
        # dosyalari baska branch'ler referans ediyor olabilir).
        #
        # Kullanim:  .\run.ps1 gc [saklama_saati]     ornek: .\run.ps1 gc 168
        $hours = if ($Rest -and $Rest[0]) { $Rest[0] } else { "168" }   # 7 gun

        $ver = (Select-String -Path ".\.env" -Pattern '^NESSIE_VERSION=(.+)$').Matches.Groups[1].Value.Trim()
        $jar = "tools\nessie-gc-$ver.jar"

        if (-not (Test-Path $jar)) {
            New-Item -ItemType Directory -Force tools | Out-Null
            $url = "https://github.com/projectnessie/nessie/releases/download/nessie-$ver/nessie-gc-$ver.jar"
            Write-Host "nessie-gc-$ver.jar indiriliyor (~126MB, bir kez)..." -ForegroundColor Cyan
            Invoke-WebRequest -Uri $url -OutFile $jar
        }

        Write-Host "`nUYARI: Nessie GC'nin --dry-run modu YOKTUR." -ForegroundColor Yellow
        Write-Host "Referansi kalmamis dosyalar GERCEKTEN SILINECEK." -ForegroundColor Yellow
        Write-Host "Saklama penceresi: son $hours saat (= time travel pencereniz).`n" -ForegroundColor Yellow

        docker cp $jar lh-spark-master:/tmp/nessie-gc.jar | Out-Null

        $gcArgs = @(
            "compose", "exec", "-T", "spark-master",
            "java", "-jar", "/tmp/nessie-gc.jar", "gc",
            "--uri", "http://nessie:19120/api/v2",
            "--inmemory",
            "--default-cutoff", "PT${hours}H",
            "--iceberg", "s3.endpoint=http://minio:9000",
            "--iceberg", "s3.path-style-access=true",
            "--iceberg", "s3.access-key-id=minioadmin",
            "--iceberg", "s3.secret-access-key=minioadmin123",
            "--iceberg", "client.region=us-east-1"
        )
        $out = & docker @gcArgs
        $out | Select-String -Pattern "considered expired|finished with status|deleted \d+ files" |
            ForEach-Object { Write-Host ("  " + $_) }
    }

    "bench" {
        # Es zamanli yuk testi. Sunumun en guclu rakamini uretir: P99.
        # Kullanim:  .\run.ps1 bench [eszamanlilik] [iterasyon]
        $conc = if ($Rest -and $Rest[0]) { $Rest[0] } else { "50" }
        $iter = if ($Rest -and $Rest[1]) { $Rest[1] } else { "300" }

        $q = "SELECT kanal, sum(toplam_hacim) FROM {0} WHERE kiymet_tipi='HISSE' AND islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31' GROUP BY kanal"

        $senaryolar = @(
            @{ Ad = "Iceberg federe (lake.gold_gunluk_menkul_ozet)"; Tablo = "lake.gold_gunluk_menkul_ozet" },
            @{ Ad = "MergeTree     (csd.gunluk_menkul_ozet)";        Tablo = "csd.gunluk_menkul_ozet" }
        )

        Write-Host "`n=== Es zamanli yuk testi: $conc kullanici, $iter sorgu ===" -ForegroundColor Green

        foreach ($s in $senaryolar) {
            Write-Host "`n--- $($s.Ad) ---" -ForegroundColor Cyan
            $sql = $q -f $s.Tablo

            # ---- IKI AYRI WINDOWS/PS TUZAGI VAR, IKISINI DE ATLIYORUZ ----
            #
            # TUZAK 1 - tirnak bolunmesi:
            #   'bash -c "echo ... | clickhouse-benchmark ..."' seklinde
            #   kurulan komutu PS 5.1 bosluklardan boler; bash '-c echo'
            #   alir, SQL konumsal parametre olur, benchmark BOS girdiyle
            #   calisir. Belirti: hata yok, cikti bos.
            #   Cozum: SQL'i ORTAM DEGISKENIYLE (-e SQL=...) geciriyoruz.
            #   bash komutu TEK TIRNAKLI oldugu icin PS ona dokunmuyor;
            #   $SQL'i bash'in kendisi genisletiyor.
            #
            # TUZAK 2 - NativeCommandError:
            #   clickhouse-benchmark istatistikleri STDERR'e yazar. PS 5.1'de
            #   native bir komutun stderr'ini '2>&1' ile PS tarafinda
            #   birlestirmek her satiri ErrorRecord'a cevirir; $ErrorActionPreference
            #   = 'Stop' oldugu icin script BURADA DURUR.
            #   Cozum: '2>&1'i BASH ICINDE yapiyoruz. PS stderr'i hic gormuyor.
            $bashCmd = 'echo "$SQL" | clickhouse-benchmark' +
                       ' --user analytics --password analytics_pass' +
                       " --concurrency $conc --iterations $iter" +
                       ' 2>&1'
            $benchArgs = @(
                "compose", "exec", "-T",
                "-e", "SQL=$sql",
                "clickhouse", "bash", "-c", $bashCmd
            )
            $out = & docker @benchArgs

            $qps = $out | Select-String -Pattern "QPS:" | Select-Object -Last 1
            if ($qps) { Write-Host ("  " + ($qps -replace '^\s+', '')) }

            # DIKKAT -- clickhouse-benchmark ARA RAPOR basar (varsayilan ~1sn'de
            # bir) ve her raporda yuzdelik blogunu TEKRAR yazar. Uzun suren
            # senaryo 5 blok, kisa suren 1 blok uretir; hepsini basmak
            # "Iceberg neden 5 kez P99 yazdi?" diye okunamaz bir cikti verir
            # ve yanlis satiri raporlama riski dogurur.
            # Bizi ilgilendiren SON (kumulatif) bloktur -> son 5 satir.
            Write-Host "  gecikme yuzdelikleri (son kumulatif rapor):"
            $tumYuzdelikler = @($out | Select-String -Pattern "^\s*(50|90|95|99|99\.9)%")
            $sonBlok = if ($tumYuzdelikler.Count -ge 5) {
                $tumYuzdelikler[($tumYuzdelikler.Count - 5)..($tumYuzdelikler.Count - 1)]
            } else { $tumYuzdelikler }
            $sonBlok | ForEach-Object {
                Write-Host ("    " + ($_ -replace '\s+', ' ').Trim())
            }
        }

        Write-Host "`nSunumda ORTALAMA degil P99 kullanin." -ForegroundColor Yellow
        Write-Host "'50 es zamanli kullanicida sorgularin %99'u X ms altinda' bir TAAHHUTTUR.`n" -ForegroundColor Yellow
    }

    "chshell" {
        docker compose exec clickhouse clickhouse-client --user analytics --password analytics_pass
    }

    "pgshell" {
        docker compose exec postgres psql -U csd -d csd_oltp
    }

    default {
        Write-Host @"

  CSD Data Lakehouse - komutlar

    .\run.ps1 build              Spark imajini derle (ilk kurulum)
    .\run.ps1 up                 Stack'i baslat
    .\run.ps1 down [-v]          Durdur (-v: volume'lari da sil)
    .\run.ps1 status             Tum servislerin saglik kontrolu
    .\run.ps1 verify-all         REGRESYON kontrolu (her degisiklikten sonra)
    .\run.ps1 logs [servis]      Log takibi
    .\run.ps1 repair-oltp        Yarim kalmis OLTP semasini tamamla

    .\run.ps1 pipeline           ETL'i bastan sona calistir (01->02->03 + gorunumler)
    .\run.ps1 job <script> [..]  Tek job calistir
    .\run.ps1 refresh-views      lake.* federe gorunumlerini yeniden uret
    .\run.ps1 test <script>      Test senaryosu calistir
    .\run.ps1 sql <dosya>        ClickHouse SQL dosyasi calistir
    .\run.ps1 sqltest <dosya>    tests/ altindaki SQL'i calistir

    .\run.ps1 shell              Spark SQL kabugu (Nessie katalogu bagli)
    .\run.ps1 chshell            ClickHouse istemcisi
    .\run.ps1 pgshell            Postgres istemcisi

  Ilk kurulum sirasi:
    .\run.ps1 build
    .\run.ps1 up
    .\run.ps1 status             (Postgres seed bitene kadar bekleyin)
    .\run.ps1 pipeline
    .\run.ps1 sql 01_catalog.sql
    .\run.ps1 sql 03_materialize_mergetree.sql

  Sunum sirasi (yonetici demosu):
    .\run.ps1 test 02_nessie_branching.py     Bozuk veri engelleniyor
    .\run.ps1 test 01_time_travel.py          Hatadan saniyeler icinde donus
    .\run.ps1 test 04_schema_evolution.py     Mevzuat degisikligi aninda
    .\run.ps1 sqltest 03_clickhouse_perf.sql  ~1541x performans (20M satir)
    .\run.ps1 sqltest 05_mutabakat.sql        10,4M pozisyon, 0 fark
    .\run.ps1 bench                           50 kullanici, P99 karsilastirmasi

  Bilinen sinir (bilerek belgelendi):
    .\run.ps1 test 06_clickhouse_branch_izolasyonu.py
      Federe gorunumler (lake.*) Nessie branch izolasyonunu GORMEZ.
      Testi calistirdiktan sonra ClickHouse'ta su sorguyu kendiniz atin:
        SELECT count() FROM lake.gold_araci_kurum_gunluk;
      Ayrinti: docs/01-mimari.md bolum 2.

"@ -ForegroundColor Cyan
    }
}
