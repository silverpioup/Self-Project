param(
    [double]$ScaleFactor = 0.01
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Jar = Join-Path $ProjectRoot "target\flink-continuous-tpch-q3-1.0.0.jar"
$MavenPath = "E:\Coding\apache-maven-3.9.16-bin\apache-maven-3.9.16\bin\mvn.cmd"
$JavaHome = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

if (Test-Path $JavaHome) {
    $env:JAVA_HOME = $JavaHome
    $env:Path = (Join-Path $JavaHome "bin") + ";" + $env:Path
}

$Maven = if (Test-Path $MavenPath) { $MavenPath } else { "mvn" }
$Java = if (Test-Path (Join-Path $JavaHome "bin\java.exe")) {
    Join-Path $JavaHome "bin\java.exe"
} else {
    "java"
}
$ScaleName = ("{0}" -f $ScaleFactor).Replace(".", "")
$DataDir = Join-Path $ProjectRoot "data\tpch_sf$ScaleName"
$UpdateFile = Join-Path $DataDir "updates.csv"
$ResultsDir = Join-Path $ProjectRoot "results"

Push-Location $ProjectRoot
try {
    & $Maven -q clean package
    if ($LASTEXITCODE -ne 0) {
        throw "Maven build failed."
    }

    python .\scripts\generate_tpch_sf.py `
        --scale-factor $ScaleFactor `
        --output-dir $DataDir
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H data generation failed. Install requirements-experiments.txt first."
    }

    python .\scripts\convert_tpch_tbl_to_updates.py `
        --customer (Join-Path $DataDir "customer.tbl") `
        --orders (Join-Path $DataDir "orders.tbl") `
        --lineitem (Join-Path $DataDir "lineitem.tbl") `
        --output $UpdateFile
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H conversion failed."
    }

    $UpdateCount = (Get-Content -LiteralPath $UpdateFile | Where-Object {
        $Trimmed = $_.Trim()
        $Trimmed.Length -gt 0 -and -not $Trimmed.StartsWith("#")
    }).Count

    $Rows = foreach ($Parallelism in 1, 2, 4, 8) {
        $Timer = [System.Diagnostics.Stopwatch]::StartNew()
        $RunOutput = & $Java --add-opens=java.base/java.util=ALL-UNNAMED `
            -jar $Jar $UpdateFile $Parallelism metrics 2>&1
        $Timer.Stop()
        if ($LASTEXITCODE -ne 0) {
            $RunOutput | Write-Host
            throw "TPC-H run failed at parallelism $Parallelism."
        }

        $MetricsLine = $RunOutput | Where-Object { $_ -match "^METRICS\|" } |
            Select-Object -Last 1
        if (-not $MetricsLine) {
            throw "No latency metrics returned at parallelism $Parallelism."
        }
        $Metrics = $MetricsLine -split "\|"
        $Seconds = [Math]::Max($Timer.Elapsed.TotalSeconds, 0.001)
        [PSCustomObject]@{
            scale_factor = $ScaleFactor
            updates = $UpdateCount
            parallelism = $Parallelism
            wall_clock_seconds = [Math]::Round($Seconds, 3)
            throughput_updates_per_second = [Math]::Round($UpdateCount / $Seconds, 2)
            mean_latency_us = [Math]::Round([double]$Metrics[2], 2)
            p50_latency_us = [int64]$Metrics[3]
            p95_latency_us = [int64]$Metrics[4]
            p99_latency_us = [int64]$Metrics[5]
        }
    }

    $BenchmarkCsv = Join-Path $ResultsDir "tpch_sf_results.csv"
    $Rows | Export-Csv -NoTypeInformation -Path $BenchmarkCsv
    $Rows | Format-Table -AutoSize

    python .\scripts\compare_flink_sqlite.py `
        $UpdateFile `
        --parallelism 8 `
        --snapshot-every 5000 `
        --java $Java `
        --output (Join-Path $ResultsDir "correctness_tpch_sf_p8.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H correctness comparison failed."
    }

    Write-Host "Saved TPC-H experiment results to $BenchmarkCsv"
} finally {
    Pop-Location
}
