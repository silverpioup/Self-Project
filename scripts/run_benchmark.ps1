$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Jar = Join-Path $ProjectRoot "target\flink-continuous-tpch-q3-1.0.0.jar"
$MavenPath = "E:\Coding\apache-maven-3.9.16-bin\apache-maven-3.9.16\bin\mvn.cmd"
$JavaHome = "C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot"

if (Test-Path $JavaHome) {
    $env:JAVA_HOME = $JavaHome
    $env:Path = (Join-Path $JavaHome "bin") + ";" + $env:Path
}

if (Test-Path $MavenPath) {
    $Maven = $MavenPath
} else {
    $Maven = "mvn"
}

Push-Location $ProjectRoot
try {
    & $Maven -q clean package
    if ($LASTEXITCODE -ne 0) {
        throw "Maven build failed."
    }

    $DataDir = Join-Path $ProjectRoot "data\benchmark"
    $ResultsDir = Join-Path $ProjectRoot "results"
    New-Item -ItemType Directory -Force -Path $DataDir, $ResultsDir | Out-Null

    $Datasets = @(
        @{ Name = "small"; Orders = 1000; LineItems = 3 },
        @{ Name = "medium"; Orders = 10000; LineItems = 3 },
        @{ Name = "large"; Orders = 30000; LineItems = 3 }
    )

    foreach ($Dataset in $Datasets) {
        $InputFile = Join-Path $DataDir ("updates_{0}.csv" -f $Dataset.Name)
        python .\scripts\generate_benchmark_updates.py `
            --orders $Dataset.Orders `
            --lineitems-per-order $Dataset.LineItems `
            --output $InputFile | Out-Null
    }

    $Rows = foreach ($Dataset in $Datasets) {
        $InputFile = Join-Path $DataDir ("updates_{0}.csv" -f $Dataset.Name)
        $UpdateCount = (Get-Content -LiteralPath $InputFile | Where-Object {
            $Trimmed = $_.Trim()
            $Trimmed.Length -gt 0 -and -not $Trimmed.StartsWith("#")
        }).Count

        foreach ($Parallelism in 1, 2, 4, 8) {
            $Timer = [System.Diagnostics.Stopwatch]::StartNew()
            $RunOutput = & java --add-opens=java.base/java.util=ALL-UNNAMED `
                -jar $Jar `
                $InputFile `
                $Parallelism `
                metrics 2>&1
            $Timer.Stop()

            if ($LASTEXITCODE -ne 0) {
                $RunOutput | Write-Host
                throw "Benchmark run failed for $($Dataset.Name), parallelism $Parallelism."
            }

            $MetricsLine = $RunOutput | Where-Object { $_ -match "^METRICS\|" } |
                Select-Object -Last 1
            if (-not $MetricsLine) {
                throw "No latency metrics returned for $($Dataset.Name), parallelism $Parallelism."
            }
            $Metrics = $MetricsLine -split "\|"
            if ([int64]$Metrics[1] -ne $UpdateCount) {
                throw "Latency sample count does not match update count."
            }

            $Seconds = [Math]::Max($Timer.Elapsed.TotalSeconds, 0.001)
            [PSCustomObject]@{
                dataset = $Dataset.Name
                orders = $Dataset.Orders
                lineitems_per_order = $Dataset.LineItems
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
    }

    $OutputCsv = Join-Path $ResultsDir "benchmark_results.csv"
    $Rows | Export-Csv -NoTypeInformation -Path $OutputCsv
    $Rows | Format-Table -AutoSize
    Write-Host "Saved benchmark results to $OutputCsv"
} finally {
    Pop-Location
}
