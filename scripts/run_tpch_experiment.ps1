param(
    [double]$ScaleFactor = 0.1,
    [int]$WarmupRuns = 1,
    [int]$Repetitions = 3
)

. "$PSScriptRoot\common.ps1"

Build-Q3Project
$ScaleName = ("{0}" -f $ScaleFactor).Replace(".", "")
$DataDir = Join-Path $script:ProjectRoot "data\tpch_sf$ScaleName"
$UpdateFile = Join-Path $DataDir "fifo_updates.csv"
$ResultsDir = Join-Path $script:ProjectRoot "results"

Push-Location $script:ProjectRoot
try {
    python .\scripts\generate_tpch_sf.py `
        --scale-factor $ScaleFactor `
        --output-dir $DataDir
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H generation failed. Install requirements-experiments.txt."
    }

    python .\scripts\generate_tpch_fifo_updates.py `
        --customer (Join-Path $DataDir "customer.tbl") `
        --orders (Join-Path $DataDir "orders.tbl") `
        --lineitem (Join-Path $DataDir "lineitem.tbl") `
        --output $UpdateFile `
        --warmup-fraction 0.5 `
        --seed 6910
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H FIFO stream generation failed."
    }

    $UpdateCount = (Get-Content -LiteralPath $UpdateFile | Where-Object {
        $Trimmed = $_.Trim()
        $Trimmed.Length -gt 0 -and -not $Trimmed.StartsWith("#")
    }).Count
    $SnapshotEvery = [Math]::Max(1, [Math]::Floor($UpdateCount / 10))

    python .\scripts\compare_flink_sqlite.py `
        $UpdateFile `
        --parallelism 8 `
        --snapshot-every $SnapshotEvery `
        --java $script:JavaCommand `
        --output (Join-Path $ResultsDir "correctness_tpch_sf$ScaleName`_p8.csv")
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H exact correctness comparison failed."
    }

    python .\scripts\benchmark_q3.py `
        --input $UpdateFile `
        --jar $script:JarPath `
        --java $script:JavaCommand `
        --dataset "tpch_sf$ScaleName`_fifo" `
        --warmup-runs $WarmupRuns `
        --repetitions $Repetitions `
        --raw-output (Join-Path $ResultsDir "tpch_sf$ScaleName`_runs.csv") `
        --summary-output (Join-Path $ResultsDir "tpch_sf$ScaleName`_summary.csv") `
        --environment-output (Join-Path $ResultsDir "experiment_environment.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H repeated benchmark failed."
    }
} finally {
    Pop-Location
}
