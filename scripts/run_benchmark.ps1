param(
    [int]$WarmupRuns = 1,
    [int]$Repetitions = 3
)

. "$PSScriptRoot\common.ps1"

Build-Q3Project
Push-Location $script:ProjectRoot
try {
    $DataDir = ".\data\benchmark"
    $ResultsDir = ".\results"
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
            --output $InputFile

        python .\scripts\benchmark_q3.py `
            --input $InputFile `
            --jar $script:JarPath `
            --java $script:JavaCommand `
            --dataset $Dataset.Name `
            --warmup-runs $WarmupRuns `
            --repetitions $Repetitions `
            --raw-output (Join-Path $ResultsDir "benchmark_$($Dataset.Name)_runs.csv") `
            --summary-output (Join-Path $ResultsDir "benchmark_$($Dataset.Name)_summary.csv") `
            --environment-output (Join-Path $ResultsDir "experiment_environment.txt")
        if ($LASTEXITCODE -ne 0) {
            throw "Benchmark failed for $($Dataset.Name)."
        }
    }
} finally {
    Pop-Location
}
