. "$PSScriptRoot\common.ps1"

Build-Q3Project
Push-Location $script:ProjectRoot
try {
    foreach ($Parallelism in 1, 2, 4, 8) {
        python .\scripts\compare_flink_sqlite.py `
            .\data\sample_updates.csv `
            --parallelism $Parallelism `
            --snapshot-every 1 `
            --java $script:JavaCommand `
            --output ".\results\correctness_sample_p$Parallelism.csv"
        if ($LASTEXITCODE -ne 0) {
            throw "Sample correctness failed at parallelism $Parallelism."
        }
    }

    $FifoFixture = ".\data\tpch_fixture\fifo_updates.csv"
    python .\scripts\generate_tpch_fifo_updates.py `
        --customer .\data\tpch_fixture\customer.tbl `
        --orders .\data\tpch_fixture\orders.tbl `
        --lineitem .\data\tpch_fixture\lineitem.tbl `
        --output $FifoFixture `
        --warmup-fraction 0.5 `
        --seed 1
    python .\scripts\compare_flink_sqlite.py `
        $FifoFixture `
        --parallelism 4 `
        --snapshot-every 1 `
        --java $script:JavaCommand `
        --output .\results\correctness_tpch_fixture_p4.csv
    if ($LASTEXITCODE -ne 0) {
        throw "TPC-H FIFO fixture correctness failed."
    }

    $LargeInput = ".\data\benchmark\updates_large.csv"
    python .\scripts\generate_benchmark_updates.py `
        --orders 30000 `
        --lineitems-per-order 3 `
        --output $LargeInput
    python .\scripts\compare_flink_sqlite.py `
        $LargeInput `
        --parallelism 8 `
        --snapshot-every 10000 `
        --java $script:JavaCommand `
        --output .\results\correctness_large_p8.csv
    if ($LASTEXITCODE -ne 0) {
        throw "Large-stream correctness failed."
    }
} finally {
    Pop-Location
}
