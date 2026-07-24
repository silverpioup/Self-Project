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

Push-Location $ProjectRoot
try {
    & $Maven -q clean package
    if ($LASTEXITCODE -ne 0) {
        throw "Maven build failed."
    }

    foreach ($Parallelism in 1, 2, 4, 8) {
        python .\scripts\compare_flink_sqlite.py `
            .\data\sample_updates.csv `
            --parallelism $Parallelism `
            --snapshot-every 1 `
            --java $Java `
            --output ".\results\correctness_sample_p$Parallelism.csv"
        if ($LASTEXITCODE -ne 0) {
            throw "Sample correctness check failed at parallelism $Parallelism."
        }
    }

    $LargeInput = Join-Path $ProjectRoot "data\benchmark\updates_large.csv"
    if (Test-Path $LargeInput) {
        python .\scripts\compare_flink_sqlite.py `
            $LargeInput `
            --parallelism 8 `
            --snapshot-every 10000 `
            --java $Java `
            --output ".\results\correctness_large_p8.csv"
        if ($LASTEXITCODE -ne 0) {
            throw "Large-stream correctness check failed."
        }
    } else {
        Write-Host "Large benchmark stream not found; run scripts\run_benchmark.ps1 first."
    }
} finally {
    Pop-Location
}
