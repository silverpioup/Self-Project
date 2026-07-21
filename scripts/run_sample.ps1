$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$jarPath = Join-Path $projectRoot "target\flink-continuous-tpch-q3-1.0.0.jar"
$samplePath = Join-Path $projectRoot "data\sample_updates.csv"

if (-not (Test-Path -LiteralPath $jarPath)) {
    Write-Host "Jar not found. Building project first..."
    mvn clean package
}

java --add-opens=java.base/java.util=ALL-UNNAMED -jar $jarPath $samplePath
