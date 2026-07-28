$ErrorActionPreference = "Stop"

$script:ProjectRoot = Split-Path -Parent $PSScriptRoot

function Resolve-JavaCommand {
    if ($env:JAVA_HOME) {
        $candidate = Join-Path $env:JAVA_HOME "bin\java.exe"
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    $command = Get-Command java -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "Java was not found. Install JDK 17 and set JAVA_HOME."
    }
    return $command.Source
}

function Resolve-MavenCommand {
    if ($env:MAVEN_HOME) {
        $candidate = Join-Path $env:MAVEN_HOME "bin\mvn.cmd"
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    $command = Get-Command mvn -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }
    throw "Maven was not found. Install Maven 3.9+ and add its bin directory to Path."
}

$script:JavaCommand = Resolve-JavaCommand
$script:MavenCommand = Resolve-MavenCommand
$script:JarPath = Join-Path $script:ProjectRoot `
    "target\flink-continuous-tpch-q3-1.0.0.jar"

function Build-Q3Project {
    Push-Location $script:ProjectRoot
    try {
        & $script:MavenCommand -q clean verify
        if ($LASTEXITCODE -ne 0) {
            throw "Maven build or tests failed."
        }
    } finally {
        Pop-Location
    }
}

function Invoke-Q3Job {
    param(
        [Parameter(Mandatory = $true)][string]$InputFile,
        [int]$Parallelism = 1,
        [ValidateSet("print", "quiet", "metrics", "both")]
        [string]$OutputMode = "print",
        [long]$CheckpointIntervalMs = 0
    )
    & $script:JavaCommand `
        --add-opens=java.base/java.util=ALL-UNNAMED `
        -jar $script:JarPath `
        $InputFile `
        $Parallelism `
        $OutputMode `
        $CheckpointIntervalMs
    if ($LASTEXITCODE -ne 0) {
        throw "Flink Q3 job failed."
    }
}
