. "$PSScriptRoot\common.ps1"

Build-Q3Project
Invoke-Q3Job `
    -InputFile (Join-Path $script:ProjectRoot "data\sample_updates.csv") `
    -Parallelism 4 `
    -OutputMode print
