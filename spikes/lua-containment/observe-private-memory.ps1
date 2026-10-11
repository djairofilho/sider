$ErrorActionPreference = 'Stop'
# Diagnostic observer only. The caller sends the PID of its newly created helper.
while ($null -ne ($line = [Console]::ReadLine())) {
    $helperId = [int]::Parse($line)
    $helper = [System.Diagnostics.Process]::GetProcessById($helperId)
    try {
        $helper.Refresh()
        [Console]::WriteLine("PRIVATE bytes=$($helper.PrivateMemorySize64) working_set=$($helper.WorkingSet64)")
        [Console]::Out.Flush()
    } finally {
        $helper.Dispose()
    }
}
