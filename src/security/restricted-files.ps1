$ErrorActionPreference = 'Stop'
try {
    $serviceSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $trusted = @($serviceSid, 'S-1-5-18', 'S-1-5-32-544')
    $ancestorOwners = $trusted + 'S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464'
    function Assert-Descriptor($descriptor, $sensitive) {
        $owner = $descriptor.GetOwner([Security.Principal.SecurityIdentifier]).Value
        if ($sensitive) {
            if ($owner -ne $serviceSid) { throw 'owner' }
        } elseif ($owner -notin $ancestorOwners) { throw 'owner' }
        $raw = [Security.AccessControl.RawSecurityDescriptor]::new($descriptor.GetSecurityDescriptorBinaryForm(), 0)
        if ($null -eq $raw.DiscretionaryAcl) { throw 'null DACL' }
        foreach ($rule in $descriptor.GetAccessRules($true, $true, [Security.Principal.SecurityIdentifier])) {
            if ($rule.AccessControlType -ne [Security.AccessControl.AccessControlType]::Allow) { continue }
            if (($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -ne 0) { continue }
            if ($rule.IdentityReference.Value -in $trusted) { continue }
            $mask = [int64]$rule.FileSystemRights -band 0xffffffffL
            # An ancestor may let another principal create a sibling directory;
            # it must not let that principal rename, remove, or alter this path.
            if ($sensitive -or ($mask -band 0x500D0152L) -ne 0) { throw 'access' }
        }
    }
    $directory = [IO.Path]::GetFullPath($env:SIDER_SECURITY_READ_DIR)
    $ancestors = [Collections.Generic.List[string]]::new()
    $current = [IO.DirectoryInfo]::new($directory)
    while ($null -ne $current) { $ancestors.Add($current.FullName); $current = $current.Parent }
    $ancestors.Reverse()
    foreach ($ancestor in $ancestors) {
        $attributes = [IO.File]::GetAttributes($ancestor)
        if (($attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0 -or ($attributes -band [IO.FileAttributes]::Directory) -eq 0) { throw 'type' }
        Assert-Descriptor (Get-Acl -LiteralPath $ancestor) ($ancestor -eq $directory)
    }
    function Read-Restricted($name) {
        $path = [IO.Path]::Combine($directory, $name)
        $attributes = [IO.File]::GetAttributes($path)
        if (($attributes -band ([IO.FileAttributes]::Directory -bor [IO.FileAttributes]::ReparsePoint)) -ne 0) { throw 'type' }
        $file = [IO.FileStream]::new($path, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::Read)
        try {
            Assert-Descriptor ($file.GetAccessControl()) $true
            if ($file.Length -eq 0 -or $file.Length -gt 4194304) { throw 'size' }
            $buffer = [byte[]]::new([int]$file.Length)
            $position = 0
            while ($position -lt $buffer.Length) {
                $read = $file.Read($buffer, $position, $buffer.Length - $position)
                if ($read -eq 0) { throw 'truncated' }
                $position += $read
            }
            return [Convert]::ToBase64String($buffer)
        } finally { $file.Dispose() }
    }
    $result = [ordered]@{}
    $result['security-manifest.json'] = Read-Restricted 'security-manifest.json'
    foreach ($name in $env:SIDER_SECURITY_READ_FILES.Split(',')) { $result[$name] = Read-Restricted $name }
    $result['manifest-after'] = Read-Restricted 'security-manifest.json'
    [Console]::Out.Write(($result | ConvertTo-Json -Compress))
} catch { exit 1 }
