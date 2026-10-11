$ErrorActionPreference = 'Stop'
try {
    Add-Type -Language CSharp -TypeDefinition @'
using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

public static class SiderRestrictedHandle {
    [StructLayout(LayoutKind.Sequential)]
    private struct FileTime { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    private struct FileInformation {
        public uint Attributes;
        public FileTime Created;
        public FileTime Accessed;
        public FileTime Written;
        public uint Volume;
        public uint SizeHigh;
        public uint SizeLow;
        public uint Links;
        public uint IndexHigh;
        public uint IndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(
        string path, uint access, uint share, IntPtr security, uint disposition,
        uint flags, IntPtr template);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(
        SafeFileHandle handle, out FileInformation information);

    [DllImport("advapi32.dll")]
    private static extern uint GetSecurityInfo(
        SafeFileHandle handle, uint objectType, uint requested,
        out IntPtr owner, out IntPtr group, out IntPtr dacl, out IntPtr sacl,
        out IntPtr descriptor);

    [DllImport("advapi32.dll")]
    private static extern uint GetSecurityDescriptorLength(IntPtr descriptor);

    [DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr memory);

    public static SafeFileHandle Open(string path, bool directory) {
        const uint genericRead = 0x80000000;
        const uint shareReadWrite = 0x00000003;
        const uint openExisting = 3;
        const uint openReparsePoint = 0x00200000;
        const uint backupSemantics = 0x02000000;
        SafeFileHandle handle = CreateFileW(
            path, genericRead, shareReadWrite, IntPtr.Zero, openExisting,
            openReparsePoint | (directory ? backupSemantics : 0), IntPtr.Zero);
        if (handle.IsInvalid) {
            int error = Marshal.GetLastWin32Error();
            handle.Dispose();
            throw new Win32Exception(error);
        }
        try {
            FileInformation information;
            if (!GetFileInformationByHandle(handle, out information))
                throw new Win32Exception(Marshal.GetLastWin32Error());
            bool isDirectory = (information.Attributes & 0x10) != 0;
            if ((information.Attributes & 0x400) != 0 || isDirectory != directory)
                throw new IOException("type");
            return handle;
        } catch {
            handle.Dispose();
            throw;
        }
    }

    public static byte[] Descriptor(SafeFileHandle handle) {
        IntPtr owner, group, dacl, sacl, descriptor;
        uint error = GetSecurityInfo(handle, 1, 0x5, out owner, out group,
            out dacl, out sacl, out descriptor);
        if (error != 0) throw new Win32Exception((int)error);
        try {
            uint length = GetSecurityDescriptorLength(descriptor);
            if (length == 0 || length > 65536) throw new IOException("descriptor size");
            byte[] bytes = new byte[length];
            Marshal.Copy(descriptor, bytes, 0, (int)length);
            return bytes;
        } finally {
            LocalFree(descriptor);
        }
    }
}
'@
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
    $openedAncestors = [Collections.Generic.List[Microsoft.Win32.SafeHandles.SafeFileHandle]]::new()
    try {
        foreach ($ancestor in $ancestors) {
            $handle = [SiderRestrictedHandle]::Open($ancestor, $true)
            $openedAncestors.Add($handle)
            $descriptor = [Security.AccessControl.DirectorySecurity]::new()
            $descriptor.SetSecurityDescriptorBinaryForm([SiderRestrictedHandle]::Descriptor($handle))
            Assert-Descriptor $descriptor ($ancestor -eq $directory)
        }
        function Read-Restricted($name) {
            $path = [IO.Path]::Combine($directory, $name)
            $handle = [SiderRestrictedHandle]::Open($path, $false)
            try {
                $file = [IO.FileStream]::new($handle, [IO.FileAccess]::Read, 4096, $false)
                try {
                    $descriptor = [Security.AccessControl.FileSecurity]::new()
                    $descriptor.SetSecurityDescriptorBinaryForm([SiderRestrictedHandle]::Descriptor($file.SafeFileHandle))
                    Assert-Descriptor $descriptor $true
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
            } finally { $handle.Dispose() }
        }
        $result = [ordered]@{}
        $result['security-manifest.json'] = Read-Restricted 'security-manifest.json'
        foreach ($name in $env:SIDER_SECURITY_READ_FILES.Split(',')) { $result[$name] = Read-Restricted $name }
        $result['manifest-after'] = Read-Restricted 'security-manifest.json'
        [Console]::Out.Write(($result | ConvertTo-Json -Compress))
    } finally {
        foreach ($handle in $openedAncestors) { $handle.Dispose() }
    }
} catch { exit 1 }
