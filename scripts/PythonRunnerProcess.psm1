Set-StrictMode -Version Latest
if(-not ('DepositPythonOwnedProcess' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;

// The job is owned by the runner, never inherited. Assignment precedes the
// first instruction in the child, so even an immediately exiting parent cannot
// leave descendants outside our ownership. No breakaway/fallback is permitted.
public class DepositPythonOwnedProcess : IDisposable {
    [StructLayout(LayoutKind.Sequential)] private struct SecurityAttributes {
        public int Length; public IntPtr Descriptor; public int Inherit;
    }
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] private struct StartupInfo {
        public int Size; public string Reserved; public string Desktop; public string Title;
        public int X, Y, XSize, YSize, XCount, YCount, Fill, Flags;
        public short ShowWindow, ReservedSize; public IntPtr ReservedBytes, Input, Output, Error;
    }
    [StructLayout(LayoutKind.Sequential)] private struct StartupInfoEx {
        public StartupInfo Info; public IntPtr Attributes;
    }
    [StructLayout(LayoutKind.Sequential)] private struct ProcessInfo {
        public IntPtr Process, Thread; public int ProcessId, ThreadId;
    }
    [StructLayout(LayoutKind.Sequential)] private struct BasicLimits {
        public long ProcessTime, JobTime; public uint Flags;
        public UIntPtr MinWorkingSet, MaxWorkingSet; public uint ActiveLimit;
        public UIntPtr Affinity; public uint Priority, Scheduling;
    }
    [StructLayout(LayoutKind.Sequential)] private struct IoCounters {
        public ulong ReadCount, WriteCount, OtherCount, ReadBytes, WriteBytes, OtherBytes;
    }
    [StructLayout(LayoutKind.Sequential)] private struct ExtendedLimits {
        public BasicLimits Basic; public IoCounters Io;
        public UIntPtr ProcessMemory, JobMemory, PeakProcessMemory, PeakJobMemory;
    }
    [StructLayout(LayoutKind.Sequential)] private struct Accounting {
        public long UserTime, KernelTime, PeriodUserTime, PeriodKernelTime;
        public uint PageFaults, TotalProcesses, ActiveProcesses, TerminatedProcesses;
    }
    [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] private static extern IntPtr CreateJobObject(IntPtr attributes, string name);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool SetInformationJobObject(IntPtr job, int kind, ref ExtendedLimits limits, int size);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool TerminateJobObject(IntPtr job, uint code);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool QueryInformationJobObject(IntPtr job, int kind, out Accounting accounting, int size, IntPtr returned);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool CreatePipe(out IntPtr read, out IntPtr write, ref SecurityAttributes attributes, int size);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool SetHandleInformation(IntPtr handle, uint mask, uint flags);
    [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] private static extern IntPtr CreateFile(string name, uint access, uint share, ref SecurityAttributes attributes, uint creation, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool InitializeProcThreadAttributeList(IntPtr attributes, int count, int flags, ref IntPtr size);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool UpdateProcThreadAttribute(IntPtr attributes, uint flags, IntPtr attribute, IntPtr value, IntPtr size, IntPtr previous, IntPtr returned);
    [DllImport("kernel32.dll")] private static extern void DeleteProcThreadAttributeList(IntPtr attributes);
    [DllImport("kernel32.dll", SetLastError=true, CharSet=CharSet.Unicode)] private static extern bool CreateProcess(string application, StringBuilder command, IntPtr processAttributes, IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory, ref StartupInfoEx startup, out ProcessInfo process);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool TerminateProcess(IntPtr process, uint code);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool GetExitCodeProcess(IntPtr process, out uint code);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool CloseHandle(IntPtr handle);
    private SafeFileHandle job, process;
    public Stream StandardOutput { get; private set; }
    public Stream StandardError { get; private set; }
    public int Id { get; private set; }
    private static Win32Exception Failure(string operation) { return new Win32Exception(Marshal.GetLastWin32Error(), operation); }
    private static void Close(ref IntPtr handle) {
        if (handle != IntPtr.Zero && handle != new IntPtr(-1)) CloseHandle(handle);
        handle = IntPtr.Zero;
    }
    // Virtual native boundaries allow the probes to force real invalid-handle
    // errors without adding test switches to the runner's public contract.
    protected virtual bool ConfigureJob(IntPtr handle) {
        ExtendedLimits limits = new ExtendedLimits();
        limits.Basic.Flags = 0x2000; // JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
        return SetInformationJobObject(handle, 9, ref limits, Marshal.SizeOf(typeof(ExtendedLimits)));
    }
    protected virtual bool AssignJob(IntPtr handle, IntPtr child) { return AssignProcessToJobObject(handle, child); }
    protected virtual uint ResumeChild(IntPtr thread) { return ResumeThread(thread); }
    public void Start(string executable, string arguments, string directory) {
        IntPtr outRead=IntPtr.Zero, outWrite=IntPtr.Zero, errRead=IntPtr.Zero, errWrite=IntPtr.Zero;
        IntPtr input=IntPtr.Zero, attributes=IntPtr.Zero, handles=IntPtr.Zero;
        bool initialized=false;
        ProcessInfo child=new ProcessInfo();
        try {
            job=new SafeFileHandle(CreateJobObject(IntPtr.Zero, null), true);
            if (job.IsInvalid) throw Failure("Could not create runner job");
            if (!ConfigureJob(job.DangerousGetHandle())) throw Failure("Could not configure runner job");
            SecurityAttributes security=new SecurityAttributes();
            security.Length=Marshal.SizeOf(typeof(SecurityAttributes)); security.Inherit=1;
            if (!CreatePipe(out outRead, out outWrite, ref security, 0)) throw Failure("Could not create stdout pipe");
            if (!CreatePipe(out errRead, out errWrite, ref security, 0)) throw Failure("Could not create stderr pipe");
            if (!SetHandleInformation(outRead, 1, 0) || !SetHandleInformation(errRead, 1, 0)) throw Failure("Could not protect runner pipe handles");
            input=CreateFile("NUL", 0x80000000, 3, ref security, 3, 0, IntPtr.Zero);
            if (input == new IntPtr(-1)) throw Failure("Could not open child stdin");
            IntPtr size=IntPtr.Zero;
            InitializeProcThreadAttributeList(IntPtr.Zero, 1, 0, ref size);
            attributes=Marshal.AllocHGlobal(size);
            if (!InitializeProcThreadAttributeList(attributes, 1, 0, ref size)) throw Failure("Could not initialize child handle list");
            initialized=true;
            handles=Marshal.AllocHGlobal(3 * IntPtr.Size);
            Marshal.WriteIntPtr(handles, 0, input); Marshal.WriteIntPtr(handles, IntPtr.Size, outWrite); Marshal.WriteIntPtr(handles, 2 * IntPtr.Size, errWrite);
            if (!UpdateProcThreadAttribute(attributes, 0, new IntPtr(0x20002), handles, new IntPtr(3 * IntPtr.Size), IntPtr.Zero, IntPtr.Zero)) throw Failure("Could not restrict child handle inheritance");
            StartupInfoEx startup=new StartupInfoEx();
            startup.Info.Size=Marshal.SizeOf(typeof(StartupInfoEx)); startup.Info.Flags=0x100;
            startup.Info.Input=input; startup.Info.Output=outWrite; startup.Info.Error=errWrite; startup.Attributes=attributes;
            // Preserve PATH lookup for a bare executable and quote its command token.
            string application=executable;
            if (executable.IndexOf('\\') < 0 && executable.IndexOf('/') < 0) application=null;
            StringBuilder command=new StringBuilder("\"" + executable + "\" " + arguments);
            if (!CreateProcess(application, command, IntPtr.Zero, IntPtr.Zero, true, 0x08080004, IntPtr.Zero, directory, ref startup, out child)) throw Failure("Could not create suspended runner child");
            process=new SafeFileHandle(child.Process, true); child.Process=IntPtr.Zero; Id=child.ProcessId;
            if (!AssignJob(job.DangerousGetHandle(), process.DangerousGetHandle())) throw Failure("Could not assign child to runner job");
            StandardOutput=new FileStream(new SafeFileHandle(outRead, true), FileAccess.Read, 4096, false); outRead=IntPtr.Zero;
            StandardError=new FileStream(new SafeFileHandle(errRead, true), FileAccess.Read, 4096, false); errRead=IntPtr.Zero;
            if (ResumeChild(child.Thread) == uint.MaxValue) throw Failure("Could not resume contained runner child");
        } catch (Exception startFailure) {
            // Even assignment failure owns a suspended process handle. Terminate
            // it directly; never resume an uncontained child or fall back to PIDs.
            if (process != null && !process.IsInvalid) {
                Exception cleanupFailure=null;
                try {
                    if (!TerminateProcess(process.DangerousGetHandle(), 1) && !HasExited) throw Failure("Could not terminate failed-start child");
                    if (!WaitForExit(5000)) throw new TimeoutException("Failed-start child exceeded its reap deadline");
                } catch (Exception error) { cleanupFailure=error; }
                if (cleanupFailure != null) throw new AggregateException(startFailure, cleanupFailure);
            }
            throw;
        } finally {
            Close(ref child.Thread); Close(ref child.Process);
            Close(ref outRead); Close(ref outWrite); Close(ref errRead); Close(ref errWrite); Close(ref input);
            if (initialized) DeleteProcThreadAttributeList(attributes);
            if (attributes != IntPtr.Zero) Marshal.FreeHGlobal(attributes);
            if (handles != IntPtr.Zero) Marshal.FreeHGlobal(handles);
        }
    }
    public bool WaitForExit(int milliseconds) {
        uint result=WaitForSingleObject(process.DangerousGetHandle(), (uint)milliseconds);
        if (result == uint.MaxValue) throw Failure("Could not wait for runner child");
        return result == 0;
    }
    public bool HasExited { get { return WaitForExit(0); } }
    public int ExitCode {
        get {
            if (!HasExited) throw new InvalidOperationException("Runner child is still active");
            uint code; if (!GetExitCodeProcess(process.DangerousGetHandle(), out code)) throw Failure("Could not read child exit status");
            return unchecked((int)code);
        }
    }
    public void StopTree(int milliseconds) {
        if (job == null || job.IsInvalid || job.IsClosed) return;
        if (!TerminateJobObject(job.DangerousGetHandle(), 1)) throw Failure("Could not terminate runner job");
        Stopwatch deadline=Stopwatch.StartNew();
        for (;;) {
            Accounting accounting;
            if (!QueryInformationJobObject(job.DangerousGetHandle(), 1, out accounting, Marshal.SizeOf(typeof(Accounting)), IntPtr.Zero)) throw Failure("Could not verify runner job cleanup");
            if (accounting.ActiveProcesses == 0) return;
            if (deadline.ElapsedMilliseconds >= milliseconds) throw new TimeoutException("Runner job members exceeded their cleanup deadline");
            Thread.Sleep(10);
        }
    }
    public void Dispose() {
        // Closing the non-inherited job is also the last-resort OS cleanup.
        if (job != null) job.Dispose();
        if (StandardOutput != null) StandardOutput.Dispose();
        if (StandardError != null) StandardError.Dispose();
        if (process != null) process.Dispose();
    }
}
'@
}
$script:RunnerProcessFactory={New-Object DepositPythonOwnedProcess}
if(-not ('DepositPythonProcessCapture' -as [type])) {
    Add-Type -TypeDefinition @'
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
public sealed class DepositPythonProcessCapture {
    private readonly object gate = new object();
    private readonly MemoryStream stdout = new MemoryStream();
    private readonly MemoryStream stderr = new MemoryStream();
    private readonly int limit;
    private bool stdoutOverflow;
    private bool stderrOverflow;
    private Task stdoutPump;
    private Task stderrPump;
    public DepositPythonProcessCapture(int maximumCharacters) { limit = maximumCharacters; }
    public void Attach(Stream output, Stream error) {
        stdoutPump = Pump(output, stdout, true);
        stderrPump = Pump(error, stderr, false);
    }
    private async Task Pump(Stream source, MemoryStream target, bool isStdout) {
        byte[] chunk = new byte[4096];
        int count;
        while ((count = await source.ReadAsync(chunk, 0, chunk.Length).ConfigureAwait(false)) > 0) {
            lock (gate) {
                bool overflow = isStdout ? stdoutOverflow : stderrOverflow;
                if (overflow) continue;
                if (target.Length + count <= limit) target.Write(chunk, 0, count);
                else {
                    target.SetLength(0);
                    if (isStdout) stdoutOverflow = true;
                    else stderrOverflow = true;
                }
            }
        }
    }
    public bool Drain(int timeoutMilliseconds) {
        return Task.WaitAll(new Task[] { stdoutPump ?? Task.CompletedTask, stderrPump ?? Task.CompletedTask }, timeoutMilliseconds);
    }
    public string Stdout { get { lock (gate) return Encoding.UTF8.GetString(stdout.ToArray()); } }
    public string Stderr { get { lock (gate) return Encoding.UTF8.GetString(stderr.ToArray()); } }
    public bool StdoutOverflow { get { lock (gate) return stdoutOverflow; } }
    public bool StderrOverflow { get { lock (gate) return stderrOverflow; } }
}
'@
}

function ConvertTo-NativeProcessArgument([string]$Value) {
    if ($Value -notmatch '[\s"]') { return $Value }
    $result = '"'
    $slashes = 0
    foreach ($character in $Value.ToCharArray()) {
        if ($character -eq '\') { $slashes++; continue }
        if ($character -eq '"') {
            $result += ('\' * ($slashes * 2 + 1)) + '"'
            $slashes = 0
            continue
        }
        $result += ('\' * $slashes) + $character
        $slashes = 0
    }
    $result += ('\' * ($slashes * 2)) + '"'
    return $result
}

function Stop-RunnerProcessTree([DepositPythonOwnedProcess]$Process,[int]$TimeoutMilliseconds=5000) {
    $Process.StopTree($TimeoutMilliseconds)
}

function Invoke-RunnerProcess {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string]$FilePath,
        [string[]]$Arguments=@(),
        [Parameter(Mandatory=$true)][string]$WorkingDirectory,
        [Parameter(Mandatory=$true)][string]$StandardOutputPath,
        [Parameter(Mandatory=$true)][string]$StandardErrorPath,
        [ValidateRange(1,7200)][int]$TimeoutSeconds=300,
        [string[]]$Secrets=@(),
        [ValidateRange(1,16777216)][int]$CaptureLimitBytes=8388608
    )
    foreach($path in @($StandardOutputPath,$StandardErrorPath)) {
        $parent=Split-Path -Parent $path
        if($parent){New-Item -ItemType Directory -Force -Path $parent | Out-Null}
    }
    $argumentLine=(@($Arguments | ForEach-Object { ConvertTo-NativeProcessArgument ([string]$_) }) -join ' ')
    $process=& $script:RunnerProcessFactory
    $capture=New-Object DepositPythonProcessCapture -ArgumentList $CaptureLimitBytes
    $limit=$CaptureLimitBytes
    $timedOut=$false;$started=$false;$nativeExitCode=1;$hasExited=$false;$processId=$null
    try {
        $process.Start($FilePath,$argumentLine,$WorkingDirectory)
        $started=$true;$processId=$process.Id
        $capture.Attach($process.StandardOutput,$process.StandardError)
        if(!$process.WaitForExit($TimeoutSeconds*1000)) {
            $timedOut=$true
            Stop-RunnerProcessTree $process
        }
        if(!$process.WaitForExit(5000)){throw 'Child process did not exit before the reap deadline'}
        $nativeExitCode=$process.ExitCode
        # Parent exit is not ownership completion: descendants may retain pipes.
        Stop-RunnerProcessTree $process
        if(!$capture.Drain(5000)){throw 'Child process output streams did not close before the drain deadline'}
    } catch { throw }
    finally {
        $cleanupFailure=$null
        try {
            try {
                if($started){
                    try{Stop-RunnerProcessTree $process}catch{$cleanupFailure=$_}
                    if(!$process.WaitForExit(5000)){
                        if(!$cleanupFailure){$cleanupFailure='Child process could not be reaped before the cleanup deadline'}
                        $process.StandardOutput.Close();$process.StandardError.Close()
                    }
                    $hasExited=[bool]$process.HasExited
                    if(!$capture.Drain(5000)){
                        $process.StandardOutput.Close();$process.StandardError.Close()
                        if(!$capture.Drain(1000) -and !$cleanupFailure){$cleanupFailure='Child output capture could not be drained before the cleanup deadline'}
                    }
                }
            } catch { if(!$cleanupFailure){$cleanupFailure=$_} }
            # A native cleanup/drain error must not skip sanitized evidence.
            $outText=$capture.Stdout;$errText=$capture.Stderr
            if($capture.StdoutOverflow){$outText += "`n[output truncated after $limit bytes]`n"}
            if($capture.StderrOverflow){$errText += "`n[output truncated after $limit bytes]`n"}
            foreach($secret in $Secrets){if($secret){$outText=$outText.Replace($secret,'[REDACTED]');$errText=$errText.Replace($secret,'[REDACTED]')}}
            [IO.File]::WriteAllText($StandardOutputPath,$outText,(New-Object Text.UTF8Encoding($false)))
            [IO.File]::WriteAllText($StandardErrorPath,$errText,(New-Object Text.UTF8Encoding($false)))
        } catch { if(!$cleanupFailure){$cleanupFailure=$_} }
        finally { $process.Dispose() }
        if($cleanupFailure){throw $cleanupFailure}
    }
    return [pscustomobject]@{ExitCode=$nativeExitCode;ProcessId=$processId;HasExited=$hasExited;TimedOut=$timedOut;StdOutPath=$StandardOutputPath;StdErrPath=$StandardErrorPath}
}

Export-ModuleMember -Function Invoke-RunnerProcess
