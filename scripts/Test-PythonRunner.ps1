[CmdletBinding()]
param([string]$Root=(Join-Path $PSScriptRoot '..\..\test_harness'))
$ErrorActionPreference='Stop'
$Root=[IO.Path]::GetFullPath($Root)
$probeRoot=Join-Path $Root 'tools\python-tests'
New-Item -ItemType Directory -Path $probeRoot -Force | Out-Null
$scratch=Join-Path $probeRoot ('runner-probe-'+[Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $scratch -Force | Out-Null
Import-Module (Join-Path $PSScriptRoot 'PythonRunnerProcess.psm1') -Force
$runnerModule=Get-Module PythonRunnerProcess
$sentinel=$null
function New-FailingRunnerProcessType([string]$Method) {
    # Override one native boundary with an invalid handle. This exercises real
    # Win32 failures before resume, without an environment/public API test flag.
    $name='RunnerFailure'+$Method+[Guid]::NewGuid().ToString('N')
    $assembly=[AppDomain]::CurrentDomain.DefineDynamicAssembly((New-Object Reflection.AssemblyName($name)),[Reflection.Emit.AssemblyBuilderAccess]::Run)
    $builder=$assembly.DefineDynamicModule($name).DefineType($name,[Reflection.TypeAttributes]::Public,[DepositPythonOwnedProcess])
    $baseMethod=[DepositPythonOwnedProcess].GetMethod($Method,([Reflection.BindingFlags]'Instance,NonPublic'))
    $parameters=@($baseMethod.GetParameters() | ForEach-Object {$_.ParameterType})
    $override=$builder.DefineMethod($Method,([Reflection.MethodAttributes]'Family,Virtual,HideBySig'),$baseMethod.ReturnType,[Type[]]$parameters)
    $il=$override.GetILGenerator()
    $il.Emit([Reflection.Emit.OpCodes]::Ldarg_0)
    $il.Emit([Reflection.Emit.OpCodes]::Ldsfld,[IntPtr].GetField('Zero'))
    for($index=1;$index -lt $parameters.Count;$index++){$il.Emit([Reflection.Emit.OpCodes]::Ldarg,[int16]($index+1))}
    $il.Emit([Reflection.Emit.OpCodes]::Call,$baseMethod)
    $il.Emit([Reflection.Emit.OpCodes]::Ret)
    $builder.DefineMethodOverride($override,$baseMethod)
    return $builder.CreateType()
}
try {
    $sentinel=Start-Process -FilePath (Join-Path $PSHOME 'powershell.exe') -ArgumentList @('-NoProfile','-NonInteractive','-Command','Start-Sleep -Seconds 120') -WindowStyle Hidden -PassThru
    $fail=Join-Path $scratch 'fail.ps1';[IO.File]::WriteAllText($fail,"Write-Output 'probe-secret'; exit 23")
    $secret='probe-secret'
    $result=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$fail) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch 'fail.stdout.log') -StandardErrorPath (Join-Path $scratch 'fail.stderr.log') -TimeoutSeconds 10 -Secrets @($secret)
    if($result.ExitCode -ne 23 -or $result.TimedOut){throw "Nonzero child exit was not propagated (exit=$($result.ExitCode), timedOut=$($result.TimedOut), output=$([IO.File]::ReadAllText($result.StdOutPath)))"}
    if([IO.File]::ReadAllText($result.StdOutPath).Contains($secret)){throw 'Child output secret was not redacted'}
    $slow=Join-Path $scratch 'slow.ps1';[IO.File]::WriteAllText($slow,"Write-Host -NoNewline 'chunk-secret'; Start-Sleep -Seconds 20")
    $timed=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$slow) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch 'slow.stdout.log') -StandardErrorPath (Join-Path $scratch 'slow.stderr.log') -TimeoutSeconds 1 -Secrets @('chunk-secret')
    if(!$timed.TimedOut -or !$timed.HasExited){throw 'Timed-out child was not terminated and reaped'}
    if(Get-Process -Id $timed.ProcessId -ErrorAction SilentlyContinue){throw 'Timed-out child process remains after cleanup'}
    if([IO.File]::ReadAllText($timed.StdOutPath).Contains('chunk-secret')){throw 'Timed-out child output secret was not redacted'}
    $grandchildScript=Join-Path $scratch 'grandchild.ps1';[IO.File]::WriteAllText($grandchildScript,'Start-Sleep -Seconds 20')
    $treePidFile=Join-Path $scratch 'grandchild.pid'
    $tree=Join-Path $scratch 'tree.ps1'
    [IO.File]::WriteAllText($tree,('$grand=Start-Process -FilePath (Join-Path $PSHOME ''powershell.exe'') -ArgumentList @(''-NoProfile'',''-NonInteractive'',''-File'',''{0}'') -PassThru; [IO.File]::WriteAllText(''{1}'',[string]$grand.Id); Start-Sleep -Seconds 20' -f $grandchildScript,$treePidFile))
    $stopwatch=[Diagnostics.Stopwatch]::StartNew()
    $treeResult=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$tree) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch 'tree.stdout.log') -StandardErrorPath (Join-Path $scratch 'tree.stderr.log') -TimeoutSeconds 1
    $stopwatch.Stop()
    if(!$treeResult.TimedOut -or !$treeResult.HasExited){throw 'Timed-out process tree was not terminated and reaped'}
    if($stopwatch.Elapsed.TotalSeconds -ge 15){throw 'Timed-out process tree cleanup exceeded its bounded deadline'}
    if(!(Test-Path $treePidFile)){throw 'Process-tree probe did not record its grandchild'}
    $grandchildId=[int][IO.File]::ReadAllText($treePidFile)
    if(Get-Process -Id $grandchildId -ErrorAction SilentlyContinue){throw 'Timed-out grandchild process remains after cleanup'}
    $inheritHelper=Join-Path $scratch 'inherit-pipes.ps1'
    [IO.File]::WriteAllText($inheritHelper,@'
Add-Type -TypeDefinition @"
using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Text;
public static class RunnerPipeDescendant {
    [StructLayout(LayoutKind.Sequential, CharSet=CharSet.Unicode)] private struct Startup {
        public int Size; public string Reserved, Desktop, Title;
        public int X, Y, XSize, YSize, XCount, YCount, Fill, Flags;
        public short ShowWindow, ReservedSize; public IntPtr ReservedBytes, Input, Output, Error;
    }
    [StructLayout(LayoutKind.Sequential)] private struct Child { public IntPtr Process, Thread; public int Id, ThreadId; }
    [DllImport("kernel32.dll",SetLastError=true,CharSet=CharSet.Unicode)] private static extern bool CreateProcess(string application, StringBuilder command, IntPtr processAttributes, IntPtr threadAttributes, bool inherit, uint flags, IntPtr environment, string directory, ref Startup startup, out Child child);
    [DllImport("kernel32.dll")] private static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll")] private static extern bool CloseHandle(IntPtr handle);
    public static void Start(string executable, string script) {
        Startup startup=new Startup(); startup.Size=Marshal.SizeOf(typeof(Startup)); startup.Flags=0x100;
        startup.Input=GetStdHandle(-10); startup.Output=GetStdHandle(-11); startup.Error=GetStdHandle(-12);
        Child child;
        if(!CreateProcess(executable,new StringBuilder("\""+executable+"\" -NoProfile -NonInteractive -File \""+script+"\""),IntPtr.Zero,IntPtr.Zero,true,0x08000000,IntPtr.Zero,null,ref startup,out child)) throw new Win32Exception();
        CloseHandle(child.Thread); CloseHandle(child.Process);
    }
}
"@
'@)
    foreach($parentExit in @(0,23)) {
        $ready=Join-Path $scratch "early-$parentExit.pid"
        $inherited=Join-Path $scratch "inherited-$parentExit.ps1"
        [IO.File]::WriteAllText($inherited,('[Console]::Out.WriteLine(''grandchild-secret''); [Console]::Out.Flush(); [Console]::Error.WriteLine(''grandchild-secret''); [Console]::Error.Flush(); [IO.File]::WriteAllText(''{0}'',[string]$PID); Start-Sleep -Seconds 60' -f $ready))
        $early=Join-Path $scratch "early-$parentExit.ps1"
        # Explicitly inherit the parent's standard handles. Wait for the
        # descendant's own marker so this proves a live
        # pipe-holding descendant existed before its parent exited.
        [IO.File]::WriteAllText($early,('. ''{3}''; [RunnerPipeDescendant]::Start((Join-Path $PSHOME ''powershell.exe''),''{0}''); $deadline=[Diagnostics.Stopwatch]::StartNew(); while(!(Test-Path -LiteralPath ''{1}'')){{if($deadline.Elapsed.TotalSeconds -ge 5){{exit 91}}; Start-Sleep -Milliseconds 10}}; Write-Output ''parent-secret''; exit {2}' -f $inherited,$ready,$parentExit,$inheritHelper))
        $stopwatch=[Diagnostics.Stopwatch]::StartNew()
        $earlyResult=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$early) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch "early-$parentExit.stdout.log") -StandardErrorPath (Join-Path $scratch "early-$parentExit.stderr.log") -TimeoutSeconds 10 -Secrets @('parent-secret','grandchild-secret')
        $stopwatch.Stop()
        if($earlyResult.ExitCode -ne $parentExit -or $earlyResult.TimedOut -or !$earlyResult.HasExited){throw "Early-exiting parent status was lost: expected $parentExit, actual $($earlyResult.ExitCode)"}
        if($stopwatch.Elapsed.TotalSeconds -ge 8){throw 'Early-exiting parent descendant cleanup exceeded its deadline'}
        if(!(Test-Path -LiteralPath $ready)){throw 'Early-exit descendant did not run before parent exit'}
        $descendantId=[int][IO.File]::ReadAllText($ready)
        if(Get-Process -Id $descendantId -ErrorAction SilentlyContinue){throw "Descendant survived parent exit $parentExit"}
        $earlyOut=[IO.File]::ReadAllText($earlyResult.StdOutPath);$earlyErr=[IO.File]::ReadAllText($earlyResult.StdErrPath)
        if($earlyOut.Contains('parent-secret') -or $earlyOut.Contains('grandchild-secret') -or $earlyErr.Contains('grandchild-secret')){throw 'Early-exit descendant logs were not redacted'}
        if(($earlyOut.Split([string[]]@('[REDACTED]'),[StringSplitOptions]::None).Count - 1) -ne 2 -or !$earlyErr.Contains('[REDACTED]')){throw "Early-exit descendant did not inherit captured pipes or sanitized logs were not finalized (stdout=$earlyOut; stderr=$earlyErr)"}
    }
    foreach($boundary in @('ConfigureJob','AssignJob','ResumeChild')) {
        $failureType=New-FailingRunnerProcessType $boundary
        $marker=Join-Path $scratch "$boundary.executed"
        $blocked=Join-Path $scratch "$boundary.ps1"
        [IO.File]::WriteAllText($blocked,('[IO.File]::WriteAllText(''{0}'',''must never execute'')' -f $marker))
        & $runnerModule {param($type) $script:RunnerProbeType=$type; $script:RunnerProcessFactory={ $script:RunnerProbeOwner=[Activator]::CreateInstance($script:RunnerProbeType); $script:RunnerProbeOwner }} $failureType
        $failedStart=$false
        $stopwatch=[Diagnostics.Stopwatch]::StartNew()
        try {
            Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$blocked) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch "$boundary.stdout.log") -StandardErrorPath (Join-Path $scratch "$boundary.stderr.log") -TimeoutSeconds 2 | Out-Null
        } catch {
            $expected=@{ConfigureJob='configure runner job';AssignJob='assign child to runner job';ResumeChild='resume contained runner child'}[$boundary]
            if(!$_.Exception.ToString().Contains($expected)){throw}
            $failedStart=$true
        } finally { & $runnerModule {$script:RunnerProcessFactory={New-Object DepositPythonOwnedProcess}} }
        $stopwatch.Stop()
        if(!$failedStart){throw "Injected $boundary failure was not propagated"}
        if($stopwatch.Elapsed.TotalSeconds -ge 7){throw "Injected $boundary failure cleanup was not bounded"}
        if(Test-Path -LiteralPath $marker){throw "Child executed after $boundary failure"}
        $failedOwner=& $runnerModule {$script:RunnerProbeOwner}
        if($boundary -ne 'ConfigureJob' -and $failedOwner.Id -le 0){throw "The $boundary probe never created a suspended child"}
        if($failedOwner.Id -gt 0 -and (Get-Process -Id $failedOwner.Id -ErrorAction SilentlyContinue)){throw "Suspended child survived $boundary failure"}
        foreach($stream in @('stdout','stderr')) {
            $log=Join-Path $scratch "$boundary.$stream.log"
            if(!(Test-Path -LiteralPath $log) -or [IO.File]::ReadAllText($log)){throw "$boundary failed-start logs were not finalized empty"}
        }
    }
    if(Get-ChildItem -LiteralPath $scratch -Filter '*.raw' -Recurse){throw 'Runner created a plaintext raw capture artifact'}
    $large=Join-Path $scratch 'large.ps1';[IO.File]::WriteAllText($large,"Write-Output ('partial-secret-' + ('x' * 2048))")
    $bounded=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$large) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch 'large.stdout.log') -StandardErrorPath (Join-Path $scratch 'large.stderr.log') -TimeoutSeconds 10 -Secrets @('partial-secret') -CaptureLimitBytes 64
    $boundedText=[IO.File]::ReadAllText($bounded.StdOutPath)
    if(!$boundedText.Contains('[output truncated after 64 bytes]') -or $boundedText.Contains('partial-secret')){throw 'Capture limit did not discard overflow safely'}
    if($boundedText.Length -gt 80){throw 'Retained bounded capture exceeded configured limit plus marker'}
    $missingOut=Join-Path $scratch 'missing.stdout.log';$missingErr=Join-Path $scratch 'missing.stderr.log'
    try { Invoke-RunnerProcess -FilePath (Join-Path $scratch 'absent.exe') -WorkingDirectory $scratch -StandardOutputPath $missingOut -StandardErrorPath $missingErr -TimeoutSeconds 2 | Out-Null; throw 'Missing child executable unexpectedly started' } catch { if($_.Exception.Message -eq 'Missing child executable unexpectedly started'){throw} }
    if(!(Test-Path $missingOut) -or !(Test-Path $missingErr)){throw 'Failed capture did not finalize sanitized evidence logs'}
    if([IO.File]::ReadAllText($missingOut) -or [IO.File]::ReadAllText($missingErr)){throw 'Failed child start left unexpected output'}
    $crash=Join-Path $scratch 'abnormal.ps1';[IO.File]::WriteAllText($crash,"[Console]::Error.Write('failure-secret'); exit 29")
    $abnormal=Invoke-RunnerProcess -FilePath (Join-Path $PSHOME 'powershell.exe') -Arguments @('-NoProfile','-NonInteractive','-File',$crash) -WorkingDirectory $scratch -StandardOutputPath (Join-Path $scratch 'abnormal.stdout.log') -StandardErrorPath (Join-Path $scratch 'abnormal.stderr.log') -TimeoutSeconds 10 -Secrets @('failure-secret')
    if($abnormal.ExitCode -ne 29 -or [IO.File]::ReadAllText($abnormal.StdErrPath).Contains('failure-secret')){throw 'Abnormal child exit did not preserve exit status and redact captured stderr'}
    if($sentinel.HasExited){throw 'Runner cleanup terminated an unrelated sentinel process'}
    Write-Output 'PASS: runner exit, bounded timeout/job cleanup, early parent exits 0/23 with inherited pipes, descendant reaping, config/assignment/resume failures, unrelated sentinel, bounded capture, failed start, abnormal exit, redaction, and no-raw-artifact probes'
} catch {
    [IO.File]::WriteAllText((Join-Path $env:TEMP 'deposit-python-runner-probe.failure.txt'),("{0}`n{1}" -f $_.Exception.Message,$_.ScriptStackTrace))
    Write-Error ("Runner probe failed: {0}`n{1}" -f $_.Exception.Message,$_.ScriptStackTrace)
    throw
} finally {
    if($sentinel){if(!$sentinel.HasExited){$sentinel.Kill();if(!$sentinel.WaitForExit(5000)){throw 'Sentinel cleanup exceeded its deadline'}};$sentinel.Dispose()}
    Remove-Module PythonRunnerProcess -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $scratch -Recurse -Force -ErrorAction SilentlyContinue
    if(Test-Path -LiteralPath $scratch){throw "Runner probe scratch cleanup failed: $scratch"}
}
