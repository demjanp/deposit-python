[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][ValidateRange(1,6)][int]$Stage,
    [switch]$ListOnly,
    [string]$Root,
    [string[]]$Group,
    [string[]]$Selection,
    [string]$PythonExecutable='python',
    [string]$PythonBuildBundle
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$scriptFile=$MyInvocation.MyCommand.Path
if(!$Root){$Root=Join-Path (Split-Path $scriptFile -Parent) '..\..\test_harness'}
$server=Join-Path (Split-Path $repo -Parent) 'deposit-server'
$Root=[IO.Path]::GetFullPath($Root)
if($PythonBuildBundle){
    $PythonBuildBundle=[IO.Path]::GetFullPath($PythonBuildBundle)
    $bundlePrefix=[IO.Path]::GetFullPath((Join-Path $Root 'cache\python-acceptance-build-bundles')).TrimEnd('\')+'\'
    if(!$PythonBuildBundle.StartsWith($bundlePrefix,[StringComparison]::OrdinalIgnoreCase) -or !(Test-Path -LiteralPath $PythonBuildBundle -PathType Container)){throw 'PythonBuildBundle must identify a directory under the harness-owned bundle cache.'}
}
$stageGroups=@('foundation','core','gateway','domain','workflows','release')
$foundationGroups=@('package','manifest','storage','identity','runner','live')
$validGroups=@('foundation','package','manifest','storage','identity','runner','live','core','gateway','domain','workflows','release')
$groups=if($Group){@($Group | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })}else{@('foundation')}
$invalidGroups=@($groups|Where-Object {$_ -notin $validGroups})
if($invalidGroups.Count){throw "Unknown Python test group(s): $($invalidGroups -join ', ')"}
$selectedGroups=if($groups -contains 'foundation'){@($foundationGroups)}else{@($groups)}
$later=@($selectedGroups|Where-Object {$_ -notin $foundationGroups})
$known=@{
    package=@('tests/unit/test_installed_package.py','tests/unit/test_client.py')
    manifest=@('tests/unit/test_manifest.py')
    storage=@('tests/unit/test_protected_storage.py')
    identity=@('tests/unit/test_identity.py')
    runner=@('scripts/Test-PythonRunner.ps1','../deposit-server/scripts/Test-PythonAcceptanceBuildBundle.ps1')
    live=@('tests/integration/test_live_smoke.py')
}
if($ListOnly) {
    Write-Output "Python repository: $repo"
    Write-Output "Server repository: $server"
    Write-Output "Stage: $Stage; groups: $($groups -join ', ')"
    Write-Output 'Target interpreter: Windows x64 Python 3.14.x; caller-owned environment recorded per run.'
    Write-Output 'Foundation groups and concrete test selections:'
    foreach($item in $foundationGroups) {
        Write-Output ("  {0}: {1}" -f $item,($known[$item] -join ', '))
    }
    Write-Output 'Prerequisites: network access for pinned Python dependencies; Windows DPAPI/ACLs; for live, the existing server harness and its fresh-runtime lifecycle owner.'
    Write-Output 'Live hook: Invoke-IntegrationSuite.ps1 -SelectedPythonTest Smoke -PythonRoot <repo> -PythonEnvironment <harness venv> -Root <harness>'
    Write-Output 'Optional live build bundle: -PythonBuildBundle <verified clean tagged bundle under harness cache>; skips only server Build/Package.'
    return
}

if($Stage -ne 1) {throw "Stage $Stage selections are not implemented yet; this runner currently executes Stage 1 foundation groups only."}
if($later.Count){throw 'Only Stage 1 foundation groups are available in this implementation.'}
if($Selection -and $selectedGroups.Count -ne 1){throw 'Use -Selection with exactly one foundation group.'}
if($PythonBuildBundle -and $selectedGroups -notcontains 'live'){throw 'PythonBuildBundle requires the live foundation group.'}
if(!(Test-Path -LiteralPath $server -PathType Container)){throw "Sibling server checkout is missing: $server"}
$resolvedPython=(Get-Command $PythonExecutable -ErrorAction Stop).Source
$pythonVersion=& $resolvedPython --version 2>&1
if($LASTEXITCODE -ne 0 -or $pythonVersion -notmatch '^Python 3\.14\.') {throw "Stage 1 requires Python 3.14.x; found $pythonVersion"}
$runId=[DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')+'-'+[Guid]::NewGuid().ToString('N').Substring(0,8)
$runnerRoot=Join-Path $Root 'tools\python-tests'
$runRoot=Join-Path $runnerRoot $runId
$resultsRoot=Join-Path $Root ('results\python-'+$runId)
$workingLogs=Join-Path $runRoot 'logs'
New-Item -ItemType Directory -Force -Path $runRoot,$workingLogs | Out-Null
Import-Module (Join-Path $PSScriptRoot 'PythonRunnerProcess.psm1') -Force
$previousSourceRoot=$env:DEPOSIT_PYTHON_SOURCE_ROOT
$previousPythonPath=$env:PYTHONPATH
$env:DEPOSIT_PYTHON_SOURCE_ROOT=$repo
$env:PYTHONPATH=if($previousPythonPath){$repo+[IO.Path]::PathSeparator+$previousPythonPath}else{$repo}
$records=New-Object 'System.Collections.Generic.List[object]'
function Invoke-Recorded([string]$Label,[string]$File,[string[]]$Arguments,[string]$WorkingDirectory,[int]$Timeout=900) {
    $out=Join-Path $workingLogs ($Label+'.stdout.log');$err=Join-Path $workingLogs ($Label+'.stderr.log')
    $result=Invoke-RunnerProcess -FilePath $File -Arguments $Arguments -WorkingDirectory $WorkingDirectory -StandardOutputPath $out -StandardErrorPath $err -TimeoutSeconds $Timeout
    $records.Add([pscustomobject]@{label=$Label;command=$File;arguments=$Arguments;exitCode=$result.ExitCode;timedOut=$result.TimedOut;stdout=$out;stderr=$err})
    if($result.TimedOut){throw "$Label timed out; retained output: $resultsRoot"}
    if($result.ExitCode -ne 0){throw "$Label failed with exit code $($result.ExitCode); retained output: $resultsRoot"}
    return $result
}
function New-TestEnvironment([string]$Name) {
    $environment=Join-Path $runRoot $Name
    if(!(Test-Path -LiteralPath $environment)){$null=Invoke-Recorded "$Name-venv" $resolvedPython @('-m','venv',$environment) $repo}
    return (Join-Path $environment 'Scripts\python.exe')
}
function Resolve-TestTargets([string[]]$Targets) {
    return @($Targets | ForEach-Object { [IO.Path]::GetFullPath((Join-Path $repo $_)) })
}

try {
    $builder=New-TestEnvironment 'builder'
    Invoke-Recorded 'builder-dependencies' $builder @('-m','pip','install','--disable-pip-version-check','--no-input','-r',(Join-Path $repo 'requirements-test.lock')) $repo 1800
    $distributionRoot=Join-Path $runRoot 'dist'
    New-Item -ItemType Directory -Force -Path $distributionRoot | Out-Null
    Invoke-Recorded 'build-wheel-sdist' $builder @('-m','build','--wheel','--sdist','--outdir',$distributionRoot,$repo) $repo 1800
    $wheel=Get-ChildItem -LiteralPath $distributionRoot -Filter 'deposit-*.whl' | Select-Object -First 1
    $sdist=Get-ChildItem -LiteralPath $distributionRoot -Filter 'deposit-*.tar.gz' | Select-Object -First 1
    if(!$wheel -or !$sdist){throw 'Build did not produce both wheel and sdist artifacts'}

    $wheelPython=New-TestEnvironment 'wheel-install'
    Invoke-Recorded 'wheel-install-dependencies' $wheelPython @('-m','pip','install','--disable-pip-version-check','--no-input','-r',(Join-Path $repo 'requirements-test.lock')) $repo 1800
    Invoke-Recorded 'wheel-install' $wheelPython @('-m','pip','install','--disable-pip-version-check','--no-input',$wheel.FullName) $repo 1800
    $importAssertion='import deposit,sysconfig; from importlib.metadata import distribution; from pathlib import Path; p=Path(deposit.__file__).resolve(); s=Path(sysconfig.get_paths()["purelib"]).resolve(); d=Path(distribution("deposit").locate_file("")).resolve(); r=Path(__import__("os").environ["DEPOSIT_PYTHON_SOURCE_ROOT"]).resolve(); assert s in p.parents and d == s and r not in p.parents; print(deposit.__version__); print(p); print(d)'
    Invoke-Recorded 'wheel-import-smoke' $wheelPython @('-c',$importAssertion) $runRoot

    if($selectedGroups -contains 'package') {
        $targets=Resolve-TestTargets $(if($Selection){@($Selection)}else{@($known.package)})
        Invoke-Recorded 'wheel-package-tests' $wheelPython (@('-m','pytest','-q','--import-mode=importlib')+$targets) $runRoot 1800
    }
    if($selectedGroups -contains 'manifest') {
        $targets=Resolve-TestTargets $(if($Selection){@($Selection)}else{@($known.manifest)})
        Invoke-Recorded 'wheel-manifest-tests' $wheelPython (@('-m','pytest','-q','--import-mode=importlib')+$targets) $runRoot 1800
    }
    if($selectedGroups -contains 'storage') {
        $targets=Resolve-TestTargets $(if($Selection){@($Selection)}else{@($known.storage)})
        Invoke-Recorded 'wheel-storage-tests' $wheelPython (@('-m','pytest','-q','--import-mode=importlib')+$targets) $runRoot 1800
    }
    if($selectedGroups -contains 'identity') {
        $targets=Resolve-TestTargets $(if($Selection){@($Selection)}else{@($known.identity)})
        Invoke-Recorded 'wheel-identity-tests' $wheelPython (@('-m','pytest','-q','--import-mode=importlib')+$targets) $runRoot 1800
    }
    if($selectedGroups -contains 'runner') {
        Invoke-Recorded 'runner-probes' (Join-Path $PSHOME 'powershell.exe') @('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',(Join-Path $PSScriptRoot 'Test-PythonRunner.ps1'),'-Root',$Root) $repo 120
        Invoke-Recorded 'build-bundle-probes' (Join-Path $PSHOME 'powershell.exe') @('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',(Join-Path $server 'scripts\Test-PythonAcceptanceBuildBundle.ps1')) $server 120
    }
    if($selectedGroups -contains 'live') {
        $suite=Join-Path $server 'scripts\Invoke-IntegrationSuite.ps1'
        $arguments=@('-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass','-File',$suite,'-SelectedPythonTest','Smoke','-PythonRoot',$repo,'-PythonEnvironment',(Split-Path (Split-Path $wheelPython -Parent) -Parent),'-Root',$Root)
        if($PythonBuildBundle){$arguments+=@('-PythonBuildBundle',$PythonBuildBundle)}
        Invoke-Recorded 'live-smoke-hook' (Join-Path $PSHOME 'powershell.exe') $arguments $server 7200
    }

    $sdistPython=New-TestEnvironment 'sdist-install'
    Invoke-Recorded 'sdist-install-dependencies' $sdistPython @('-m','pip','install','--disable-pip-version-check','--no-input','-r',(Join-Path $repo 'requirements-test.lock')) $repo 1800
    Invoke-Recorded 'sdist-install' $sdistPython @('-m','pip','install','--disable-pip-version-check','--no-input',$sdist.FullName) $repo 1800
    Invoke-Recorded 'sdist-import-smoke' $sdistPython @('-c',$importAssertion) $runRoot
    if($selectedGroups -contains 'package') {
        $targets=Resolve-TestTargets $(if($Selection){@($Selection)}else{@($known.package)})
        Invoke-Recorded 'sdist-package-tests' $sdistPython (@('-m','pytest','-q','--import-mode=importlib')+$targets) $runRoot 1800
    }
    New-Item -ItemType Directory -Force -Path $resultsRoot | Out-Null
    Copy-Item -LiteralPath $workingLogs -Destination $resultsRoot -Recurse -Force
    $resultFile=Join-Path $resultsRoot 'results.json'
    $summary=[ordered]@{schemaVersion=1;runId=$runId;python=$pythonVersion;pythonExecutable=$resolvedPython;groups=$groups;wheel=$wheel.FullName;sdist=$sdist.FullName;commands=@($records.ToArray());completedUtc=[DateTime]::UtcNow.ToString('o')}
    [IO.File]::WriteAllText($resultFile,($summary|ConvertTo-Json -Depth 8),(New-Object Text.UTF8Encoding($false)))
    Write-Output "PASS: Python Stage $Stage; retained artifacts and logs under $resultsRoot"
    Write-Output "Result record: $resultFile"
} catch {
    New-Item -ItemType Directory -Force -Path $resultsRoot | Out-Null
    if(Test-Path -LiteralPath $workingLogs){Copy-Item -LiteralPath $workingLogs -Destination $resultsRoot -Recurse -Force}
    $resultFile=Join-Path $resultsRoot 'results.json'
    $summary=[ordered]@{schemaVersion=1;runId=$runId;python=$pythonVersion;pythonExecutable=$resolvedPython;groups=$groups;commands=@($records.ToArray());failure=$_.Exception.Message;completedUtc=[DateTime]::UtcNow.ToString('o')}
    [IO.File]::WriteAllText($resultFile,($summary|ConvertTo-Json -Depth 8),(New-Object Text.UTF8Encoding($false)))
    throw
} finally {
    if($null -eq $previousSourceRoot){Remove-Item Env:\DEPOSIT_PYTHON_SOURCE_ROOT -ErrorAction SilentlyContinue}else{$env:DEPOSIT_PYTHON_SOURCE_ROOT=$previousSourceRoot}
    if($null -eq $previousPythonPath){Remove-Item Env:\PYTHONPATH -ErrorAction SilentlyContinue}else{$env:PYTHONPATH=$previousPythonPath}
    Remove-Module PythonRunnerProcess -ErrorAction SilentlyContinue
}
