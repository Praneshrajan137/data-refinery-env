<#
.SYNOPSIS
    Publishes tonight's inputs for the four-stage cloud pipeline: a fresh source snapshot,
    the task, and a new MANIFEST.json.

.DESCRIPTION
    The cloud fires have no route to GitHub, so they cannot fetch source themselves. This
    script is the only thing that can refresh what they see. It runs locally BEFORE the first
    fire and does four things:

      1. Builds a source snapshot from `git archive HEAD` - deliberately HEAD, not the working
         tree, so that another session's uncommitted work never ships into an unattended run.
      2. Uploads that snapshot and the task file to the user workspace stage.
      3. DELETES the previous run's handoff artifacts. This is not tidiness: without it, a
         night where stage 1 dies leaves yesterday's 01-explore.md in place for stage 2 to
         read as though it were today's, and the manifest check is the only thing standing
         between that and confidently wrong work. Removing the files makes the failure loud
         twice over.
      4. Writes a fresh MANIFEST.json carrying run_id, the sha256 of the task and the md5 of
         the snapshot, with no stage entries. Every stage validates its own computed hashes
         against these, so if the inputs change mid-run the pipeline refuses instead of
         producing work from mismatched material.

    Uses SQL PUT/GET via `snow sql`, not `cortex ws cp`, which is broken on Windows in both
    directions (upload: "undefined is not a directory"; download: a doubled drive letter).

.PARAMETER TaskFile
    The task for tonight. Defaults to scripts/automation/TASK.md if it exists.

.PARAMETER RunId
    Overrides the run id. Defaults to today's UTC date. The stages compare this against the
    manifest, so it only needs to be internally consistent for one run.

.PARAMETER Connection
    Snowflake connection profile. Defaults to the headless key-pair profile; the interactive
    OAuth profile needs a browser once its token expires and so cannot be used from a
    scheduled task.

.PARAMETER KeepHandoffs
    Do not delete the previous run's artifacts. For debugging only. Leaving stale artifacts in
    place is exactly the condition this script exists to prevent.

.EXAMPLE
    powershell -NoProfile -File scripts\automation\publish_run_inputs.ps1 -TaskFile C:\tmp\task.md

.NOTES
    Windows PowerShell 5.1. `pwsh` is not installed on this machine.
    Exit codes: 0 = published; 1 = refused (bad input, e.g. no task); 2 = Snowflake failure.
#>
[CmdletBinding()]
param(
    [string]$TaskFile,
    [string]$RunId,
    [string]$Connection = 'dataforge_automation',
    [switch]$KeepHandoffs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RepoRoot  = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Workspace = 'USER$PRANESH07.PUBLIC.DEFAULT$'
$StagePath = "snow://workspace/$Workspace/versions/live"
$WorkDir   = Join-Path $env:TEMP 'dataforge-automation'
$Snow      = Join-Path $env:LOCALAPPDATA 'dataforge-automation\venv\Scripts\snow.exe'

if (-not $RunId)    { $RunId = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd') }
if (-not $TaskFile) { $TaskFile = Join-Path $PSScriptRoot 'TASK.md' }

function Write-Step { param([string]$Message) Write-Host "`n==> $Message" -ForegroundColor Cyan }
function Write-Note { param([string]$Message) Write-Host "    $Message" -ForegroundColor DarkGray }
function Write-Fail { param([string]$Message) Write-Host "!!! $Message" -ForegroundColor Yellow }

# Native tools write progress to stderr on success, which $ErrorActionPreference = 'Stop'
# turns into a terminating NativeCommandError. Judge by exit code only.
function Invoke-Native {
    param([Parameter(Mandatory)][string]$File, [string[]]$Arguments = @())
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $File @Arguments 2>&1 | Out-String
        return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output }
    }
    finally { $ErrorActionPreference = $prev }
}

function Invoke-Sql {
    param([Parameter(Mandatory)][string]$Query)
    return Invoke-Native -File $Snow -Arguments @('sql', '-c', $Connection, '--format', 'json', '-q', $Query)
}

Write-Step 'Publishing run inputs for the four-stage cloud pipeline'
Write-Note "run_id     = $RunId"
Write-Note "task       = $TaskFile"
Write-Note "connection = $Connection"

if (-not (Test-Path $Snow))     { Write-Fail "Snowflake CLI not found at $Snow"; exit 2 }
if (-not (Test-Path $WorkDir))  { New-Item -ItemType Directory -Path $WorkDir | Out-Null }

# --- 1. The task -----------------------------------------------------------------------
# Refuse rather than publish an empty task. A stage that reads an empty TASK.md is instructed
# to fail, so publishing one would burn four fires to reach a foregone conclusion.
if (-not (Test-Path $TaskFile)) {
    Write-Fail "No task file at $TaskFile"
    Write-Note 'Copy scripts\automation\TASK.template.md, fill it in, and pass it with -TaskFile.'
    exit 1
}
$taskText = (Get-Content $TaskFile -Raw)
if (-not $taskText -or $taskText.Trim().Length -lt 20) {
    Write-Fail 'Task file is empty or trivially short. Refusing to publish it.'
    exit 1
}

# --- 2. Build the snapshot from HEAD ---------------------------------------------------
Write-Step 'Building the source snapshot from HEAD'

# HEAD, never the working tree: this checkout routinely carries other sessions' in-flight
# work, and an unattended run must not review or patch against it.
#
# `training/` was moved to `archive/` in an earlier session and no longer exists in HEAD. That
# matters more than it looks: git archive FAILS OUTRIGHT on a pathspec that matches nothing, so
# the previously recorded pathspec would now produce no snapshot at all.
#
# GOTCHA: `*.md` is passed to git literally and matches .md at ANY depth, so markdown from
# nested directories comes along. That is text and harmless, but it is why the entry count is
# larger than the directory list suggests.
$paths = @(
    'dataforge', 'docs', 'tests', 'scripts', 'specs', 'packages', 'dataforge-mcp',
    'playground', 'archive', 'constitutions', 'requirements', 'fixtures',
    'benchmark_results', 'eval/thresholds', 'eval/preregistration', 'eval/results',
    '.github', 'pyproject.toml', 'Makefile', 'uv.lock', 'test_map.json', '*.md'
)

$snapshot = Join-Path $WorkDir 'dataforge-snapshot.tar.gz'
if (Test-Path $snapshot) { Remove-Item $snapshot -Force }

$archiveArgs = @('-C', $RepoRoot, 'archive', '--format=tar.gz', '-o', $snapshot, 'HEAD') + $paths
$res = Invoke-Native -File 'git' -Arguments $archiveArgs
if ($res.ExitCode -ne 0 -or -not (Test-Path $snapshot)) {
    Write-Fail "git archive failed (exit $($res.ExitCode))"
    Write-Note $res.Output
    Write-Note 'A pathspec matching nothing is fatal to git archive; check the $paths list above.'
    exit 1
}

$snapBytes = (Get-Item $snapshot).Length
$entries = (Invoke-Native -File 'tar' -Arguments @('-tzf', $snapshot)).Output -split "`r?`n" |
           Where-Object { $_.Trim() }
Write-Note "snapshot = $snapBytes bytes, $($entries.Count) entries"

# Data leakage check. The snapshot is deliberately small and excludes the datasets; if a
# future pathspec edit pulls data/ in, the upload silently becomes hundreds of megabytes.
$leak = @($entries | Where-Object { $_ -match '^data/' })
if ($leak.Count -gt 0) {
    Write-Fail "Snapshot contains $($leak.Count) entries under data/. Refusing to publish."
    exit 1
}

# The stages verify these hashes themselves. Compute them the same way the stages will:
# md5 for the snapshot, sha256 for the task.
$snapMd5  = (Get-FileHash -Path $snapshot -Algorithm MD5).Hash.ToLower()
$taskSha  = (Get-FileHash -Path $TaskFile -Algorithm SHA256).Hash.ToLower()
Write-Note "snapshot md5 = $snapMd5"
Write-Note "task sha256  = $taskSha"

# --- 3. Clear the previous run's handoffs ----------------------------------------------
$handoffs = @(
    '01-explore.md', '02-plan.md', '03-code.md', '04-verify.md',
    'changes.patch', 'COMMIT_MSG.txt', 'daily-review.md', 'MANIFEST.json'
)

# Tool cache directories, cleared by prefix. A fire's working directory is /workspace, so a
# stage that runs ruff, mypy or pytest without changing directory first creates these in the
# outbox. The prompts now tell every stage to cd into its source tree, but clearing them here
# too means one stage forgetting cannot leave debris that the next run has to tell apart from
# real deliverables.
$cachePrefixes = @('.mypy_cache/', '.ruff_cache/', '.pytest_cache/', '.benchmarks/', '.hypothesis/')

if ($KeepHandoffs) {
    Write-Step 'Keeping previous handoffs (-KeepHandoffs) - stale artifacts may be read as current'
}
else {
    Write-Step 'Clearing the previous run''s handoff artifacts'
    foreach ($h in $handoffs) {
        $r = Invoke-Sql -Query "REMOVE '$StagePath/$h'"
        # REMOVE exits 0 whether or not anything was there, so this cannot honestly report
        # which files existed. Say what was actually done rather than claiming a removal.
        if ($r.ExitCode -ne 0) { Write-Note "  $h : REMOVE failed (exit $($r.ExitCode))" }
        else { Write-Note "  $h : cleared" }
    }
    foreach ($c in $cachePrefixes) {
        $r = Invoke-Sql -Query "REMOVE '$StagePath/$c'"
        if ($r.ExitCode -ne 0) { Write-Note "  $c : REMOVE failed (exit $($r.ExitCode))" }
        else { Write-Note "  $c : cleared" }
    }
}

# --- 4. Write the manifest -------------------------------------------------------------
Write-Step 'Writing MANIFEST.json'

# No stage entries. Each stage appends its own, and every stage refuses unless the stages it
# depends on are present with status OK and the hashes match what it computes itself.
$manifest = [ordered]@{
    run_id         = $RunId
    task_sha256    = $taskSha
    snapshot_md5   = $snapMd5
    snapshot_bytes = $snapBytes
    snapshot_built = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH:mm:ssZ')
    published_from = $RepoRoot
    head_sha       = (Invoke-Native -File 'git' -Arguments @('-C', $RepoRoot, 'rev-parse', 'HEAD')).Output.Trim()
    stages         = [ordered]@{}
}
$manifestPath = Join-Path $WorkDir 'MANIFEST.json'
$manifestJson = $manifest | ConvertTo-Json -Depth 5

# ASCII, no BOM. A UTF-8 BOM breaks json.load() in the sandbox with a confusing
# "Expecting value: line 1 column 1" that reads like malformed JSON rather than an encoding
# problem, and every stage would refuse for the wrong reason.
[System.IO.File]::WriteAllText($manifestPath, $manifestJson, (New-Object System.Text.UTF8Encoding($false)))
Write-Note "head_sha = $($manifest.head_sha)"

# --- 5. Upload -------------------------------------------------------------------------
Write-Step 'Uploading to the workspace stage'

# AUTO_COMPRESS=FALSE for all three: the tarball is already gzipped, and gzipping the text
# files would make the fires fetch <name>.gz and find no file at the name they expect.
$uploads = @(
    @{ Local = $snapshot;     Name = 'dataforge-snapshot.tar.gz' },
    @{ Local = $TaskFile;     Name = 'TASK.md' },
    @{ Local = $manifestPath; Name = 'MANIFEST.json' }
)

foreach ($u in $uploads) {
    $src = ((Resolve-Path $u.Local).Path -replace '\\', '/')
    $r = Invoke-Sql -Query "PUT 'file://$src' '$StagePath/' AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    if ($r.ExitCode -ne 0) {
        Write-Fail "PUT failed for $($u.Name) (exit $($r.ExitCode))"
        Write-Note $r.Output
        exit 2
    }
    Write-Note "uploaded $($u.Name)"
}

# --- 6. Confirm ------------------------------------------------------------------------
# Verify by listing, not by trusting PUT's own report. Note that `cortex ws ls` misreports
# size and md5 for staged files (an upload has listed 4 bytes larger with a different md5 while
# round-tripping byte-identically), so treat presence as the assertion here and rely on the
# stages' own hash computation for integrity.
Write-Step 'Confirming the stage contents'
$ls = Invoke-Sql -Query "LS '$StagePath/'"
if ($ls.ExitCode -ne 0) {
    Write-Fail 'Uploads reported success but the stage could not be listed. State unknown.'
    exit 2
}
$names = @([regex]::Matches($ls.Output, '"name"\s*:\s*"([^"]+)"') |
           ForEach-Object { ($_.Groups[1].Value -split '/')[-1] })
Write-Note "stage now contains: $($names -join ', ')"

$missing = @($uploads | Where-Object { $names -notcontains $_.Name } | ForEach-Object { $_.Name })
if ($missing.Count -gt 0) {
    Write-Fail "Missing after upload: $($missing -join ', ')"
    exit 2
}

Write-Step 'Published'
Write-Note "Stage 1 fires at 00:30 Asia/Calcutta and will read TASK.md ($taskSha)."
exit 0
