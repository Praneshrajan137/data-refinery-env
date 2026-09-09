<#
.SYNOPSIS
    Creates the four chained cloud automations (explore, plan, code, verify) and optionally
    drops the superseded monolithic one.

.DESCRIPTION
    Each stage's prompt is assembled at create time from two files: the shared preamble
    (prompts/_preamble.md) and the stage body (prompts/stage<N>-*.md). Assembling rather than
    duplicating means the environment facts, the budget rule and the handoff contract exist in
    exactly one place; four copies would drift, and a drifted copy in an unattended prompt is
    invisible until a fire misbehaves.

    WHY FOUR AUTOMATIONS AND NOT ONE
    A fire is hard-killed at roughly 15 minutes. The predecessor
    (COCO_ROUTINE_PROJECT) asked one fire to explore, plan, implement, test and report; on
    2026-09-06 and 2026-09-07 it was killed at exactly 15 min 01 s having written nothing,
    while the enclosing task still reported SUCCEEDED. Splitting the work multiplies the budget
    and, more importantly, makes partial progress durable: each stage's output is on the stage
    mount before the next one starts.

    WHY 30 MINUTES APART
    Each stage needs its own 15-minute wall plus slack for a slow start. 30-minute spacing
    gives 100% headroom and still finishes an hour before the local pickup at 03:00, which
    applies the patch and opens the pull request.

    ACCESS MODE
    These are created with --without-read-only --force, i.e. fully READ-WRITE, at the user's
    explicit instruction. Recorded honestly: no stage needs SQL DML - all four work on the
    sandbox filesystem, which already succeeds under the read-only default - so this grants
    four unattended fires per night the ability to run DML on any database with the user's
    ACCOUNTADMIN token and no human present. To tighten it later, drop the two flags on the
    New-Stage call below and re-run with -Recreate; nothing else needs to change.

.PARAMETER DryRun
    Print the generated task metadata and SQL for each stage without creating anything.

.PARAMETER Recreate
    Drop each stage automation before creating it. Use after editing a prompt file, since
    prompts are baked into the task definition at create time and editing the .md files on disk
    does NOT change an already-created automation.

.PARAMETER DropMonolith
    Also drop COCO_ROUTINE_PROJECT, the single-fire predecessor this pipeline replaces.

.PARAMETER Only
    Create just one stage, by number. For iterating on a single prompt.

.PARAMETER Connection
    Snowflake connection the automations are created on. Must be the connection whose account
    owns the workspace stage and the existing automations, otherwise the CLI refuses: the
    CREATE AGENT TASK runs on the SQL connection while the fire's thread is only visible
    through the agent connection's account, and a split between the two would leave fires whose
    transcripts cannot be read. AEGIS15 is the interactive OAuth profile, which is correct here
    because this script is run by hand; the unattended half of the pipeline uses the headless
    key-pair profile instead.

.EXAMPLE
    powershell -NoProfile -File scripts\automation\create_stages.ps1 -DryRun
    powershell -NoProfile -File scripts\automation\create_stages.ps1 -Recreate -DropMonolith

.NOTES
    Windows PowerShell 5.1. Requires the `cortex` CLI on PATH and signed in.
#>
[CmdletBinding()]
param(
    [switch]$DryRun,
    [switch]$Recreate,
    [switch]$DropMonolith,
    [ValidateRange(1, 4)][int]$Only,
    [string]$Connection = 'AEGIS15'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$PromptDir = Join-Path $PSScriptRoot 'prompts'
$Workspace = 'USER$PRANESH07.PUBLIC.DEFAULT$'
$Timezone  = 'Asia/Calcutta'   # this machine is UTC+05:30; the CLI default is UTC
$BuildDir  = Join-Path $env:TEMP 'dataforge-automation\prompts'

function Write-Step { param([string]$Message) Write-Host "`n==> $Message" -ForegroundColor Cyan }
function Write-Note { param([string]$Message) Write-Host "    $Message" -ForegroundColor DarkGray }
function Write-Fail { param([string]$Message) Write-Host "!!! $Message" -ForegroundColor Yellow }

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

# The four stages. Cron rather than natural language so the spacing is unambiguous.
$stages = @(
    [pscustomobject]@{ N = 1; Name = 'DF_STAGE1_EXPLORE'; File = 'stage1-explore.md';       Cron = '30 0 * * *'; At = '00:30' }
    [pscustomobject]@{ N = 2; Name = 'DF_STAGE2_PLAN';    File = 'stage2-plan.md';          Cron = '0 1 * * *';  At = '01:00' }
    [pscustomobject]@{ N = 3; Name = 'DF_STAGE3_CODE';    File = 'stage3-code.md';          Cron = '30 1 * * *'; At = '01:30' }
    [pscustomobject]@{ N = 4; Name = 'DF_STAGE4_VERIFY';  File = 'stage4-verify-commit.md'; Cron = '0 2 * * *';  At = '02:00' }
)

if ($Only) { $stages = @($stages | Where-Object { $_.N -eq $Only }) }

Write-Step 'Assembling stage prompts'

$preamblePath = Join-Path $PromptDir '_preamble.md'
if (-not (Test-Path $preamblePath)) { throw "Missing preamble at $preamblePath" }
$preamble = Get-Content $preamblePath -Raw

if (-not (Test-Path $BuildDir)) { New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null }

foreach ($s in $stages) {
    $bodyPath = Join-Path $PromptDir $s.File
    if (-not (Test-Path $bodyPath)) { throw "Missing stage prompt at $bodyPath" }
    $combined = $preamble.TrimEnd() + "`n`n" + (Get-Content $bodyPath -Raw)

    # A prompt containing '$$' collides with the SQL body delimiter in the generated
    # CREATE AGENT TASK. Catch it here with a clear message rather than at Snowflake with an
    # opaque syntax error.
    if ($combined -match '\$\$') {
        throw "Prompt for $($s.Name) contains '`$`$', which collides with the SQL body delimiter. Rephrase it."
    }

    $out = Join-Path $BuildDir ("{0}.md" -f $s.Name)
    [System.IO.File]::WriteAllText($out, $combined, (New-Object System.Text.UTF8Encoding($false)))
    $s | Add-Member -NotePropertyName PromptPath -NotePropertyValue $out
    Write-Note ("{0,-18} {1,6} chars  fires {2} {3}" -f $s.Name, $combined.Length, $s.At, $Timezone)
}

if ($DropMonolith) {
    Write-Step 'Dropping the superseded monolithic automation'
    $r = Invoke-Native -File 'cortex' -Arguments @('automation', 'drop', 'COCO_ROUTINE_PROJECT', '--connection', $Connection)
    if ($r.ExitCode -ne 0) { Write-Note 'COCO_ROUTINE_PROJECT not dropped (may already be gone)' }
    else { Write-Note 'COCO_ROUTINE_PROJECT dropped' }
    Write-Note $r.Output.Trim()
}

Write-Step 'Creating the stage automations'

$failed = New-Object System.Collections.Generic.List[string]

foreach ($s in $stages) {
    if ($Recreate -and -not $DryRun) {
        # Prompts are baked in at create time, so editing a .md file has no effect on an
        # existing automation. Dropping first is the only way an edit takes effect.
        $d = Invoke-Native -File 'cortex' -Arguments @('automation', 'drop', $s.Name, '--connection', $Connection)
        if ($d.ExitCode -eq 0) { Write-Note "$($s.Name): dropped before recreate" }
    }

    $args = @(
        'automation', 'create',
        '--connection', $Connection,
        '--name', $s.Name,
        '--prompt-file', $s.PromptPath,
        '--schedule', $s.Cron,
        '--timezone', $Timezone,
        '--workspace', $Workspace,
        # Persisting /workspace across fires IS the handoff mechanism. With --no-workspace the
        # mount is ephemeral per fire and every stage would find its inputs missing.
        '--without-read-only', '--force'
    )
    if ($DryRun) { $args += '--dry-run' }

    $r = Invoke-Native -File 'cortex' -Arguments $args
    if ($r.ExitCode -ne 0) {
        Write-Fail "$($s.Name): create failed (exit $($r.ExitCode))"
        Write-Note $r.Output
        $failed.Add($s.Name)
    }
    else {
        Write-Note "$($s.Name): ok"
        if ($DryRun) { Write-Host $r.Output }
    }
}

if ($failed.Count -gt 0) {
    Write-Fail "Failed: $($failed -join ', ')"
    exit 1
}

Write-Step 'Done'
if (-not $DryRun) {
    Write-Note 'Verify with: cortex automation list'
    Write-Note 'Inspect a stage with: cortex automation doctor DF_STAGE1_EXPLORE'
    Write-Note 'Remember that inputs must be published BEFORE 00:30 by publish_run_inputs.ps1.'
}
exit 0
