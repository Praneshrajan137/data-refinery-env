<#
.SYNOPSIS
    Applies the daily automation patch, gates it, and opens a PR if it does not regress.

.DESCRIPTION
    The cloud automation cannot reach GitHub (the sandbox proxy blocks it), so it delivers a
    unified diff into the user workspace instead. This script is the local half of that
    pipeline: it retrieves the patch, applies it to a CLEAN worktree at origin/main, runs the
    real gates before and after, and opens a pull request only if nothing regressed.

    Three design decisions worth understanding before editing this file:

    1. It works in a `git worktree` at origin/main, NEVER in the developer's checkout. The
       checkout routinely carries unrelated in-flight changes (33 at the time of writing,
       including a mid-rename Makefile), so applying a patch there would make gate failures
       unattributable and could corrupt someone else's work.

    2. The PR condition is NO REGRESSION versus a baseline, not an absolute pass. The
       baseline may already be red from in-flight work on main, and a shared virtualenv can
       distort results. Measuring before and after in the same worktree cancels both, because
       whatever distorts one run distorts the other identically.

    3. It uses SQL PUT/GET rather than `cortex ws cp`, which is broken on Windows in both
       directions (upload fails with "undefined is not a directory"; download mangles the
       path to C:\C:\...).

    The script never merges, never pushes to main, and never creates a tag. A pull request
    cannot land by itself, so human review remains the last gate.

.PARAMETER DryRun
    Do everything except push the branch and open the PR. Reports what it would have done.

.PARAMETER PatchFile
    Use a local patch file instead of fetching from the workspace. For testing.

.PARAMETER SkipTests
    Skip `make test` (the two pytest passes). Speeds up a smoke test of the plumbing at the
    cost of the gate that matters most. Never use for a real run.

.PARAMETER Connection
    Snowflake connection profile used to retrieve the patch. Defaults to the headless
    key-pair profile. The interactive OAuth profile cannot be used unattended: it needs a
    browser once its cached token expires.

.EXAMPLE
    powershell -NoProfile -File scripts/automation/apply_and_pr.ps1
    powershell -NoProfile -File scripts/automation/apply_and_pr.ps1 -DryRun -PatchFile C:\tmp\test.patch

.NOTES
    Windows PowerShell 5.1. `pwsh` (PowerShell 7) is not installed on this machine.
#>
[CmdletBinding()]
param(
    [switch]$DryRun,
    [string]$PatchFile,
    [switch]$SkipTests,
    [string]$Connection = 'dataforge_automation'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RepoRoot   = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$Workspace  = 'USER$PRANESH07.PUBLIC.DEFAULT$'
$StagePath  = "snow://workspace/$Workspace/versions/live"
$WorkDir    = Join-Path $env:TEMP 'dataforge-automation'
$WorktreeDir = Join-Path $WorkDir 'worktree'
$Stamp      = (Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')
$Branch     = "automation/impl-$Stamp"

# Snowflake CLI and the gate interpreter live in their own venvs, deliberately NOT the repo
# .venv: a dependency added there would diverge from pyproject.toml and uv.lock and could
# perturb the long, exact file lists in `make lint` and `make type`.
$Snow = Join-Path $env:LOCALAPPDATA 'dataforge-automation\venv\Scripts\snow.exe'

# The gate venv carries the repo's [dev,playground] extras. It is repointed at the worktree
# on every run (see Set-GateVenvToWorktree) so that `import dataforge` resolves to the PATCHED
# source. Using the repo .venv here would be wrong: its
# __editable__.dataforge_07-0.1.0.pth installs a finder that hardcodes C:\dev\dataforge, so
# pytest would collect the worktree's tests while exercising the main checkout's code, and a
# runtime regression would be invisible.
$GatePython = Join-Path $env:LOCALAPPDATA 'dataforge-automation\gatevenv\Scripts\python.exe'

function Write-Step { param([string]$Message) Write-Host "`n==> $Message" -ForegroundColor Cyan }
function Write-Note { param([string]$Message) Write-Host "    $Message" -ForegroundColor DarkGray }
function Write-Fail { param([string]$Message) Write-Host "!!! $Message" -ForegroundColor Yellow }

# Native tools (git, gh, ruff, mypy, pytest) routinely write progress and diagnostics to
# stderr on SUCCESS. Under $ErrorActionPreference = 'Stop' PowerShell turns that into a
# terminating NativeCommandError, so `git worktree add` "fails" while having worked. Every
# external command therefore goes through here: stderr is captured as text, and the caller
# judges the outcome by the exit code, never by whether anything reached stderr.
function Invoke-Native {
    param(
        [Parameter(Mandatory)][string]$File,
        [string[]]$Arguments = @()
    )
    $prev = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $File @Arguments 2>&1 | Out-String
        return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $output }
    }
    finally {
        $ErrorActionPreference = $prev
    }
}

function Invoke-Git {
    param([Parameter(Mandatory)][string[]]$Arguments)
    return Invoke-Native -File 'git' -Arguments $Arguments
}

# Lists the workspace stage. Returns Ok plus the bare file names found.
#
# This exists because `GET` on a file that does not exist ALSO exits 1, so a GET exit code
# cannot distinguish "the fire implemented nothing" from "the connection is broken". Listing
# first separates the three outcomes properly:
#   LS fails            -> we know nothing, that is an error
#   LS ok, no patch      -> the fire implemented nothing, that is normal
#   LS ok, patch present -> fetch it, and a GET failure now really is an error
function Get-WorkspaceListing {
    if (-not (Test-Path $Snow)) {
        Write-Fail "RETRIEVAL FAILED: Snowflake CLI not found at $Snow"
        Write-Note 'Create it with: py -3.12 -m venv <dir> ; <dir>\Scripts\pip install snowflake-cli'
        return [pscustomobject]@{ Ok = $false; Names = @() }
    }

    $res = Invoke-Native -File $Snow -Arguments @(
        'sql', '-c', $Connection, '--format', 'json',
        '-q', "LS '$StagePath/'"
    )
    if ($res.ExitCode -ne 0) {
        Write-Fail "RETRIEVAL FAILED: snow sql LS exited $($res.ExitCode)"
        Write-Note $res.Output
        Write-Note "Check the '$Connection' profile in ~/.snowflake/connections.toml and that"
        Write-Note 'its private_key_file still matches the RSA key registered on the user.'
        return [pscustomobject]@{ Ok = $false; Names = @() }
    }

    # Names come back as /versions/live/<file>; reduce to the bare leaf.
    $names = @([regex]::Matches($res.Output, '"name"\s*:\s*"([^"]+)"') |
               ForEach-Object { ($_.Groups[1].Value -split '/')[-1] })
    return [pscustomobject]@{ Ok = $true; Names = $names }
}

# Retrieves one file from the workspace stage into $WorkDir. Only call this once
# Get-WorkspaceListing has confirmed the file is actually there.
function Get-WorkspaceFile {
    param([Parameter(Mandatory)][string]$Name)

    $dest = ($WorkDir -replace '\\', '/')
    $res = Invoke-Native -File $Snow -Arguments @(
        'sql', '-c', $Connection,
        '-q', "GET '$StagePath/$Name' 'file://$dest/'"
    )
    if ($res.ExitCode -ne 0) {
        Write-Fail "RETRIEVAL FAILED: snow sql exited $($res.ExitCode) fetching $Name"
        Write-Note $res.Output
        return $false
    }
    return $true
}

function Remove-StaleWorktree {
    if (Test-Path $WorktreeDir) {
        Write-Note "Removing stale worktree at $WorktreeDir"
        Invoke-Git @('-C', $RepoRoot, 'worktree', 'remove', '--force', $WorktreeDir) | Out-Null
        if (Test-Path $WorktreeDir) { Remove-Item $WorktreeDir -Recurse -Force }
    }
    Invoke-Git @('-C', $RepoRoot, 'worktree', 'prune') | Out-Null
}

# Repoints the gate venv's editable install at the worktree, then PROVES it worked.
#
# The assertion is the whole point. Without it the gate silently exercises whatever tree the
# editable finder happens to reference, which for the repo .venv is always C:\dev\dataforge.
# Note the check runs from a neutral cwd: from inside the worktree, Python would resolve
# `dataforge` from the current directory and the test would pass no matter what is installed.
function Set-GateVenvToWorktree {
    if (-not (Test-Path $GatePython)) {
        Write-Fail "Gate venv not found at $GatePython"
        Write-Note 'Create it with: py -3.12 -m venv <dir>, then <dir>\Scripts\pip install -e <repo>[dev,playground]'
        return $false
    }

    Write-Note 'repointing gate venv at the worktree'
    $res = Invoke-Native -File $GatePython -Arguments @('-m', 'pip', 'install', '-q', '-e', "$WorktreeDir[dev,playground]")
    if ($res.ExitCode -ne 0) {
        Write-Fail "Could not repoint the gate venv (exit $($res.ExitCode))"
        Write-Note $res.Output
        return $false
    }

    Push-Location 'C:\'
    try {
        $probe = Invoke-Native -File $GatePython -Arguments @('-c', 'import dataforge; print(dataforge.__file__)')
    }
    finally {
        Pop-Location
    }

    # Separate "the import blew up" from "the import resolved somewhere unexpected". These
    # need different verdicts: the first means the patch is broken (block it and say so), the
    # second means the gate itself is misconfigured (block, but do not blame the patch). An
    # earlier version reported a SyntaxError in the patched source as a gate misconfiguration,
    # because the traceback text happened to contain the string "dataforge".
    if ($probe.ExitCode -ne 0) {
        Write-Fail 'ABORTING: `import dataforge` FAILED in the patched tree.'
        Write-Note $probe.Output
        Write-Note 'The patch breaks importing the package, so no gate result would be meaningful.'
        return $false
    }

    $resolved = ($probe.Output -split "`r?`n" | Where-Object { $_.Trim() -match '__init__\.py$' } | Select-Object -Last 1)
    if ($resolved) { $resolved = $resolved.Trim() }
    if (-not $resolved -or -not $resolved.StartsWith($WorktreeDir, [StringComparison]::OrdinalIgnoreCase)) {
        Write-Fail 'ABORTING: the gate venv does not resolve dataforge to the worktree.'
        Write-Note "resolved to: $resolved"
        Write-Note "expected under: $WorktreeDir"
        Write-Note 'Gating would test the wrong source tree, so no result would be meaningful.'
        return $false
    }
    Write-Note "dataforge resolves to $resolved"
    return $true
}

# Runs the real Makefile targets. GNU Make is available, so there is no reason to approximate
# them: an earlier version of this script ran `ruff check dataforge tests scripts/ci` and
# `mypy --strict dataforge` while the PR body claimed "make lint / make type / make test".
# Real `make lint` is six commands over a much longer path list and real `make type` covers
# ~35 files beyond dataforge, so the PR was overstating what had been verified.
#
# PYTHON must be overridden: the Makefile prefers .venv/Scripts/python.exe and falls back to
# bare `python`, and the throwaway worktree has no .venv.
#
# The test gate deliberately does NOT use `make test`. That target runs `pytest -x`, and under
# `-n logical` on this machine `-x` produced 12 spurious ERRORs on an UNMODIFIED origin/main
# while the identical tree passed 2715/0 without `-x` (a known pytest-xdist shouldstop issue
# that pyproject.toml already comments on). Worse than the flakiness: `-x` aborts the session,
# so everything after the abort is never run and the gate looks stricter than it is. Running
# the whole suite and comparing counts is both more stable and more informative.
function Invoke-Gates {
    param([string]$Label)

    Write-Step "Gates ($Label)"
    $results = [ordered]@{}
    $makePython = $GatePython -replace '\\', '/'

    Push-Location $WorktreeDir
    try {
        Write-Note 'make lint (verbatim)'
        $results['lint'] = (Invoke-Native -File 'make' -Arguments @('lint', "PYTHON=$makePython")).ExitCode

        Write-Note 'make type (verbatim)'
        $results['type'] = (Invoke-Native -File 'make' -Arguments @('type', "PYTHON=$makePython")).ExitCode

        if ($SkipTests) {
            Write-Note 'tests SKIPPED (-SkipTests)'
            $results['test'] = 'skipped'
        }
        else {
            Write-Note 'pytest tests/ -n logical (no -x, see comment above)'
            $run = Invoke-Native -File $GatePython -Arguments @('-m', 'pytest', 'tests/', '-q', '-n', 'logical')
            $counts = Get-PytestCounts -Output $run.Output
            $results['test'] = $counts
            Write-Note $counts.Line
        }
    }
    finally {
        Pop-Location
    }

    foreach ($k in $results.Keys) {
        if ($results[$k] -is [string]) { Write-Note ("{0,-6} => {1}" -f $k, $results[$k]) }
        elseif ($results[$k] -is [hashtable]) { Write-Note ("{0,-6} => {1} failed, {2} errors" -f $k, $results[$k].Failed, $results[$k].Errors) }
        else { Write-Note ("{0,-6} => {1}" -f $k, $results[$k]) }
    }
    return $results
}

# A gate regressed only if it was passing and now is not. A gate that was already failing
# stays failing without blocking, because that failure is not the patch's fault.
# Extracts counts from a pytest summary line such as "2 failed, 2713 passed, 9 skipped".
#
# Counts matter, not the exit code. On this machine the suite is FLAKY in a worktree under
# `-n logical`: an unmodified origin/main produced "2715 passed, 12 errors" on one run and
# "2715 passed, 0 errors" on another. Comparing exit codes therefore hides real breakage,
# because a baseline already nonzero for a spurious reason makes later failures look like no
# change. Not hypothetical: a patch inverting a branch of SafetyFilter.confirms produced 2
# genuine test failures and was WAVED THROUGH by exit-code comparison, with lint and type
# both clean. Only the failure count caught it.
function Get-PytestCounts {
    param([string]$Output)
    $line = ($Output -split "`r?`n" | Where-Object { $_ -match '\d+ (passed|failed|error)' } | Select-Object -Last 1)
    $text = ''
    if ($line) { $text = $line.Trim() }
    $counts = @{ Failed = 0; Errors = 0; Passed = 0; Line = $text }
    if ($text -match '(\d+) failed') { $counts.Failed = [int]$Matches[1] }
    if ($text -match '(\d+) error')  { $counts.Errors = [int]$Matches[1] }
    if ($text -match '(\d+) passed') { $counts.Passed = [int]$Matches[1] }
    return $counts
}

# Returns a comma-separated list of regressed gate names, or an empty string if none.
# A string rather than an array on purpose: PowerShell unwraps an empty array to $null (and
# $null.Count throws under StrictMode), while `,$arr` wraps it one level too deep and reports
# Count 1 for an empty result. A string has neither failure mode.
function Get-Regressions {
    param($Before, $After)
    $regressed = New-Object System.Collections.Generic.List[string]

    foreach ($k in @('lint', 'type')) {
        if ($Before[$k] -eq 'skipped' -or $After[$k] -eq 'skipped') { continue }
        if ($Before[$k] -eq 0 -and $After[$k] -ne 0) { $regressed.Add($k) }
    }

    # Tests compare FAILURE COUNTS, so a flaky-red baseline cannot mask new breakage.
    if ($Before['test'] -isnot [string] -and $After['test'] -isnot [string]) {
        if ($After['test'].Failed -gt $Before['test'].Failed) {
            $regressed.Add("test ($($Before['test'].Failed) -> $($After['test'].Failed) failed)")
        }
    }

    return ($regressed -join ', ')
}

# ---------------------------------------------------------------------------------------

Write-Step "DataForge automation: apply, gate, and open a PR"
Write-Note "repo       = $RepoRoot"
Write-Note "branch     = $Branch"
Write-Note "connection = $Connection"
Write-Note "dry run    = $DryRun"

if (-not (Test-Path $GatePython)) { throw "Gate venv python not found at $GatePython. Create it first." }

if (-not (Test-Path $WorkDir)) { New-Item -ItemType Directory -Path $WorkDir | Out-Null }

# --- 1. Obtain the patch ---------------------------------------------------------------
Write-Step 'Retrieving the patch'

if ($PatchFile) {
    $localPatch = (Resolve-Path $PatchFile).Path
    Write-Note "Using supplied patch: $localPatch"
}
else {
    $localPatch = Join-Path $WorkDir 'changes.patch'
    if (Test-Path $localPatch) { Remove-Item $localPatch }

    $listing = Get-WorkspaceListing
    if (-not $listing.Ok) {
        Write-Fail 'Could not determine whether a patch exists. Exiting NONZERO deliberately.'
        Write-Note 'This is NOT the same as "the fire implemented nothing" - we simply do not know.'
        exit 2
    }
    Write-Note "workspace contains: $($listing.Names -join ', ')"

    if ($listing.Names -notcontains 'changes.patch') {
        Write-Note 'Retrieval worked; there is no changes.patch in the workspace.'
        Write-Note 'The fire implemented nothing, or has not run since the last pickup.'
        Write-Note 'Exiting 0. This is a normal outcome, not an error.'
        exit 0
    }

    if (-not (Get-WorkspaceFile -Name 'changes.patch')) { exit 2 }
    if (-not (Test-Path $localPatch)) {
        Write-Fail 'RETRIEVAL FAILED: GET reported success but no local file appeared.'
        exit 2
    }
}

$patchBytes = (Get-Item $localPatch).Length
Write-Note "patch size = $patchBytes bytes"
if ($patchBytes -eq 0) {
    Write-Fail 'Patch is empty. Nothing to do.'
    exit 0
}

# --- 2. Clean worktree at origin/main --------------------------------------------------
Write-Step 'Preparing a clean worktree at origin/main'
Remove-StaleWorktree
Invoke-Git @('-C', $RepoRoot, 'fetch', 'origin') | Out-Null

# core.autocrlf is true on this machine and there is no .gitattributes, so a normal checkout
# produces CRLF files. The snapshot handed to the fire comes from `git archive`, which emits
# the LF blob content, so the fire's patch carries LF context lines. Applying an LF patch to a
# CRLF worktree fails on every hunk with "patch does not apply". Forcing autocrlf=false for
# this throwaway worktree makes it LF, matching the snapshot the patch was generated against.
# Committing from an LF worktree is harmless: git normalizes to LF in the object store anyway.
$wt = Invoke-Git @('-c', 'core.autocrlf=false', '-C', $RepoRoot, 'worktree', 'add', '--detach', $WorktreeDir, 'origin/main')
if (-not (Test-Path (Join-Path $WorktreeDir 'PRODUCT.md'))) {
    Write-Note $wt.Output
    throw "Worktree creation failed: $WorktreeDir does not look like the repo."
}

# Verify the line endings actually came out LF. If this assertion ever fails, every patch will
# be rejected as corrupt, and the cause is very hard to see from the error message alone.
$probe = [System.IO.File]::ReadAllBytes((Join-Path $WorktreeDir 'PRODUCT.md'))
$crlfCount = 0
for ($i = 1; $i -lt $probe.Length; $i++) { if ($probe[$i] -eq 10 -and $probe[$i - 1] -eq 13) { $crlfCount++ } }
if ($crlfCount -gt 0) {
    Write-Fail "Worktree checked out with CRLF ($crlfCount occurrences in PRODUCT.md)."
    Write-Note 'An LF patch from the sandbox cannot apply to a CRLF worktree. Aborting rather than reporting a misleading corrupt-patch error.'
    exit 1
}
Write-Note 'worktree line endings = LF (matches the snapshot)'

$baseSha = (Invoke-Git @('-C', $WorktreeDir, 'rev-parse', '--short', 'HEAD')).Output.Trim()
Write-Note "worktree at origin/main = $baseSha"

try {
    # --- 3. Baseline -------------------------------------------------------------------
    if (-not (Set-GateVenvToWorktree)) { exit 1 }

    $before = Invoke-Gates -Label "baseline @ $baseSha"

    # --- 4. Apply ---------------------------------------------------------------------
    Write-Step 'Applying the patch'
    # -p1 strips the leading base/ or src/ component produced by `diff -ruN base src`.
    $apply = Invoke-Git @('-C', $WorktreeDir, 'apply', '--3way', '-p1', $localPatch)
    if ($apply.ExitCode -ne 0) {
        Write-Note $apply.Output
        Write-Fail "Patch did not apply cleanly (exit $($apply.ExitCode)). No PR."
        Write-Note 'Most likely the snapshot is stale relative to origin/main. Rebuild it.'
        exit 1
    }

    # `git apply --3way` STAGES what it applies, so `git diff --name-only` (unstaged only)
    # reports nothing and the patch looks like a no-op. Diff against HEAD to catch staged and
    # unstaged alike. The --3way pass also prints "repository lacks the necessary blob" for a
    # `diff -ruN` patch, which carries no index lines, then falls back to direct application
    # and succeeds: that message is noise, not failure.
    $tracked = (Invoke-Git @('-C', $WorktreeDir, 'diff', 'HEAD', '--name-only')).Output -split "`r?`n"
    $untracked = (Invoke-Git @('-C', $WorktreeDir, 'ls-files', '--others', '--exclude-standard')).Output -split "`r?`n"
    $changed = @($tracked + $untracked | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_.Trim() })
    Write-Note "files changed = $($changed.Count)"
    foreach ($f in $changed) { Write-Note "  $f" }

    if ($changed.Count -eq 0) {
        Write-Fail 'Patch applied but changed nothing. No PR.'
        exit 0
    }

    # The fire is told not to touch these. Enforce it here rather than trusting the prompt:
    # a protected-path edit is a hard stop, not a regression to be weighed.
    $protected = @('PRODUCT.md', 'DECISIONS.md', 'CLAUDE.md', 'docs/quantitative_claims.yaml')
    $violations = @($changed | Where-Object {
        $p = $_ -replace '\\', '/'
        ($protected -contains $p) -or $p.StartsWith('docs/trust/') -or $p.StartsWith('eval/results/')
    })
    if ($violations.Count -gt 0) {
        Write-Fail "Patch touches protected paths. No PR."
        foreach ($v in $violations) { Write-Note "  PROTECTED: $v" }
        exit 1
    }

    # --- 5. Re-gate -------------------------------------------------------------------
    # Repoint again: the patch may have changed packaging metadata, and this is cheap.
    if (-not (Set-GateVenvToWorktree)) { exit 1 }

    $after = Invoke-Gates -Label 'after patch'

    $regressed = Get-Regressions -Before $before -After $after
    if ($regressed) {
        Write-Fail "Patch REGRESSED: $regressed. No PR."
        Write-Note 'The patch is preserved at the path above if you want to inspect it.'
        exit 1
    }
    Write-Note 'No regression. Proceeding.'

    # --- 6. Branch, commit, push, PR --------------------------------------------------
    Write-Step 'Opening the pull request'

    $reviewLocal = Join-Path $WorkDir 'daily-review.md'
    if (-not $PatchFile) {
        if (Test-Path $reviewLocal) { Remove-Item $reviewLocal }
        # A missing review is not fatal: the patch is the deliverable and the gates already
        # passed. Note it in the PR body rather than discarding verified work over a doc.
        Get-WorkspaceFile -Name 'daily-review.md' | Out-Null
    }

    $prBody = @"
Automated implementation from the daily Cortex Code automation.

**Do not merge without reading the diff.** This was written by an unattended agent. It ran
its own tests in the sandbox, but the authoritative gates below were run here, against a
clean worktree at ``origin/main`` with the patch applied, in a venv repointed so that
``import dataforge`` resolves to the patched tree.

| Gate | Baseline @ $baseSha | After patch |
| --- | --- | --- |
| ``make lint`` (verbatim) | $($before['lint']) | $($after['lint']) |
| ``make type`` (verbatim) | $($before['type']) | $($after['type']) |
| ``pytest tests/ -n logical`` | $(if ($before['test'] -is [string]) { $before['test'] } else { "$($before['test'].Failed) failed, $($before['test'].Errors) errors" }) | $(if ($after['test'] -is [string]) { $after['test'] } else { "$($after['test'].Failed) failed, $($after['test'].Errors) errors" }) |

The test row compares **failure counts**, not exit codes: this suite is flaky in a worktree
under ``-n logical`` (an unmodified ``origin/main`` has produced 0 and 12 collection errors on
consecutive runs), and comparing exit codes let a real 2-test regression through unblocked.

A gate that was already failing at baseline does not block, since that failure predates this
patch. Only a pass-to-fail transition blocks.

Files changed: $($changed.Count)

---

$(if (Test-Path $reviewLocal) { Get-Content $reviewLocal -Raw } else { '_No review file was retrieved._' })
"@

    if ($DryRun) {
        Write-Note 'DRY RUN: would create branch, commit, push, and open a PR.'
        Write-Note "  branch = $Branch"
        Write-Note "  body   = $($prBody.Length) chars"
        exit 0
    }

    Invoke-Git @('-C', $WorktreeDir, 'checkout', '-b', $Branch) | Out-Null
    Invoke-Git (@('-C', $WorktreeDir, 'add', '--') + $changed) | Out-Null
    Invoke-Git @(
        '-C', $WorktreeDir,
        '-c', 'user.email=automation@dataforge.local',
        '-c', 'user.name=DataForge Automation',
        'commit',
        '-m', "Automated implementation $Stamp",
        '-m', 'Written by the daily unattended automation. Gates were run locally against a clean worktree at origin/main; see the PR body for baseline and post-patch results. The agent could not run the test suite itself.'
    ) | Out-Null

    $push = Invoke-Git @('-C', $WorktreeDir, 'push', 'origin', $Branch)
    Write-Note $push.Output
    if ($push.ExitCode -ne 0) {
        Write-Fail "Push failed (exit $($push.ExitCode)). No PR."
        exit 1
    }

    $bodyFile = Join-Path $WorkDir 'pr_body.md'
    Set-Content -Path $bodyFile -Value $prBody -Encoding UTF8

    Push-Location $WorktreeDir
    try {
        $pr = Invoke-Native -File 'gh' -Arguments @(
            'pr', 'create', '--base', 'main', '--head', $Branch,
            '--title', "Automated implementation $Stamp",
            '--body-file', $bodyFile
        )
        Write-Host $pr.Output
        if ($pr.ExitCode -ne 0) { Write-Fail "gh pr create failed (exit $($pr.ExitCode)). Branch is pushed; open the PR manually." }
    }
    finally {
        Pop-Location
    }

    Write-Step 'Done'
}
finally {
    Write-Step 'Cleaning up the worktree'
    Remove-StaleWorktree
    Write-Note 'Developer checkout was never touched.'
}
