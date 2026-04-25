/**
 * DataForge Playground frontend.
 *
 * This file assumes Cloudflare Pages serves the static UI and the backend URL
 * is provided through config.js at runtime.
 */

const BACKEND_URL = String(window.__DATAFORGE_CONFIG__?.BACKEND_URL ?? "").trim();
const HEALTH_TIMEOUT_MS = 3000;
const HEALTH_MAX_RETRIES = 8;
const HEALTH_INITIAL_DELAY_MS = 1000;

const statusBanner = document.getElementById("status-banner");
const statusText = document.getElementById("status-text");
const warmupProgress = document.getElementById("warmup-progress");

const csvUpload = document.getElementById("csv-upload");
const sampleSelect = document.getElementById("sample-select");
const advancedToggle = document.getElementById("advanced-toggle");
const profileBtn = document.getElementById("profile-btn");
const repairBtn = document.getElementById("repair-btn");

const resultsSection = document.getElementById("results-section");
const tabProfile = document.getElementById("tab-profile");
const tabRepair = document.getElementById("tab-repair");
const tabRevert = document.getElementById("tab-revert");

const panelProfile = document.getElementById("panel-profile");
const panelRepair = document.getElementById("panel-repair");
const panelRevert = document.getElementById("panel-revert");

const profileLoading = document.getElementById("profile-loading");
const profileResults = document.getElementById("profile-results");
const repairLoading = document.getElementById("repair-loading");
const repairResults = document.getElementById("repair-results");
const revertJournal = document.getElementById("revert-journal");

const tabs = [tabProfile, tabRepair, tabRevert];
const panels = [panelProfile, panelRepair, panelRevert];

let currentFile = null;
let backendReady = false;
let requestInFlight = false;
let advancedAvailable = false;
let activeTabIndex = 0;

function backendPath(path) {
    return BACKEND_URL ? `${BACKEND_URL}${path}` : path;
}

async function checkHealth() {
    showBanner("Warming up the backend...", true);
    let delay = HEALTH_INITIAL_DELAY_MS;

    for (let attempt = 0; attempt < HEALTH_MAX_RETRIES; attempt += 1) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), HEALTH_TIMEOUT_MS);
            const response = await fetch(backendPath("/api/health"), {
                signal: controller.signal,
            });
            clearTimeout(timeout);

            if (response.ok) {
                const payload = await response.json();
                backendReady = true;
                advancedAvailable = Boolean(payload.advanced_available);
                configureAdvancedToggle();
                hideBanner();
                updateControls();
                return;
            }
        } catch {
            // Keep retrying while the Space wakes up.
        }

        statusText.textContent = `Warming up the backend... (attempt ${attempt + 2}/${HEALTH_MAX_RETRIES})`;
        await sleep(delay);
        delay = Math.min(delay * 2, 8000);
    }

    showBanner(
        "Backend is unavailable. The Space may be sleeping. Try refreshing in 30 seconds.",
        false,
    );
    backendReady = false;
    configureAdvancedToggle();
    updateControls();
}

function configureAdvancedToggle() {
    if (!advancedAvailable) {
        advancedToggle.checked = false;
        advancedToggle.title = "Advanced mode requires a backend provider key.";
    } else {
        advancedToggle.title = "Use backend-configured LLM assistance when available.";
    }
    updateControls();
}

function setActiveTab(index, options = {}) {
    const focus = Boolean(options.focus);
    activeTabIndex = index;

    tabs.forEach((tab, tabIndex) => {
        const isActive = tabIndex === index;
        tab.setAttribute("aria-selected", isActive ? "true" : "false");
        tab.setAttribute("tabindex", isActive ? "0" : "-1");
        tab.classList.toggle("tab-active", isActive);
        panels[tabIndex].classList.toggle("tab-panel--hidden", !isActive);
        if (isActive && focus) {
            tab.focus();
        }
    });
}

function handleTabKeydown(event) {
    if (!["ArrowRight", "ArrowLeft", "Home", "End"].includes(event.key)) {
        return;
    }

    event.preventDefault();

    if (event.key === "Home") {
        setActiveTab(0, { focus: true });
        return;
    }
    if (event.key === "End") {
        setActiveTab(tabs.length - 1, { focus: true });
        return;
    }

    const direction = event.key === "ArrowRight" ? 1 : -1;
    const nextIndex = (activeTabIndex + direction + tabs.length) % tabs.length;
    setActiveTab(nextIndex, { focus: true });
}

tabs.forEach((tab, index) => {
    tab.addEventListener("click", () => setActiveTab(index));
    tab.addEventListener("keydown", handleTabKeydown);
});

csvUpload.addEventListener("change", () => {
    const [file] = csvUpload.files;
    currentFile = file ?? null;
    if (file) {
        sampleSelect.value = "";
    }
    updateControls();
});

sampleSelect.addEventListener("change", async () => {
    const sampleName = sampleSelect.value;
    if (!sampleName) {
        currentFile = null;
        updateControls();
        return;
    }

    try {
        const response = await fetch(backendPath(`/api/samples/${sampleName}`));
        if (!response.ok) {
            throw new Error(`Failed to fetch sample: ${response.status}`);
        }
        const blob = await response.blob();
        currentFile = new File([blob], `${sampleName}.csv`, { type: "text/csv" });
        csvUpload.value = "";
        updateControls();
    } catch (error) {
        currentFile = null;
        updateControls();
        showError(profileResults, `Failed to load sample: ${error.message}`);
    }
});

profileBtn.addEventListener("click", async () => {
    if (!currentFile || !backendReady || requestInFlight) {
        return;
    }

    setActiveTab(0);
    resultsSection.classList.remove("results-hidden");
    profileResults.innerHTML = "";
    showLoading(profileLoading, true);
    setRequestInFlight(true);

    const formData = new FormData();
    formData.append("file", currentFile);

    const params = new URLSearchParams();
    if (advancedToggle.checked) {
        params.set("advanced", "true");
    }

    try {
        const url = backendPath(`/api/profile${params.toString() ? `?${params}` : ""}`);
        const response = await fetch(url, { method: "POST", body: formData });
        const handled = await handleApiError(response, profileResults);
        if (handled) {
            return;
        }
        renderProfileResults(await response.json());
    } catch (error) {
        showError(profileResults, `Profile failed: ${error.message}`);
    } finally {
        showLoading(profileLoading, false);
        setRequestInFlight(false);
    }
});

repairBtn.addEventListener("click", async () => {
    if (!currentFile || !backendReady || requestInFlight) {
        return;
    }

    setActiveTab(1);
    resultsSection.classList.remove("results-hidden");
    repairResults.innerHTML = "";
    showLoading(repairLoading, true);
    setRequestInFlight(true);

    const formData = new FormData();
    formData.append("file", currentFile);

    const params = new URLSearchParams({ dry_run: "true" });
    if (advancedToggle.checked) {
        params.set("advanced", "true");
    }

    try {
        const response = await fetch(backendPath(`/api/repair?${params}`), {
            method: "POST",
            body: formData,
        });
        const handled = await handleApiError(response, repairResults);
        if (handled) {
            return;
        }
        const payload = await response.json();
        renderRepairResults(payload);
        renderRevertJournal(payload.txn_journal);
    } catch (error) {
        showError(repairResults, `Repair failed: ${error.message}`);
    } finally {
        showLoading(repairLoading, false);
        setRequestInFlight(false);
    }
});

async function handleApiError(response, container) {
    if (response.ok) {
        return false;
    }

    if (response.status === 413) {
        showError(container, "File too large. Maximum upload size is 1 MB.");
        return true;
    }
    if (response.status === 429) {
        showError(container, "Too many requests from this browser session. Please wait a minute.");
        return true;
    }
    if (response.status === 400) {
        const payload = await response.json();
        if (payload.detail?.error === "advanced_mode_unavailable") {
            advancedAvailable = false;
            configureAdvancedToggle();
            showError(container, "Advanced mode is unavailable because no provider key is configured.");
            return true;
        }
        if (payload.detail?.error === "apply_not_supported") {
            showError(container, "Playground repairs are dry-run only. Use the CLI to apply changes.");
            return true;
        }
    }

    showError(container, `Request failed with status ${response.status}.`);
    return true;
}

function renderProfileResults(payload) {
    const { issues, meta } = payload;

    let html = `<div class="meta-summary">
        <strong>${meta.rows}</strong> rows · <strong>${meta.columns}</strong> columns ·
        <strong>${meta.total_issues}</strong> issue${meta.total_issues !== 1 ? "s" : ""} detected
    </div>`;

    if (issues.length === 0) {
        html += '<p class="no-issues">No data-quality issues detected.</p>';
    } else {
        html += `<div class="table-wrapper"><table role="grid">
            <thead>
                <tr>
                    <th scope="col">Column</th>
                    <th scope="col">Issue Type</th>
                    <th scope="col">Severity</th>
                    <th scope="col">Rows Affected</th>
                    <th scope="col">Count</th>
                </tr>
            </thead>
            <tbody>`;

        for (const issue of issues) {
            const rows = issue.row_indices.length <= 5
                ? issue.row_indices.join(", ")
                : `${issue.row_indices.slice(0, 5).join(", ")}...`;
            html += `<tr>
                <td><code>${escapeHtml(issue.column)}</code></td>
                <td>${escapeHtml(issue.issue_type)}</td>
                <td>${severityBadge(issue.severity)}</td>
                <td class="row-indices">${rows}</td>
                <td>${issue.count}</td>
            </tr>`;
        }

        html += "</tbody></table></div>";
    }

    profileResults.innerHTML = html;
}

function renderRepairResults(payload) {
    const { fixes } = payload;

    if (fixes.length === 0) {
        repairResults.innerHTML = '<p class="no-issues">No repairs proposed. The data looks clean.</p>';
        return;
    }

    let html = `<div class="meta-summary">
        <strong>${fixes.length}</strong> repair${fixes.length !== 1 ? "s" : ""} proposed (dry run)
    </div>`;

    html += '<pre class="diff-view">';
    for (const fix of fixes) {
        html += `<span class="diff-header">--- Row ${fix.row}, Column: ${escapeHtml(fix.column)} (${escapeHtml(fix.detector_id)})</span>\n`;
        html += `<span class="deletion">- ${escapeHtml(fix.old_value)}</span>\n`;
        html += `<span class="addition">+ ${escapeHtml(fix.new_value)}</span>\n`;
        html += `<span class="diff-reason">  ${escapeHtml(fix.reason)}</span>\n\n`;
    }
    html += "</pre>";

    repairResults.innerHTML = html;
}

function renderRevertJournal(journal) {
    if (!journal) {
        revertJournal.textContent = "No transaction journal available.";
        revertJournal.classList.add("journal-empty");
        return;
    }

    revertJournal.textContent = JSON.stringify(journal, null, 2);
    revertJournal.classList.remove("journal-empty");
}

function setRequestInFlight(value) {
    requestInFlight = value;
    updateControls();
}

function updateControls() {
    const hasFile = currentFile !== null;
    csvUpload.disabled = requestInFlight;
    sampleSelect.disabled = requestInFlight;
    advancedToggle.disabled = requestInFlight || !advancedAvailable;
    profileBtn.disabled = !hasFile || !backendReady || requestInFlight;
    repairBtn.disabled = !hasFile || !backendReady || requestInFlight;
}

function showBanner(message, showProgress) {
    statusText.textContent = message;
    warmupProgress.style.display = showProgress ? "block" : "none";
    statusBanner.classList.remove("banner--hidden");
    statusBanner.classList.add("banner--visible");
}

function hideBanner() {
    statusBanner.classList.add("banner--hidden");
    statusBanner.classList.remove("banner--visible");
}

function showLoading(element, show) {
    element.classList.toggle("loading-hidden", !show);
    element.classList.toggle("loading-visible", show);
}

function showError(container, message) {
    container.innerHTML = `<article class="error-card"><p>${escapeHtml(message)}</p></article>`;
}

function severityBadge(severity) {
    return `<span class="badge badge--${severity}">${severity.toUpperCase()}</span>`;
}

function escapeHtml(text) {
    const div = document.createElement("div");
    div.textContent = String(text);
    return div.innerHTML;
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

document.addEventListener("DOMContentLoaded", () => {
    setActiveTab(0);
    configureAdvancedToggle();
    updateControls();
    void checkHealth();
});
