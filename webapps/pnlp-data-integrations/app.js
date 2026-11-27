// Tableau de bord des intégrations de données vers DHIS2 NMDR (PNLP).
// Lit le fichier produit par le pipeline "PNLP Integrations Status" et superpose l'état en direct des pipelines.

const STATUS_FILE = "pipelines/pnlp_integrations_status/output/status_latest.json";
const COLLECTOR_CODE = "pnlp-integrations-status";
const LIVE_POLL_MS = 30_000;
const RUN_POLL_MS = 5_000;
const STALE_HOURS = 26;
const SAMPLE_MODE = new URLSearchParams(location.search).has("sample");

const LEVEL_LABELS = {
  ok: "OK",
  info: "Info",
  unknown: "Inconnu",
  warning: "Attention",
  error: "Erreur",
  running: "En cours",
};
const LEVEL_ORDER = ["ok", "info", "unknown", "warning", "error"];
const ROLE_LABELS = { extract: "Extraction", transform: "Transformation", push: "Push vers NMDR" };
const ROLE_ORDER = ["extract", "transform", "push"];
const RUN_LABELS = {
  success: "réussie",
  failed: "échouée",
  stopped: "arrêtée",
  running: "en cours",
  queued: "en file d'attente",
  terminating: "en arrêt",
  skipped: "ignorée",
};
const ACTIVE = new Set(["running", "queued", "terminating"]);

const state = {
  status: null, // contenu de status_latest.json
  live: {}, // code pipeline -> dernière exécution (en direct)
  expanded: new Set(),
  livePoll: null,
  refreshing: false,
};

const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------- API

async function gql(query, variables = {}) {
  const response = await fetch("/graphql/", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  if (!response.ok) throw new Error(`${response.status} : ${await response.text()}`);
  const body = await response.json();
  if (body.errors) throw new Error(body.errors.map((e) => e.message).join(" ; "));
  return body.data;
}

function workspaceSlug() {
  return window.OPENHEXA?.workspaceSlug;
}

async function loadStatus() {
  if (SAMPLE_MODE) {
    const response = await fetch("status.sample.json", { cache: "no-store" });
    return response.json();
  }
  const data = await gql(
    `query ($ws: String!, $path: String!) {
      readFileContent(workspaceSlug: $ws, filePath: $path) { success errors content endLine totalLines }
    }`,
    { ws: workspaceSlug(), path: STATUS_FILE },
  );
  const file = data.readFileContent;
  if (!file.success) throw new Error(`Fichier de statut introuvable (${file.errors.join(", ")}).`);
  if (file.endLine < file.totalLines) throw new Error("Fichier de statut tronqué à la lecture.");
  return JSON.parse(file.content);
}

async function loadLiveRuns(codes) {
  const parts = codes.map(
    (code, i) =>
      `p${i}: pipelineByCode(workspaceSlug: $ws, code: "${code}") { id code runs(page: 1, perPage: 1) { items { id status executionDate duration } } }`,
  );
  const data = await gql(`query ($ws: String!) { ${parts.join(" ")} }`, { ws: workspaceSlug() });
  const live = {};
  codes.forEach((code, i) => {
    const pipeline = data[`p${i}`];
    live[code] = pipeline ? { id: pipeline.id, run: pipeline.runs.items[0] || null } : null;
  });
  return live;
}

// ---------------------------------------------------------------- Formatting

const dateTimeFormat = new Intl.DateTimeFormat("fr-FR", { dateStyle: "short", timeStyle: "short" });
const dateFormat = new Intl.DateTimeFormat("fr-FR", { dateStyle: "short" });
const numberFormat = new Intl.NumberFormat("fr-FR");

function esc(value) {
  return String(value ?? "").replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );
}

function fmtDateTime(iso) {
  return iso ? dateTimeFormat.format(new Date(iso)) : "—";
}

function fmtDate(value) {
  if (!value) return "—";
  return /^\d{4}-\d{2}-\d{2}$/.test(value) ? dateFormat.format(new Date(`${value}T00:00:00`)) : value;
}

function fmtAgo(iso) {
  if (!iso) return "";
  const minutes = Math.round((Date.now() - new Date(iso).getTime()) / 60_000);
  if (minutes < 1) return "à l'instant";
  if (minutes < 60) return `il y a ${minutes} min`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `il y a ${hours} h`;
  return `il y a ${Math.round(hours / 24)} j`;
}

function fmtDuration(seconds) {
  if (seconds == null) return "";
  if (seconds < 60) return `${seconds} s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes} min ${seconds % 60} s`;
  return `${Math.floor(minutes / 60)} h ${minutes % 60} min`;
}

const nextRunFormat = new Intl.DateTimeFormat("fr-FR", {
  weekday: "short",
  day: "2-digit",
  month: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

function fmtIn(date) {
  const minutes = Math.round((date.getTime() - Date.now()) / 60_000);
  if (minutes < 60) return `dans ${Math.max(minutes, 1)} min`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `dans ${hours} h`;
  return `dans ${Math.round(hours / 24)} j`;
}

// ---------------------------------------------------------------- Cron (OpenHEXA schedules are in UTC)

function parseCronField(field, min, max) {
  const values = new Set();
  for (const part of field.split(",")) {
    const [range, stepText] = part.split("/");
    const step = stepText ? Number(stepText) : 1;
    let start;
    let end;
    if (range === "*") [start, end] = [min, max];
    else if (range.includes("-")) [start, end] = range.split("-").map(Number);
    else [start, end] = [Number(range), stepText ? max : Number(range)];
    if ([start, end, step].some((n) => !Number.isInteger(n)) || step < 1 || start < min || end > max) return null;
    for (let v = start; v <= end; v += step) values.add(v);
  }
  return values;
}

function nextCronRun(expression, from = new Date()) {
  const fields = (expression || "").trim().split(/\s+/);
  if (fields.length !== 5) return null;
  const [minutes, hours, days, months, weekdays] = [
    parseCronField(fields[0], 0, 59),
    parseCronField(fields[1], 0, 23),
    parseCronField(fields[2], 1, 31),
    parseCronField(fields[3], 1, 12),
    parseCronField(fields[4], 0, 7),
  ];
  if (![minutes, hours, days, months, weekdays].every(Boolean)) return null;
  if (weekdays.has(7)) weekdays.add(0); // 7 = dimanche
  const sortedHours = [...hours].sort((a, b) => a - b);
  const sortedMinutes = [...minutes].sort((a, b) => a - b);
  // Standard cron: when both day-of-month and day-of-week are restricted, either one matches.
  const eitherDay = fields[2] !== "*" && fields[4] !== "*";
  const start = new Date(from.getTime() + 60_000);
  start.setUTCSeconds(0, 0);

  for (let offset = 0; offset < 400; offset += 1) {
    const day = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate() + offset));
    if (!months.has(day.getUTCMonth() + 1)) continue;
    const domMatch = days.has(day.getUTCDate());
    const dowMatch = weekdays.has(day.getUTCDay());
    if (!(eitherDay ? domMatch || dowMatch : domMatch && dowMatch)) continue;
    for (const hour of sortedHours) {
      for (const minute of sortedMinutes) {
        const candidate = new Date(Date.UTC(day.getUTCFullYear(), day.getUTCMonth(), day.getUTCDate(), hour, minute));
        if (candidate >= start) return candidate;
      }
    }
  }
  return null;
}

function fmtNextRun(schedule) {
  if (!schedule) return "aucune (lancement manuel)";
  const next = nextCronRun(schedule);
  return next ? `${nextRunFormat.format(next)} (${fmtIn(next)})` : "—";
}

function fmtCounts(totals) {
  if (!totals) return "";
  return `${numberFormat.format(totals.imported)} importées · ${numberFormat.format(totals.updated)} mises à jour · ${numberFormat.format(totals.ignored)} ignorées`;
}

function chip(level, label) {
  return `<span class="chip ${esc(level)}">${esc(label ?? LEVEL_LABELS[level] ?? level)}</span>`;
}

function link(url, label) {
  return url ? `<a href="${esc(url)}" target="_blank" rel="noopener">${esc(label)}</a>` : esc(label);
}

function pipelineLabel(step) {
  return `${link(step.url, step.name)} <span class="mono small">(${esc(step.code)})</span>`;
}

// ---------------------------------------------------------------- Live overlay

function liveRun(code) {
  return state.live[code]?.run || null;
}

function liveChip(step) {
  const run = liveRun(step.code);
  if (!run) return "";
  if (ACTIVE.has(run.status)) {
    const since = Math.max(0, Math.round((Date.now() - new Date(run.executionDate).getTime()) / 60_000));
    const label = run.status === "queued" ? "En file d'attente" : `En cours depuis ${since} min`;
    return chip("running", label);
  }
  if (step.last_run && run.id !== step.last_run.id) {
    return chip("info", `Nouvelle exécution ${RUN_LABELS[run.status] || run.status}`);
  }
  return "";
}

function flowIsRunning(flow) {
  return flow.steps.some((step) => ACTIVE.has(liveRun(step.code)?.status));
}

// ---------------------------------------------------------------- Rendering

function renderHeader() {
  const status = state.status;
  const target = status.target || {};
  $("target").innerHTML = `Instance cible : ${esc(target.name)} ${target.url ? `(${link(target.url, target.url)})` : ""}`;

  const generated = status.generated_at;
  const checked = status.checked_at;
  const stale = Date.now() - new Date(checked || generated).getTime() > STALE_HOURS * 3_600_000;
  const parts = [`Collecte du ${fmtDateTime(generated)} (${fmtAgo(generated)})`];
  if (checked && checked !== generated) parts.push(`vérifié le ${fmtDateTime(checked)} sans changement`);
  if (status.collector_run_url) parts.push(link(status.collector_run_url, "exécution du collecteur"));
  if (SAMPLE_MODE) parts.push("<strong>données d'exemple (status.sample.json)</strong>");
  if (stale) parts.push(`<strong>données de plus de ${STALE_HOURS} h</strong>`);
  $("meta").innerHTML = parts.join(" · ");
  $("meta").classList.toggle("stale", stale);
}

function renderCounters() {
  const counts = { error: 0, warning: 0, ok: 0, running: 0 };
  for (const flow of state.status.flows) {
    if (flow.status === "error") counts.error += 1;
    else if (flow.status === "warning") counts.warning += 1;
    else counts.ok += 1;
    if (flowIsRunning(flow)) counts.running += 1;
  }
  const items = [
    ["error", "Erreur"],
    ["warning", "Attention"],
    ["ok", "OK"],
    ["running", "En cours"],
  ];
  $("counters").innerHTML = items
    .map(
      ([level, label]) =>
        `<div class="counter ${level}"><strong>${counts[level]}</strong><span>${label}${level === "running" ? "" : " (flux)"}</span></div>`,
    )
    .join("");
}

function freshnessCell(items, key) {
  if (!items.length) return "—";
  return items
    .map((item) => {
      const value = key === "pushed" && item.pushed_week ? `${fmtDate(item.pushed)} (${item.pushed_week})` : fmtDate(item[key]);
      return items.length > 1 ? `<div><span class="small">${esc(item.label)} :</span> ${esc(value)}</div>` : `<div>${esc(value)}</div>`;
    })
    .join("");
}

function sortedFlags(flags) {
  return [...flags].sort((a, b) => LEVEL_ORDER.indexOf(b.level) - LEVEL_ORDER.indexOf(a.level));
}

function allFlags(flow) {
  const stepFlags = flow.steps.flatMap((step) => step.flags.map((f) => ({ ...f, message: `${step.name} : ${f.message}` })));
  return sortedFlags([...flow.flags, ...stepFlags]);
}

function renderFlowRow(flow) {
  const expanded = state.expanded.has(flow.id);
  const flags = allFlags(flow).filter((f) => f.level !== "ok");
  const lastPush = flow.last_push;
  const running = flowIsRunning(flow) ? chip("running", "En cours") : "";
  return `
    <tr class="flow-row" data-flow="${esc(flow.id)}" tabindex="0" aria-expanded="${expanded}">
      <td><div class="stack">${chip(flow.status)}${running ? `<div>${running}</div>` : ""}</div></td>
      <td><span class="toggle">▸</span> <span class="flow-name">${esc(flow.label)}</span><div class="small">Source : ${esc(flow.source)}</div></td>
      <td>${freshnessCell(flow.freshness, "source")}</td>
      <td>${freshnessCell(flow.freshness, "pushed")}</td>
      <td>${
        lastPush
          ? `<div>${link(lastPush.run.url, fmtDateTime(lastPush.run.date))}</div><div class="small">${fmtCounts(lastPush.totals)}</div>`
          : '<span class="small">Aucun push récent</span>'
      }</td>
      <td class="alert-line">${
        flags.length
          ? `<div class="stack">${chip(flags[0].level, `${flags.length} alerte${flags.length > 1 ? "s" : ""}`)}<div class="small">${esc(flags[0].message)}</div></div>`
          : '<span class="small">—</span>'
      }</td>
    </tr>
    ${expanded ? `<tr class="detail-row"><td colspan="6">${renderDetail(flow)}</td></tr>` : ""}`;
}

function renderStep(step) {
  const run = step.last_run;
  const evidence = step.evidence || {};
  const notes = [];
  if (evidence.new_version) notes.push("Nouvelle version source détectée");
  if (evidence.no_new_data) notes.push("Pas de nouvelle donnée à pousser");
  if (evidence.last_update_set) notes.push(`Horodatage mis à jour : ${evidence.last_update_set}`);
  if (evidence.empty_periods?.length) notes.push(`Périodes vides : ${evidence.empty_periods.join(", ")}`);
  for (const table of evidence.tables || []) notes.push(`Table ${table.table} : ${table.year}W${String(table.week).padStart(2, "0")}`);

  return `
    <article class="step ${esc(step.status)}">
      <div class="step-head">
        <div class="step-name">${pipelineLabel(step)}</div>
        <div class="stack">${chip(step.status)}${liveChip(step) ? `<div>${liveChip(step)}</div>` : ""}</div>
      </div>
      <dl>
        <dt>Dernière exécution</dt>
        <dd>${run ? `${link(run.url, fmtDateTime(run.date))} · ${esc(RUN_LABELS[run.status] || run.status)} · ${fmtDuration(run.duration)}` : "—"}</dd>
        <dt>Dernière réussite</dt>
        <dd>${fmtDateTime(step.last_success)}</dd>
        <dt>Prochaine exécution</dt>
        <dd>${esc(fmtNextRun(step.schedule))}</dd>
        <dt>Planification</dt>
        <dd>${step.schedule ? `<code>${esc(step.schedule)}</code> (UTC)` : "manuelle"} · attendue tous les ${esc(step.interval_days)} j</dd>
        ${evidence.error_count || evidence.warning_count ? `<dt>Messages</dt><dd>${evidence.error_count} erreur(s), ${evidence.warning_count} avertissement(s)</dd>` : ""}
      </dl>
      ${notes.length ? `<ul class="plain-list small">${notes.map((n) => `<li>${esc(n)}</li>`).join("")}</ul>` : ""}
      ${evidence.errors?.length ? `<ul class="plain-list small">${evidence.errors.map((e) => `<li class="mono">${esc(e)}</li>`).join("")}</ul>` : ""}
      ${step.flags.length ? `<ul class="flags">${sortedFlags(step.flags).map((f) => `<li>${chip(f.level)} <span>${esc(f.message)}</span></li>`).join("")}</ul>` : ""}
    </article>`;
}

function renderChain(flow) {
  const groups = ROLE_ORDER.map((role) => [role, flow.steps.filter((s) => s.role === role)]).filter(([, steps]) => steps.length);
  return groups
    .map(
      ([role, steps], i) =>
        `${i ? '<div class="chain-arrow" aria-hidden="true">→</div>' : ""}
        <div class="chain-group"><h4>${esc(ROLE_LABELS[role] || role)}</h4>${steps.map(renderStep).join("")}</div>`,
    )
    .join("");
}

function renderPush(flow) {
  const push = flow.last_push;
  if (!push) return '<p class="small">Aucun résumé de push trouvé dans les dernières exécutions.</p>';
  const rows = push.summaries
    .map(
      (s) => `<tr><td>${esc(s.section || "—")}</td><td>${numberFormat.format(s.imported ?? 0)}</td><td>${numberFormat.format(s.updated ?? 0)}</td>
        <td>${numberFormat.format(s.ignored ?? 0)}</td><td>${numberFormat.format(s.deleted ?? 0)}</td></tr>`,
    )
    .join("");
  return `
    <p>${link(push.run.url, fmtDateTime(push.run.date))} — ${fmtCounts(push.totals)}</p>
    ${push.periods.length ? `<div class="pills">${push.periods.map((p) => `<span class="pill mono">${esc(p)}</span>`).join("")}</div>` : ""}
    <table class="inner"><thead><tr><th>Section</th><th>Importées</th><th>Mises à jour</th><th>Ignorées</th><th>Supprimées</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
}

function renderDetail(flow) {
  const flags = allFlags(flow);
  const freshnessRows = flow.freshness
    .map(
      (f) =>
        `<tr><td>${esc(f.label)}</td><td>${esc(fmtDate(f.source))}</td><td>${esc(fmtDate(f.pushed))}${f.pushed_week ? ` (${esc(f.pushed_week)})` : ""}</td></tr>`,
    )
    .join("");
  const datasets = flow.datasets
    .map(
      (d) => `<li>${link(d.url, d.name)} <span class="small">(${esc(d.workspace || "")})</span><br />
        <span class="small">Dernière version : <span class="mono">${esc(d.latest_version || "—")}</span> du ${fmtDateTime(d.latest_version_date)}${d.produced_by ? ` · produite par ${esc(d.produced_by)}` : ""}</span></li>`,
    )
    .join("");
  const connections = flow.connections
    .map((c) => `<li>${esc(c.name)} ${c.url ? `(${link(c.url, c.url)})` : `<span class="small">(${esc(c.slug)})</span>`}</li>`)
    .join("");

  return `
    <div class="detail">
      ${flags.length ? `<section class="panel"><h3>Alertes</h3><ul class="flags">${flags.map((f) => `<li>${chip(f.level)} <span>${esc(f.message)}</span></li>`).join("")}</ul></section>` : ""}
      <section class="panel"><h3>Chaîne de traitement</h3><div class="chain">${renderChain(flow)}</div></section>
      <div class="panels-2">
        <section class="panel"><h3>Fraîcheur des données</h3>
          ${freshnessRows ? `<table class="inner"><thead><tr><th>Donnée</th><th>Disponible à la source</th><th>Poussée vers NMDR</th></tr></thead><tbody>${freshnessRows}</tbody></table>` : '<p class="small">—</p>'}
        </section>
        <section class="panel"><h3>Dernier push</h3>${renderPush(flow)}</section>
      </div>
      <div class="panels-2">
        <section class="panel"><h3>Sources (datasets)</h3>${datasets ? `<ul class="plain-list">${datasets}</ul>` : '<p class="small">Source externe, pas de dataset OpenHEXA.</p>'}</section>
        <section class="panel"><h3>Connexions</h3><ul class="plain-list">${connections}</ul></section>
      </div>
    </div>`;
}

function render() {
  if (!state.status) return;
  renderHeader();
  renderCounters();
  $("flows").innerHTML = state.status.flows.map(renderFlowRow).join("");
  updateRefreshButton();
}

function showMessage(text) {
  $("message").hidden = !text;
  $("message").textContent = text || "";
}

// ---------------------------------------------------------------- Live polling

function trackedCodes() {
  const codes = new Set([COLLECTOR_CODE]);
  for (const flow of state.status?.flows || []) for (const step of flow.steps) codes.add(step.code);
  return [...codes];
}

async function refreshLive() {
  if (SAMPLE_MODE || !workspaceSlug()) return;
  try {
    state.live = await loadLiveRuns(trackedCodes());
  } catch (error) {
    console.warn("État en direct indisponible", error);
  }
  render();
  const anyActive = Object.values(state.live).some((p) => ACTIVE.has(p?.run?.status));
  clearTimeout(state.livePoll);
  if (anyActive) state.livePoll = setTimeout(refreshLive, LIVE_POLL_MS);
}

// ---------------------------------------------------------------- Refresh button

function setRefreshStatus(text) {
  $("refresh-status").innerHTML = text;
}

function updateRefreshButton() {
  const button = $("refresh");
  const collectorRun = liveRun(COLLECTOR_CODE);
  const collectorActive = ACTIVE.has(collectorRun?.status);
  button.disabled = SAMPLE_MODE || !workspaceSlug() || state.refreshing || collectorActive;
  if (SAMPLE_MODE) button.title = "Indisponible en mode exemple";
  else if (collectorActive && !state.refreshing) {
    // A collection started elsewhere (schedule or another viewer): follow it and reload when done.
    setRefreshStatus("Une collecte est déjà en cours…");
    waitForCollector(collectorRun.id);
  }
}

async function startRefresh() {
  state.refreshing = true;
  updateRefreshButton();
  setRefreshStatus("Lancement de la collecte…");
  try {
    const collector = state.live[COLLECTOR_CODE] || (await loadLiveRuns([COLLECTOR_CODE]))[COLLECTOR_CODE];
    if (!collector) throw new Error(`Pipeline de collecte (${COLLECTOR_CODE}) introuvable dans le workspace.`);
    const data = await gql(
      `mutation ($input: RunPipelineInput!) { runPipeline(input: $input) { success errors run { id } } }`,
      { input: { id: collector.id, config: { force: false } } },
    );
    if (!data.runPipeline.success) {
      const errors = data.runPipeline.errors.join(", ");
      throw new Error(errors.includes("PERMISSION") ? "Vous n'avez pas le droit de lancer ce pipeline." : errors);
    }
    await waitForCollector(data.runPipeline.run.id);
  } catch (error) {
    setRefreshStatus(`<span class="chip error">Erreur</span> ${esc(error.message)}`);
    state.refreshing = false;
    updateRefreshButton();
  }
}

async function waitForCollector(runId) {
  state.refreshing = true;
  const previousGenerated = state.status?.generated_at;
  for (;;) {
    const data = await gql(`query ($id: UUID!) { pipelineRun(id: $id) { status progress } }`, { id: runId });
    const run = data.pipelineRun;
    if (!ACTIVE.has(run.status)) {
      if (run.status === "success") {
        state.status = await loadStatus();
        const changed = state.status.generated_at !== previousGenerated;
        setRefreshStatus(
          changed
            ? `Données collectées ${fmtDateTime(state.status.generated_at)}.`
            : `Aucun changement depuis le ${fmtDateTime(state.status.generated_at)}.`,
        );
      } else {
        setRefreshStatus(`<span class="chip error">Erreur</span> La collecte s'est terminée avec le statut « ${esc(RUN_LABELS[run.status] || run.status)} ».`);
      }
      break;
    }
    setRefreshStatus(`Collecte ${RUN_LABELS[run.status] || run.status}${run.progress ? ` (${run.progress} %)` : ""}…`);
    await new Promise((resolve) => setTimeout(resolve, RUN_POLL_MS));
  }
  state.refreshing = false;
  await refreshLive();
}

// ---------------------------------------------------------------- Events & start

function toggleFlow(id) {
  if (state.expanded.has(id)) state.expanded.delete(id);
  else state.expanded.add(id);
  render();
}

$("flows").addEventListener("click", (event) => {
  if (event.target.closest("a")) return;
  const row = event.target.closest(".flow-row");
  if (row) toggleFlow(row.dataset.flow);
});

$("flows").addEventListener("keydown", (event) => {
  const row = event.target.closest(".flow-row");
  if (row && (event.key === "Enter" || event.key === " ")) {
    event.preventDefault();
    toggleFlow(row.dataset.flow);
  }
});

$("refresh").addEventListener("click", startRefresh);

async function main() {
  if (!SAMPLE_MODE && !workspaceSlug()) {
    $("meta").textContent = "Non connecté à OpenHEXA.";
    showMessage("Cliquez sur « Connect to OpenHEXA », ou ajoutez ?sample=1 à l'adresse pour voir les données d'exemple.");
    return;
  }
  try {
    state.status = await loadStatus();
    render();
    await refreshLive();
  } catch (error) {
    $("meta").textContent = "Impossible de charger le statut.";
    showMessage(`${error.message} Lancez « Actualiser » pour produire un premier fichier.`);
    updateRefreshButton();
  }
}

main();
