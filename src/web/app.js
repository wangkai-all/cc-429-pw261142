/* 口径不一致事件加工系统 —— 单页前端，无第三方依赖 */
const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

const SERIES_COLOR = {
  "alpha-gateway": "var(--series-1)",
  "beta-pipeline": "var(--series-2)",
  "gamma-bridge": "var(--series-3)",
};
const SOURCE_ORDER = ["alpha-gateway", "beta-pipeline", "gamma-bridge"];

let state = { overview: null, currentReport: null, currentJobDetail: null };

async function api(path, opts) {
  const res = await fetch(path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  return data;
}
const get = (p) => api(p);
const post = (p, body) => api(p, {
  method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
});
const put = (p, body) => api(p, {
  method: "PUT", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body),
});

function toast(msg, isError = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast" + (isError ? " error" : "");
  clearTimeout(toast._h);
  toast._h = setTimeout(() => t.classList.add("hidden"), 4200);
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
}
function unitLabel(u) { return (u || []).slice(1).join(" / ") || "—"; }
function pct(r) { return r == null ? "不可计算" : (r * 100).toFixed(2) + "%"; }

// --- Tab 切换 ----------------------------------------------------------------
$$("#tabs .tab").forEach((tab) => tab.addEventListener("click", () => {
  $$("#tabs .tab").forEach((t) => t.classList.remove("active"));
  $$(".panel").forEach((p) => p.classList.remove("active"));
  tab.classList.add("active");
  $(`#panel-${tab.dataset.tab}`).classList.add("active");
  if (tab.dataset.tab === "incompat") loadIncompatView();
  if (tab.dataset.tab === "report") loadReportView();
}));

// --- 总览 --------------------------------------------------------------------
async function refreshOverview() {
  state.overview = await get("/api/overview");
  const o = state.overview;
  $("#active-meta").innerHTML =
    `激活映射 <b>${esc(o.active_mapping)}</b><br>输入版 ${o.input_versions.length} 个 · 作业 ${o.jobs.length} 个`;
  renderJobs();
  renderMappingVersions();
  renderJobSelects();
}

function renderJobs() {
  const tb = $("#jobs-table tbody");
  tb.innerHTML = state.overview.jobs.map((j) => `
    <tr>
      <td><code>${esc(j.job_id)}</code></td>
      <td>${esc(j.input_version)}</td>
      <td>${esc(j.mapping_version)}</td>
      <td><span class="status ${esc(j.status)}">${esc(j.status)}</span></td>
      <td>${esc(j.last_stage || "—")}</td>
      <td><button class="btn" data-job="${esc(j.job_id)}">检查点</button></td>
    </tr>`).join("") || `<tr><td colspan="6" class="hint">暂无作业</td></tr>`;
  tb.querySelectorAll("button[data-job]").forEach((b) =>
    b.addEventListener("click", () => loadJobCheckpoints(b.dataset.job)));
}

async function loadJobCheckpoints(jobId) {
  const d = await get(`/api/job?id=${encodeURIComponent(jobId)}`);
  state.currentJobDetail = d;
  const cp = d.checkpoints.map((c) =>
    `<span class="cp ${esc(c.status)}">${esc(c.stage)}:${esc(c.status)}</span>`).join("");
  $("#job-checkpoints").innerHTML =
    `<h3>${esc(jobId)} 的阶段检查点（恢复时跳过 done）</h3><div class="checkpoint-line">${cp}</div>`;
}

function renderJobSelects() {
  const jobs = state.overview.jobs;
  const opts = jobs.map((j) =>
    `<option value="${esc(j.job_id)}">${esc(j.job_id)} (${esc(j.input_version)}, ${esc(j.status)})</option>`).join("");
  $("#incompat-job-select").innerHTML = opts;
  $("#report-job-select").innerHTML = opts;
  $("#compare-a").innerHTML = opts;
  $("#compare-b").innerHTML = opts;
  const completed = jobs.filter((j) => j.status === "completed");
  if (completed.length) {
    $("#report-job-select").value = completed[completed.length - 1].job_id;
    $("#compare-a").value = completed[0].job_id;
    if (completed[1]) $("#compare-b").value = completed[1].job_id;
  }
}

// --- 摄入 --------------------------------------------------------------------
async function runIngest(body) {
  try {
    const r = await post("/api/ingest", body);
    await refreshOverview();
    const st = r.job.status;
    if (st === "interrupted") {
      toast(`作业已中断于 ${r.job.last_stage}，可点"恢复所有中断作业"沿原输入版继续`);
      await loadJobCheckpoints(r.job.job_id);
    } else {
      toast(`加工完成：${r.job.job_id}`);
    }
    $("#ingest-result").innerHTML = `<h2>最近作业结果</h2><pre class="raw">${esc(JSON.stringify(r, null, 2))}</pre>`;
  } catch (e) {
    toast(e.message, true);
  }
}

$("#btn-run-initial").onclick = () =>
  runIngest({ sample: "initial", note: "首版：三来源虚构事件", crash: $("#arm-crash").checked });
$("#btn-run-supplement").onclick = () =>
  runIngest({ sample: "gamma_supplement", note: "补交 gamma U3 成功事件",
              include_previous: latestInputVersion(), crash: $("#arm-crash").checked });
$("#btn-run-full").onclick = () =>
  runIngest({ sample: "gamma_full", note: "gamma 补字段后全量口径",
              include_previous: latestInputVersion(), crash: $("#arm-crash").checked });
$("#btn-ingest-custom").onclick = () => {
  let payload;
  try { payload = JSON.parse($("#ingest-json").value); }
  catch (e) { return toast("JSON 解析失败：" + e.message, true); }
  runIngest({ payload, note: $("#ingest-note").value || "网页自定义摄入",
              crash: $("#arm-crash").checked });
};
$("#btn-resume-all").onclick = async () => {
  try {
    const r = await post("/api/resume", {});
    await refreshOverview();
    const n = (r.jobs || []).length;
    toast(n ? `已恢复 ${n} 个作业` : "没有需要恢复的中断作业");
  } catch (e) { toast(e.message, true); }
};
function latestInputVersion() {
  const iv = state.overview.input_versions;
  return iv.length ? iv[iv.length - 1].input_version : null;
}

// --- 口径映射 ----------------------------------------------------------------
async function loadMappingEditor() {
  const m = await get("/api/mapping");
  $("#mapping-version-tag").textContent = m.mapping_version;
  $("#mapping-editor").value = JSON.stringify(m.snapshot, null, 2);
  $("#mapping-summary").innerHTML = Object.entries(m.summary).map(([src, s]) => `
    <div class="sig-chip ${s.compatible ? "" : "bad"}">
      <span><span class="dot" style="background:${SERIES_COLOR[src] || "#888"}"></span><b>${esc(src)}</b></span>
      <span>观测：${esc(s.observed_type)} · 重试：${esc(s.retry_policy)}</span>
      <code>签名 ${esc(s.caliber_sig)} · ${s.compatible ? "相容" : "不相容"}</code>
    </div>`).join("");
  renderMappingVersions();
}
function renderMappingVersions() {
  const tb = $("#mapping-versions tbody");
  if (!tb) return;
  tb.innerHTML = state.overview.mapping_versions.map((m) => `
    <tr>
      <td><code>${esc(m.mapping_version)}</code>${m.is_active ? ' <span class="tag">激活</span>' : ""}</td>
      <td>${esc(m.created_at)}</td>
      <td>${esc(m.note)}</td>
      <td><button class="btn" data-mv="${esc(m.mapping_version)}">查看</button></td>
    </tr>`).join("");
  tb.querySelectorAll("button[data-mv]").forEach((b) => b.addEventListener("click", async () => {
    const m = await get(`/api/mapping/${encodeURIComponent(b.dataset.mv)}`);
    $("#mapping-editor").value = JSON.stringify(m.snapshot, null, 2);
    $("#mapping-version-tag").textContent = m.mapping_version + "（历史只读视图，保存仍生成新版本）";
  }));
}
$("#btn-reload-mapping").onclick = loadMappingEditor;
$("#btn-save-mapping").onclick = async () => {
  let snapshot;
  try { snapshot = JSON.parse($("#mapping-editor").value); }
  catch (e) { return toast("映射 JSON 解析失败：" + e.message, true); }
  try {
    const r = await put("/api/mapping", { snapshot, note: $("#mapping-note").value });
    $("#mapping-message").innerHTML =
      `<p class="reason-pill good">已保存并激活 ${esc(r.mapping_version)}</p>`;
    await refreshOverview();
    await loadMappingEditor();
    toast("新映射版本已激活：" + r.mapping_version);
  } catch (e) {
    $("#mapping-message").innerHTML =
      `<p class="reason-pill">校验拒绝（未生成新版本）：${esc(e.message)}</p>`;
    toast(e.message, true);
  }
};

// --- 不相容视图 --------------------------------------------------------------
async function loadIncompatView() {
  const sel = $("#incompat-job-select");
  if (!sel.value) return;
  const d = await get(`/api/incompat?id=${encodeURIComponent(sel.value)}`);
  $("#verdicts").innerHTML = d.verdicts.length ? d.verdicts.map((v) => `
    <details open>
      <summary><span class="dot" style="display:inline-block;width:9px;height:9px;border-radius:50%;
        background:${SERIES_COLOR[v.source] || "#888"}"></span> ${esc(v.source)}
        <span class="reason-pill">${esc(v.reason_code)}</span></summary>
      <p>${esc(v.reason_text || "")}</p>
      <p class="hint">观测口径 ${esc(v.observed_type)} · 重试口径 ${esc(v.retry_policy)} · 签名 <code>${esc(v.caliber_sig)}</code></p>
    </details>`).join("") : `<p class="hint">全部来源相容</p>`;

  $("#retained").innerHTML = d.retained_records.length ? d.retained_records.map((r) => `
    <details>
      <summary>${esc(r.source)} #${r.record_index ?? "—"}
        <span class="reason-pill">${esc(r.reason_code)}</span></summary>
      <p>${esc(r.reason_text)}</p>
      <pre class="raw">${esc(JSON.stringify(r.raw, null, 2))}</pre>
    </details>`).join("") : `<p class="hint">无被拒绝记录</p>`;

  $("#exclusions").innerHTML = d.exclusions.length ? `
    <div class="table-wrap"><table>
      <thead><tr><th>来源</th><th>单元</th><th>原因码</th><th>说明</th><th class="num">记录数</th></tr></thead>
      <tbody>${d.exclusions.map((e) => `
        <tr><td>${esc(e.source)}</td><td>${e.unit ? esc(unitLabel(e.unit)) : "（全部单元）"}</td>
        <td><span class="reason-pill">${esc(e.reason_code)}</span></td>
        <td>${esc(e.reason_text)}</td><td class="num">${e.record_count}</td></tr>`).join("")}
      </tbody></table></div>` : `<p class="hint">无排除单元</p>`;
}
$("#incompat-job-select").addEventListener("change", loadIncompatView);

// --- 报告与图表 --------------------------------------------------------------
async function loadReportView() {
  const sel = $("#report-job-select");
  if (!sel.value) return;
  const rep = await get(`/api/report?id=${encodeURIComponent(sel.value)}`);
  state.currentReport = rep;
  const b = rep.body;
  $("#report-meta").innerHTML = `
    <div class="kpi-row">
      <div class="kpi"><div class="v">${b.scope.narrow_common_units.length}</div><div class="k">共同范围单元</div></div>
      <div class="kpi"><div class="v">${Object.keys(b.scope.strata).length}</div><div class="k">口径分层数</div></div>
      <div class="kpi"><div class="v">${b.verdicts.filter((v) => !v.compatible).length}</div><div class="k">不相容来源</div></div>
      <div class="kpi"><div class="v">${b.retained_records.length + b.exclusions.length}</div><div class="k">留痕记录/单元</div></div>
    </div>
    <p class="hint">输入版 ${esc(rep.input_version)} · 映射版 ${esc(rep.mapping_version)} · 指标 ${esc(b.metric)}</p>
    <p class="hint">定义：${esc(b.metric_definition)}</p>`;
  $("#report-limitations").innerHTML =
    `<ul class="limitations">${b.limitations.map((l) => `<li>${esc(l)}</li>`).join("")}</ul>`;
  renderReportBlock();
}
$("#report-job-select").addEventListener("change", loadReportView);
$("#report-strategy").addEventListener("change", renderReportBlock);

function collectRows(block) {
  const rows = [];
  block.units.forEach((u) => u.rows.forEach((r) => rows.push(r)));
  block.unranked_volume_only.forEach((r) => {
    if (!rows.some((x) => x.source === r.source && JSON.stringify(x.unit) === JSON.stringify(r.unit))) {
      rows.push(r);
    }
  });
  return rows;
}

function renderReportBlock() {
  if (!state.currentReport) return;
  const strategy = $("#report-strategy").value;
  const block = state.currentReport.body[strategy];
  const rows = collectRows(block);

  // 按单元分组，rate=null 的只显示成功量、不入条形图（不补零）
  const groups = {};
  rows.forEach((r) => {
    const key = unitLabel(r.unit);
    (groups[key] = groups[key] || []).push(r);
  });
  const ranked = rows.filter((r) => r.rate != null);
  const maxRate = Math.max(0.0001, ...ranked.map((r) => r.rate));

  const legend = SOURCE_ORDER.map((s) =>
    `<span><span class="dot" style="width:10px;height:10px;border-radius:50%;display:inline-block;
      background:${SERIES_COLOR[s]}"></span>${esc(s)}</span>`).join("");
  const chartHtml = Object.entries(groups).map(([unit, rs]) => {
    const bars = rs.sort((a, b) => SOURCE_ORDER.indexOf(a.source) - SOURCE_ORDER.indexOf(b.source))
      .map((r) => {
        if (r.rate == null) {
          return `<div class="bar-row">
            <div class="bar-label"><span class="dot" style="background:${SERIES_COLOR[r.source] || "#888"}"></span>${esc(r.source)}</div>
            <div class="bar-track"><span class="bar-na">分母不可观察（成功量 ${r.success_volume}，不补零）</span></div>
            <div class="bar-value bar-na">N/A</div></div>`;
        }
        const w = (r.rate / maxRate * 100).toFixed(2);
        return `<div class="bar-row" data-tip='${esc(JSON.stringify({
          source: r.source, unit: r.unit, successes: r.successes,
          attempts: r.attempts, rate: pct(r.rate), sig: r.stratum_sig,
        }))}'>
          <div class="bar-label"><span class="dot" style="background:${SERIES_COLOR[r.source] || "#888"}"></span>${esc(r.source)}</div>
          <div class="bar-track"><div class="bar-fill" style="width:${w}%;background:${SERIES_COLOR[r.source] || "#888"}"></div></div>
          <div class="bar-value">${pct(r.rate)}</div></div>`;
      }).join("");
    return `<div class="bar-group"><h3>${esc(unit)}</h3>${bars}</div>`;
  }).join("");
  $("#chart").innerHTML = `<div class="legend">${legend}</div>${chartHtml}
    <p class="axis-note">条形长度按组内最大成功率归一；跨口径层/不可计算来源不参与排序。悬浮查看明细。</p>`;
  $("#chart-note").textContent = ranked.length
    ? `共 ${ranked.length} 条可比较成功率；另有 ${rows.length - ranked.length} 条仅成功量记录。`
    : "当前范围没有可计算成功率的记录。";
  bindTooltips();

  // 表格兜底（含全部行，含 N/A）
  $("#report-table tbody").innerHTML = rows.map((r) => `
    <tr>
      <td>${esc(unitLabel(r.unit))}</td>
      <td><code>${esc(r.stratum_sig || "—")}</code></td>
      <td>${esc(r.source)}</td>
      <td class="num">${r.successes}</td>
      <td class="num">${r.attempts}</td>
      <td class="num">${r.success_volume}</td>
      <td class="num">${r.rate == null ? '<span class="bar-na">不可计算</span>' : pct(r.rate)}</td>
      <td>${r.note ? `<span class="reason-pill">${esc(r.note)}</span>` : ""}</td>
    </tr>`).join("");
}

// --- 悬浮提示 ----------------------------------------------------------------
let tipBox;
function bindTooltips() {
  if (!tipBox) {
    tipBox = document.createElement("div");
    tipBox.className = "tooltip-box";
    document.body.appendChild(tipBox);
  }
  $$(".bar-row[data-tip]").forEach((row) => {
    row.addEventListener("mousemove", (e) => {
      const d = JSON.parse(row.dataset.tip);
      tipBox.style.display = "block";
      tipBox.innerHTML = `<b>${esc(d.source)}</b><br>${esc(d.unit.join(" / "))}<br>
        成功 ${d.successes} / 尝试 ${d.attempts}<br><b>${esc(d.rate)}</b><br>
        <span class="hint">签名 ${esc(d.sig || "narrow")}</span>`;
      tipBox.style.left = Math.min(e.clientX + 14, window.innerWidth - 240) + "px";
      tipBox.style.top = (e.clientY + 14) + "px";
    });
    row.addEventListener("mouseleave", () => (tipBox.style.display = "none"));
  });
}

// --- 跨版比较 ----------------------------------------------------------------
$("#btn-compare").onclick = async () => {
  try {
    const a = $("#compare-a").value, b = $("#compare-b").value;
    if (!a || !b || a === b) return toast("请选择两个不同的作业", true);
    const d = await get(`/api/compare?a=${encodeURIComponent(a)}&b=${encodeURIComponent(b)}`);
    const cmpB = d.b.body.comparison || {};
    let html = `<p class="hint">A: ${esc(d.a.input_version)} (${esc(a)}) vs B: ${esc(d.b.input_version)} (${esc(b)})</p>`;
    if (cmpB.caliber_warnings && cmpB.caliber_warnings.length) {
      html += `<h3>口径变化警告（不可直接比较）</h3>` +
        cmpB.caliber_warnings.map((w) =>
          `<p><span class="reason-pill">${esc(w.reason_code)}</span> ${esc(w.source)}
           ${esc(unitLabel(w.unit))}: ${esc(w.old_sig)} → ${esc(w.new_sig)}</p>`).join("");
    }
    const changes = cmpB.rate_changes || [];
    if (changes.length) {
      html += `<div class="table-wrap"><table><thead><tr>
        <th>单元</th><th>来源</th><th>类型</th><th class="num">旧率</th><th class="num">新率</th><th class="num">Δ</th><th>可比较</th>
        </tr></thead><tbody>` + changes.map((c) => `
        <tr><td>${esc(unitLabel(c.unit))}</td><td>${esc(c.source)}</td>
        <td><span class="reason-pill ${c.kind === "RATE_CHANGE" ? "good" : ""}">${esc(c.kind)}</span></td>
        <td class="num">${c.old_rate == null ? "—" : pct(c.old_rate)}</td>
        <td class="num">${c.new_rate == null ? "—" : pct(c.new_rate)}</td>
        <td class="num">${c.delta == null ? "—" : (c.delta * 100).toFixed(2) + "pp"}</td>
        <td>${c.comparable === false ? '<span class="bar-na">受限</span>' : "是"}</td></tr>`).join("")
        + `</tbody></table></div><p class="hint">${esc(cmpB.limitation || "")}</p>`;
    } else {
      html += `<p class="hint">${esc(cmpB.reason || "两版口径/范围无重合可比较行")}</p>`;
    }
    $("#compare-result").innerHTML = html;
  } catch (e) { toast(e.message, true); }
};

// --- 启动 --------------------------------------------------------------------
(async function init() {
  try {
    await refreshOverview();
    await loadMappingEditor();
    if (state.overview.jobs.length) {
      const last = state.overview.jobs[state.overview.jobs.length - 1];
      $("#incompat-job-select").value = last.job_id;
      loadIncompatView();
      const done = state.overview.jobs.filter((j) => j.status === "completed");
      if (done.length) { $("#report-job-select").value = done[done.length - 1].job_id; loadReportView(); }
    }
  } catch (e) { toast("初始化失败：" + e.message, true); }
})();
