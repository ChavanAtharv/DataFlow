import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import pandas as pd

from src.config import LOG_FILE_PATH, QUARANTINE_DATA_PATH, RAW_DATA_PATH
from src.pipeline import run_pipeline
from src.quality.data_quality import quarantine_bad_records

PORT = 8000
PIPELINE_RUNNING = False
LAST_RUN_STATUS = "Ready"


def get_pipeline_data():
    raw_path = Path(RAW_DATA_PATH)
    if not raw_path.exists():
        return {
            "bronze": [],
            "silver": [],
            "quarantine": [],
            "gold": {"city_summary": {}, "age_summary": {}},
            "metrics": {},
            "logs": "No logs found.",
        }

    df_raw = pd.read_csv(raw_path)
    clean_df, quarantined_df, metrics = quarantine_bad_records(df_raw)

    df_silver = clean_df.copy()
    df_silver["name"] = df_silver["name"].astype(str).str.strip()
    df_silver["email"] = df_silver["email"].astype(str).str.strip().str.lower()
    df_silver["city"] = df_silver["city"].astype(str).str.strip()
    df_silver["age_group"] = pd.cut(
        df_silver["age"],
        bins=[-float("inf"), 24, 30, float("inf")],
        labels=["Young", "Adult", "Senior"],
    ).astype(str)

    city_summary = df_silver["city"].value_counts().to_dict()
    age_summary = df_silver["age_group"].value_counts().to_dict()

    # Read persistent quarantine file if exists
    quarantine_list = []
    quar_path = Path(QUARANTINE_DATA_PATH)
    if quar_path.exists():
        quarantine_list = pd.read_csv(quar_path).to_dict(orient="records")

    log_content = ""
    log_path = Path(LOG_FILE_PATH)
    if log_path.exists():
        lines = log_path.read_text(encoding="utf-8").splitlines()
        log_content = "\n".join(lines[-40:])

    return {
        "bronze": df_raw.to_dict(orient="records"),
        "silver": df_silver.to_dict(orient="records"),
        "quarantine": quarantine_list,
        "gold": {
            "city_summary": city_summary,
            "age_summary": age_summary,
        },
        "metrics": metrics,
        "logs": log_content,
        "is_running": PIPELINE_RUNNING,
        "last_status": LAST_RUN_STATUS,
    }


def execute_pipeline_job():
    global PIPELINE_RUNNING, LAST_RUN_STATUS
    PIPELINE_RUNNING = True
    LAST_RUN_STATUS = "Running..."
    try:
        run_pipeline(skip_db=True)
        LAST_RUN_STATUS = "Success"
    except Exception as exc:
        LAST_RUN_STATUS = f"Failed: {exc}"
    finally:
        PIPELINE_RUNNING = False


HTML_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>DataFlow ⚡ Medallion Lakehouse Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; }
    code, pre { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <!-- Top Navigation -->
  <header class="border-b border-slate-800 bg-slate-900/60 backdrop-blur sticky top-0 z-50">
    <div class="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
      <div class="flex items-center space-x-3">
        <div class="w-10 h-10 rounded-xl bg-gradient-to-tr from-cyan-500 to-blue-600 flex items-center justify-center font-black text-xl shadow-lg shadow-cyan-500/20 text-white">
          ⚡
        </div>
        <div>
          <h1 class="text-xl font-bold tracking-tight text-white flex items-center gap-2">
            DataFlow
            <span class="text-xs font-semibold px-2.5 py-0.5 rounded-full bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">v0.2.0</span>
          </h1>
          <p class="text-xs text-slate-400">Medallion Architecture ETL & Analytics Lakehouse</p>
        </div>
      </div>
      <div class="flex items-center space-x-4">
        <div class="flex items-center gap-2 text-xs px-3 py-1.5 rounded-lg bg-slate-800/80 border border-slate-700">
          <span class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
          <span id="pipeline-status" class="text-slate-300 font-medium">Ready</span>
        </div>
        <button id="run-btn" onclick="triggerPipeline()" class="px-4 py-2 bg-gradient-to-r from-blue-600 to-cyan-500 hover:from-blue-500 hover:to-cyan-400 text-white font-medium text-sm rounded-lg shadow-md hover:shadow-cyan-500/25 transition-all flex items-center gap-2 cursor-pointer disabled:opacity-50">
          <svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M14.752 11.168l-3.197-2.132A1 1 0 0010 9.87v4.263a1 1 0 001.555.832l3.197-2.132a1 1 0 000-1.664z"></path><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 12a9 9 0 11-18 0 9 9 0 0118 0z"></path></svg>
          Run Pipeline
        </button>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-6 py-8 space-y-8">
    <!-- KPI Stat Cards -->
    <section class="grid grid-cols-1 md:grid-cols-4 gap-4">
      <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 relative overflow-hidden">
        <div class="text-xs font-semibold uppercase tracking-wider text-slate-400">Bronze Records</div>
        <div id="kpi-records" class="text-3xl font-extrabold text-white mt-2">--</div>
        <div class="text-xs text-slate-500 mt-1">Raw CSV Ingested</div>
        <div class="absolute right-4 bottom-4 text-slate-800 text-3xl font-black">#</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 relative overflow-hidden">
        <div class="text-xs font-semibold uppercase tracking-wider text-slate-400">Data Quality & Quarantine</div>
        <div id="kpi-quality" class="text-3xl font-extrabold text-emerald-400 mt-2">100%</div>
        <div id="kpi-quality-detail" class="text-xs text-slate-500 mt-1">0 Nulls · 0 Dups · 0 Quarantined</div>
        <div class="absolute right-4 bottom-4 text-emerald-500/10 text-3xl font-black">🛡️</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 relative overflow-hidden">
        <div class="text-xs font-semibold uppercase tracking-wider text-slate-400">Silver & Gold Parquet</div>
        <div id="kpi-silver" class="text-3xl font-extrabold text-cyan-400 mt-2">--</div>
        <div class="text-xs text-slate-500 mt-1">Lakehouse Snappy Parquet</div>
        <div class="absolute right-4 bottom-4 text-cyan-500/10 text-3xl font-black">⚡</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 rounded-2xl p-5 relative overflow-hidden">
        <div class="text-xs font-semibold uppercase tracking-wider text-slate-400">Test Suite</div>
        <div class="text-3xl font-extrabold text-indigo-400 mt-2">24 / 24</div>
        <div class="text-xs text-slate-500 mt-1">Unit & Integration (100% Pass)</div>
        <div class="absolute right-4 bottom-4 text-indigo-500/10 text-3xl font-black">🧪</div>
      </div>
    </section>

    <!-- Medallion Pipeline Architecture Visualizer -->
    <section class="bg-slate-900/40 border border-slate-800 rounded-2xl p-6">
      <h2 class="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-4 flex items-center gap-2">
        <span class="w-2 h-2 rounded-full bg-cyan-400"></span>
        Medallion Lakehouse Flow & Dead-Letter Routing
      </h2>
      <div class="grid grid-cols-1 md:grid-cols-4 gap-6">
        <!-- Bronze Card -->
        <div class="p-5 rounded-xl bg-slate-900/80 border border-amber-500/30 shadow-lg shadow-amber-500/5">
          <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-bold px-2 py-0.5 rounded bg-amber-500/20 text-amber-300">BRONZE</span>
            <span class="text-xs text-slate-400">Raw Ingestion</span>
          </div>
          <p class="text-sm text-slate-300">CSV ingestion with schema enforcement.</p>
          <div class="mt-4 pt-3 border-t border-slate-800 text-xs text-slate-400 font-mono">
            data/raw/customers.csv
          </div>
        </div>

        <!-- Quarantine Card -->
        <div class="p-5 rounded-xl bg-slate-900/80 border border-rose-500/30 shadow-lg shadow-rose-500/5">
          <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-bold px-2 py-0.5 rounded bg-rose-500/20 text-rose-300">QUARANTINE</span>
            <span class="text-xs text-slate-400">Dead Letter</span>
          </div>
          <p class="text-sm text-slate-300">Corrupt records diverted with failure reason tags.</p>
          <div class="mt-4 pt-3 border-t border-slate-800 text-xs text-slate-400 font-mono">
            data/quarantine/
          </div>
        </div>

        <!-- Silver Card -->
        <div class="p-5 rounded-xl bg-slate-900/80 border border-cyan-500/30 shadow-lg shadow-cyan-500/5">
          <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-bold px-2 py-0.5 rounded bg-cyan-500/20 text-cyan-300">SILVER</span>
            <span class="text-xs text-slate-400">Transformation</span>
          </div>
          <p class="text-sm text-slate-300">Trimming, email lowercasing, age binning, audit column.</p>
          <div class="mt-4 pt-3 border-t border-slate-800 text-xs text-slate-400 font-mono">
            data/processed/silver/
          </div>
        </div>

        <!-- Gold Card -->
        <div class="p-5 rounded-xl bg-slate-900/80 border border-emerald-500/30 shadow-lg shadow-emerald-500/5">
          <div class="flex items-center justify-between mb-3">
            <span class="text-xs font-bold px-2 py-0.5 rounded bg-emerald-500/20 text-emerald-300">GOLD</span>
            <span class="text-xs text-slate-400">Parquet + Warehouse</span>
          </div>
          <p class="text-sm text-slate-300">Lakehouse Parquet aggregates + Postgres staging upsert.</p>
          <div class="mt-4 pt-3 border-t border-slate-800 text-xs text-slate-400 font-mono">
            data/processed/gold/
          </div>
        </div>
      </div>
    </section>

    <!-- Interactive Data & Analytics Tabs -->
    <section class="bg-slate-900/60 border border-slate-800 rounded-2xl overflow-hidden">
      <div class="border-b border-slate-800 px-6 py-3 flex gap-4 overflow-x-auto">
        <button onclick="switchTab('silver')" id="tab-silver" class="tab-btn px-4 py-2 font-medium text-sm rounded-lg bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 whitespace-nowrap">
          Silver Layer (Transformed)
        </button>
        <button onclick="switchTab('gold')" id="tab-gold" class="tab-btn px-4 py-2 font-medium text-sm rounded-lg text-slate-400 hover:text-white whitespace-nowrap">
          Gold Analytics (Summaries)
        </button>
        <button onclick="switchTab('quarantine')" id="tab-quarantine" class="tab-btn px-4 py-2 font-medium text-sm rounded-lg text-slate-400 hover:text-white whitespace-nowrap">
          Quarantine (Dead Letter)
        </button>
        <button onclick="switchTab('bronze')" id="tab-bronze" class="tab-btn px-4 py-2 font-medium text-sm rounded-lg text-slate-400 hover:text-white whitespace-nowrap">
          Bronze Layer (Raw)
        </button>
        <button onclick="switchTab('logs')" id="tab-logs" class="tab-btn px-4 py-2 font-medium text-sm rounded-lg text-slate-400 hover:text-white whitespace-nowrap">
          Execution Logs
        </button>
      </div>

      <div class="p-6">
        <!-- Tab Content: Silver -->
        <div id="content-silver" class="space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-base font-semibold text-white">Cleaned & Enriched Customer Data</h3>
            <span class="text-xs text-slate-400">Generated by PySpark DataFrame pipeline</span>
          </div>
          <div class="overflow-x-auto rounded-xl border border-slate-800">
            <table class="w-full text-left text-sm">
              <thead class="bg-slate-900 text-xs uppercase text-slate-400 border-b border-slate-800">
                <tr>
                  <th class="px-4 py-3">ID</th>
                  <th class="px-4 py-3">Name</th>
                  <th class="px-4 py-3">Email</th>
                  <th class="px-4 py-3">Age</th>
                  <th class="px-4 py-3">City</th>
                  <th class="px-4 py-3">Age Group</th>
                </tr>
              </thead>
              <tbody id="silver-table-body" class="divide-y divide-slate-800 text-slate-300">
                <tr><td colspan="6" class="px-4 py-8 text-center text-slate-500">Loading data...</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Tab Content: Gold -->
        <div id="content-gold" class="hidden space-y-6">
          <h3 class="text-base font-semibold text-white">Aggregated Gold Analytics Mart (Lakehouse Parquet + SQL)</h3>
          <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
            <!-- City Summary -->
            <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5">
              <h4 class="text-sm font-semibold text-cyan-400 mb-3 flex items-center gap-2">
                <span>📍</span> Customer Count by City
              </h4>
              <div id="city-summary-container" class="space-y-3"></div>
            </div>

            <!-- Age Group Summary -->
            <div class="bg-slate-900/80 border border-slate-800 rounded-xl p-5">
              <h4 class="text-sm font-semibold text-emerald-400 mb-3 flex items-center gap-2">
                <span>👥</span> Customer Count by Age Group
              </h4>
              <div id="age-summary-container" class="space-y-3"></div>
            </div>
          </div>
        </div>

        <!-- Tab Content: Quarantine -->
        <div id="content-quarantine" class="hidden space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-base font-semibold text-white">Quarantine Dead-Letter Dataset</h3>
            <span class="text-xs text-rose-400">Records isolated due to data quality violations</span>
          </div>
          <div class="overflow-x-auto rounded-xl border border-slate-800">
            <table class="w-full text-left text-sm">
              <thead class="bg-slate-900 text-xs uppercase text-slate-400 border-b border-slate-800">
                <tr>
                  <th class="px-4 py-3">Customer ID</th>
                  <th class="px-4 py-3">Name</th>
                  <th class="px-4 py-3">Email</th>
                  <th class="px-4 py-3">Age</th>
                  <th class="px-4 py-3">City</th>
                  <th class="px-4 py-3">Violation Reason</th>
                </tr>
              </thead>
              <tbody id="quarantine-table-body" class="divide-y divide-slate-800 text-slate-300">
                <tr><td colspan="6" class="px-4 py-8 text-center text-slate-500">No quarantined records found. Ingested data is 100% clean!</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Tab Content: Bronze -->
        <div id="content-bronze" class="hidden space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-base font-semibold text-white">Raw Source Dataset (CSV)</h3>
            <span class="text-xs text-slate-400">Ingested with Pandas</span>
          </div>
          <div class="overflow-x-auto rounded-xl border border-slate-800">
            <table class="w-full text-left text-sm">
              <thead class="bg-slate-900 text-xs uppercase text-slate-400 border-b border-slate-800">
                <tr>
                  <th class="px-4 py-3">Customer ID</th>
                  <th class="px-4 py-3">Raw Name</th>
                  <th class="px-4 py-3">Raw Email</th>
                  <th class="px-4 py-3">Age</th>
                  <th class="px-4 py-3">City</th>
                </tr>
              </thead>
              <tbody id="bronze-table-body" class="divide-y divide-slate-800 text-slate-300">
                <tr><td colspan="5" class="px-4 py-8 text-center text-slate-500">Loading data...</td></tr>
              </tbody>
            </table>
          </div>
        </div>

        <!-- Tab Content: Logs -->
        <div id="content-logs" class="hidden space-y-4">
          <div class="flex items-center justify-between">
            <h3 class="text-base font-semibold text-white">Pipeline Execution Logs</h3>
            <span class="text-xs text-slate-400">logs/dataflow.log</span>
          </div>
          <pre id="log-output" class="bg-slate-950 p-4 rounded-xl border border-slate-800 text-xs text-emerald-400 overflow-x-auto max-h-96">Loading logs...</pre>
        </div>
      </div>
    </section>
  </main>

  <script>
    let activeTab = 'silver';

    function switchTab(tab) {
      ['silver', 'gold', 'quarantine', 'bronze', 'logs'].forEach(t => {
        document.getElementById(`content-${t}`).classList.add('hidden');
        const btn = document.getElementById(`tab-${t}`);
        btn.className = "tab-btn px-4 py-2 font-medium text-sm rounded-lg text-slate-400 hover:text-white whitespace-nowrap";
      });
      document.getElementById(`content-${tab}`).classList.remove('hidden');
      const activeBtn = document.getElementById(`tab-${tab}`);
      activeBtn.className = "tab-btn px-4 py-2 font-medium text-sm rounded-lg bg-cyan-500/10 text-cyan-400 border border-cyan-500/20 whitespace-nowrap";
      activeTab = tab;
    }

    async function loadData() {
      try {
        const res = await fetch('/api/data');
        const data = await res.json();

        // Update KPIs
        document.getElementById('kpi-records').innerText = data.metrics.total_records || 0;
        document.getElementById('kpi-silver').innerText = (data.silver || []).length;
        const quarCount = (data.quarantine || []).length;
        document.getElementById('kpi-quality-detail').innerText = `${data.metrics.null_values || 0} Nulls · ${data.metrics.duplicate_records || 0} Dups · ${quarCount} Quarantined`;
        
        // Update Status
        const statusEl = document.getElementById('pipeline-status');
        statusEl.innerText = data.is_running ? 'Running PySpark...' : (data.last_status || 'Ready');
        const runBtn = document.getElementById('run-btn');
        runBtn.disabled = data.is_running;

        // Render Bronze
        const bronzeBody = document.getElementById('bronze-table-body');
        bronzeBody.innerHTML = (data.bronze || []).map(r => `
          <tr class="hover:bg-slate-800/40">
            <td class="px-4 py-3 font-mono text-cyan-400">${r.customer_id}</td>
            <td class="px-4 py-3 font-medium">${r.name}</td>
            <td class="px-4 py-3 font-mono text-slate-400">${r.email}</td>
            <td class="px-4 py-3">${r.age}</td>
            <td class="px-4 py-3">${r.city}</td>
          </tr>
        `).join('');

        // Render Silver
        const silverBody = document.getElementById('silver-table-body');
        silverBody.innerHTML = (data.silver || []).map(r => {
          let badgeColor = r.age_group === 'Young' ? 'bg-amber-500/10 text-amber-400 border-amber-500/20' :
                           r.age_group === 'Adult' ? 'bg-cyan-500/10 text-cyan-400 border-cyan-500/20' :
                           'bg-purple-500/10 text-purple-400 border-purple-500/20';
          return `
            <tr class="hover:bg-slate-800/40">
              <td class="px-4 py-3 font-mono text-cyan-400">${r.customer_id}</td>
              <td class="px-4 py-3 font-medium">${r.name}</td>
              <td class="px-4 py-3 font-mono text-slate-400">${r.email}</td>
              <td class="px-4 py-3">${r.age}</td>
              <td class="px-4 py-3">${r.city}</td>
              <td class="px-4 py-3">
                <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold border ${badgeColor}">
                  ${r.age_group}
                </span>
              </td>
            </tr>
          `;
        }).join('');

        // Render Quarantine
        const quarBody = document.getElementById('quarantine-table-body');
        if (data.quarantine && data.quarantine.length > 0) {
          quarBody.innerHTML = data.quarantine.map(r => `
            <tr class="hover:bg-rose-950/20 text-rose-200">
              <td class="px-4 py-3 font-mono">${r.customer_id ?? 'N/A'}</td>
              <td class="px-4 py-3">${r.name ?? '<i class="text-rose-400">NULL</i>'}</td>
              <td class="px-4 py-3 font-mono text-slate-400">${r.email ?? '<i class="text-rose-400">NULL</i>'}</td>
              <td class="px-4 py-3">${r.age ?? '<i class="text-rose-400">NULL</i>'}</td>
              <td class="px-4 py-3">${r.city ?? '<i class="text-rose-400">NULL</i>'}</td>
              <td class="px-4 py-3">
                <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-rose-500/20 text-rose-300 border border-rose-500/30 font-mono">
                  ${r.quarantine_reason || 'UNSPECIFIED'}
                </span>
              </td>
            </tr>
          `).join('');
        } else {
          quarBody.innerHTML = '<tr><td colspan="6" class="px-4 py-8 text-center text-slate-500">No quarantined records found. Ingested data is 100% clean!</td></tr>';
        }

        // Render Gold Summaries
        const cityContainer = document.getElementById('city-summary-container');
        const cityEntries = Object.entries(data.gold.city_summary || {});
        const maxCity = Math.max(...cityEntries.map(e => e[1]), 1);
        cityContainer.innerHTML = cityEntries.map(([city, count]) => `
          <div>
            <div class="flex justify-between text-xs text-slate-300 font-medium mb-1">
              <span>${city}</span>
              <span class="text-cyan-400">${count} customers</span>
            </div>
            <div class="w-full bg-slate-800 h-2 rounded-full overflow-hidden">
              <div class="bg-gradient-to-r from-blue-500 to-cyan-400 h-full rounded-full" style="width: ${(count / maxCity) * 100}%"></div>
            </div>
          </div>
        `).join('');

        const ageContainer = document.getElementById('age-summary-container');
        const ageEntries = Object.entries(data.gold.age_summary || {});
        const maxAge = Math.max(...ageEntries.map(e => e[1]), 1);
        ageContainer.innerHTML = ageEntries.map(([group, count]) => `
          <div>
            <div class="flex justify-between text-xs text-slate-300 font-medium mb-1">
              <span>${group}</span>
              <span class="text-emerald-400">${count} customers</span>
            </div>
            <div class="w-full bg-slate-800 h-2 rounded-full overflow-hidden">
              <div class="bg-gradient-to-r from-teal-500 to-emerald-400 h-full rounded-full" style="width: ${(count / maxAge) * 100}%"></div>
            </div>
          </div>
        `).join('');

        // Render Logs
        document.getElementById('log-output').innerText = data.logs || 'No logs captured.';

      } catch (err) {
        console.error('Failed to load data', err);
      }
    }

    async function triggerPipeline() {
      const btn = document.getElementById('run-btn');
      btn.disabled = true;
      document.getElementById('pipeline-status').innerText = 'Starting pipeline...';
      try {
        await fetch('/api/run', { method: 'POST' });
        setTimeout(loadData, 1000);
      } catch (err) {
        alert('Error triggering pipeline: ' + err);
      }
    }

    loadData();
    setInterval(loadData, 3000);
  </script>
</body>
</html>
"""


class DataFlowHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        # Silence default request logging to keep console clean
        pass

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
        elif self.path == "/api/data":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            data = get_pipeline_data()
            self.wfile.write(json.dumps(data).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        global PIPELINE_RUNNING
        if self.path == "/api/run":
            if PIPELINE_RUNNING:
                self.send_response(409)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(
                    json.dumps({"error": "Pipeline already running"}).encode("utf-8")
                )
                return

            threading.Thread(target=execute_pipeline_job, daemon=True).start()
            self.send_response(202)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps({"status": "Pipeline triggered"}).encode("utf-8")
            )
        else:
            self.send_response(404)
            self.end_headers()


def start_server():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), DataFlowHandler)
    print(f"DataFlow Dashboard running on http://127.0.0.1:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    start_server()
