# ELT Runner — Architecture & Code Reference

## Overview

ELT Runner is a pipeline automation tool that extracts data from Oracle/Vertica,
converts to CSV, loads into a local DB (DuckDB/SQLite3/Oracle), transforms, and generates reports.
Supports both CLI (`runner.py`) and GUI (`batch_runner_gui.py`).

```
[Source DB] → EXPORT → CSV → LOAD → [Target DB] → TRANSFORM → REPORT → CSV/Excel
```

---

## Project Structure

```
elt_runner_master/
├── runner.py                    # CLI entry point (~500 lines)
├── batch_runner_gui.py          # GUI entry point (~10 lines)
│
├── engine/                      # Core engine (shared utilities)
│   ├── context.py               # RunContext dataclass
│   ├── stage_registry.py        # Stage name → function mapping
│   ├── runtime_state.py         # stop_event (thread-safe cancellation)
│   ├── connection.py            # connect_target() factory
│   ├── path_utils.py            # resolve_path() relative→absolute
│   └── sql_utils.py             # SQL sorting, table name, param detection, rendering
│
├── stages/                      # Pipeline stages (each has run(ctx) entry)
│   ├── export_stage.py          # Source DB → CSV extraction (662 lines)
│   ├── load_stage.py            # CSV → Target DB loading (251 lines)
│   ├── transform_stage.py       # SQL execution on Target DB (133 lines)
│   └── report_stage.py          # CSV generation → Excel conversion (367 lines)
│
├── adapters/
│   ├── sources/                 # Source DB connectors
│   │   ├── oracle_client.py     # Oracle thin/thick init, connection
│   │   ├── oracle_source.py     # Oracle export_sql_to_csv()
│   │   ├── vertica_client.py    # Vertica connection
│   │   └── vertica_source.py    # Vertica export_sql_to_csv()
│   └── targets/                 # Target DB loaders
│       ├── duckdb_target.py     # DuckDB load (Parquet fast-path)
│       ├── oracle_target.py     # Oracle load (schema creation, metadata)
│       └── sqlite_target.py     # SQLite3 load (type inference)
│
├── gui/                         # Tkinter GUI package
│   ├── __init__.py              # BatchRunnerGUI re-export
│   ├── constants.py             # THEMES, C, FONTS, TOOLTIPS, STAGE_CONFIG
│   ├── utils.py                 # load_jobs, load_env_hosts, scan_sql_params
│   ├── widgets.py               # SqlSelectorDialog, CollapsibleSection, Tooltip
│   ├── app.py                   # BatchRunnerGUI (Mixin assembly + __init__)
│   └── mixins/                  # Mixin modules (UI, state, run, dialogs, log, search)
│
├── config/env.sample.yml        # Environment config template
├── jobs/                        # Job definitions (two layouts supported)
│   ├── *.yml                    # Global layout (legacy)
│   └── {name}/{name}.yml        # Job-centric layout (recommended)
├── sql/                         # Shared SQL templates (optional)
└── VERSION                      # Single source of truth for version
```

---

## Pipeline Flow

```
runner.py main()
  │
  ├─ parse CLI args (--job, --env, --mode, --param, --set, --include, --stage)
  ├─ load job.yml + env.yml
  ├─ create RunContext
  │
  └─ run_pipeline(ctx)
       │
       ├─ [1] EXPORT  ─ export_stage.run(ctx)
       │     ThreadPoolExecutor → SQL queries → CSV + .meta.json
       │
       ├─ [2] LOAD    ─ load_stage.run(ctx)
       │     CSV files → Target DB (DuckDB/SQLite3/Oracle)
       │     _LOAD_HISTORY dedup by SHA256
       │
       ├─ [3] TRANSFORM ─ transform_stage.run(ctx)
       │     Execute SQL files on Target DB (schema injection)
       │
       └─ [4] REPORT  ─ report_stage.run(ctx)
             SQL → CSV, then CSV → Excel (.xlsx)
```

---

## 1. RunContext (`engine/context.py`)

All stages receive a single `RunContext` dataclass:

```python
@dataclass
class RunContext:
    job_name: str              # e.g. "job_duckdb"
    run_id: str                # e.g. "job_duckdb_01"
    job_config: dict           # parsed job.yml
    env_config: dict           # parsed env.yml (DB credentials)
    params: dict               # merged params (yml + CLI --param)
    work_dir: Path             # working directory
    mode: str                  # "plan" | "run" | "retry"
    logger: logging.Logger
    include_patterns: list     # --include SQL filter patterns
    stage_filter: list         # --stage filter list
```

---

## 2. Stage Registry (`engine/stage_registry.py`)

```python
STAGE_REGISTRY = {
    "export":      export_stage.run,
    "load":        load_stage.run,
    "load_local":  load_stage.run,   # backward compat alias
    "transform":   transform_stage.run,
    "report":      report_stage.run,
}
```

---

## 3. Export Stage (`stages/export_stage.py`)

The largest stage (~662 lines). Extracts data from Oracle/Vertica into CSV files.

### Key Functions

| Function | Purpose |
|----------|---------|
| `run(ctx)` | Main entry — orchestrates export |
| `expand_params(params)` | Generates all param combinations |
| `expand_range_value(value)` | Parses range strings |
| `build_csv_name(...)` | Builds output CSV filename |
| `get_thread_connection(...)` | Thread-local DB connection pooling |
| `sanitize_sql(sql)` | Removes trailing `;` and `/` |
| `backup_existing_file(...)` | Rotates old files (keep N backups) |

### Parameter Expansion

```python
# Single value
{"clsYymm": "202303"}  →  [{"clsYymm": "202303"}]

# Range (YYYYMM format, month-aware)
{"clsYymm": "202301:202312"}  →  12 combinations (202301..202312)

# Range with filter
"202001:202412~Q"   →  quarters only (03, 06, 09, 12)
"202001:202412~H"   →  half-years only (06, 12)
"202001:202412~Y"   →  year-end only (12)
"202001:202412~2"   →  specific month only (02 = February)

# List
{"region": "A,B,C"}  →  3 combinations

# Multi-param cartesian product
{"clsYymm": "202301:202303", "region": "A,B"}  →  6 combinations
```

### CSV Filename Rules

```
{sql_stem}__{host}__{param_key}_{param_value}[__...].csv[.gz]

Examples:
  full mode:    01_contract__local__clsYymm_202303.csv.gz
  compact mode: 01_contract__local__202303.csv.gz
  strip_prefix: contract__local__clsYymm_202303.csv.gz   (01_contract → contract)
  strip_prefix: qpv_005__local__clsYymm_202303.csv.gz   (3. qpv_005 → qpv_005)
  both:         contract__local__202303.csv.gz
```

### Metadata (.meta.json)

Each CSV gets a companion `.meta.json` file:

```json
{
  "sql_file": "01_contract.sql",
  "host": "local",
  "params": {"clsYymm": "202303"},
  "columns": [
    {"name": "CONTRACT_ID", "type": "DB_TYPE_NUMBER", "precision": 10, "scale": 0},
    {"name": "CONTRACT_AMT", "type": "DB_TYPE_NUMBER", "precision": 15, "scale": 2}
  ],
  "row_count": 15420,
  "exported_at": "2026-03-01 10:30:45"
}
```

### Parallel Execution

```yaml
export:
  parallel_workers: 4   # ThreadPoolExecutor thread count
```

Each thread gets its own DB connection via `threading.local()`.
Connections are tracked in `_thread_connections` list and closed at the end.

---

## 4. Load Stage (`stages/load_stage.py`)

Loads exported CSV files into the target database.

### Key Functions

| Function | Purpose |
|----------|---------|
| `run(ctx)` | Main entry — dispatches to adapter |
| `_extract_params(csv_path)` | Reads params from .meta.json (fallback: filename parsing) |
| `_sha256_file(path)` | File hash for dedup |
| `_collect_csv_info(...)` | Gathers file info for plan report |
| `_run_load_loop(...)` | Iterates CSV files and calls adapter |
| `_run_load_plan(...)` | Plan mode — prints file list without loading |

### Load Modes

| Mode | Behavior |
|------|----------|
| `replace` | DROP table → CREATE → INSERT (full rebuild) |
| `truncate` | DELETE all rows → INSERT (keep structure) |
| `delete` | DELETE WHERE param conditions → INSERT (surgical update) |
| `append` | INSERT only (keep existing data) |

### Dedup via _LOAD_HISTORY

Each target adapter maintains a `_LOAD_HISTORY` table:

```sql
CREATE TABLE _LOAD_HISTORY (
    job_name TEXT, table_name TEXT, file_name TEXT,
    file_hash TEXT, loaded_at TEXT
)
```

If the file's SHA256 hash matches a previous load, it's skipped.

### Delete Mode (Strict)

When `load.mode = delete`, the loader extracts params from `.meta.json`
and builds a `DELETE FROM table WHERE param_col = value` query.

Column matching: exact name first → then normalized (remove underscores).

```
params: {clsYymm: "202303"}
→ DELETE FROM TB_CONTRACT WHERE CLS_YYMM = '202303'
  (clsYymm → CLS_YYMM matched by removing underscores: CLSYYMM)
```

---

## 5. Transform Stage (`stages/transform_stage.py`)

Executes SQL files on the target DB for data transformation/aggregation.

### Key Behavior

1. Reads SQL files from `transform.sql_dir` (sorted by numeric prefix)
2. Renders parameters using `render_sql()`
3. Executes each SQL statement sequentially
4. `on_error: stop` (default) aborts on first error; `continue` skips and proceeds

### Schema Injection

```yaml
transform:
  schema: MYDATA
```

**Session schema** (set via GUI schema field):
- DuckDB: `SET schema = 'MYDATA'` — all tables resolve in that schema
- Oracle: `ALTER SESSION SET CURRENT_SCHEMA = MYDATA` — same effect
- This is independent of `@{}` param substitution

**`@{}` prefix in SQL** (set via Params):
- `@{src}TABLE_NAME` → `SRCSCHEMA.TABLE_NAME` (with param `src=SRCSCHEMA`)
- `@{tgt}TABLE_NAME` → `TGTSCHEMA.TABLE_NAME` (with param `tgt=TGTSCHEMA`)
- Empty value → prefix removed entirely

### Transfer (DB→DB)

Transfer mode ATTACHes a destination DB to the source DB during Transform,
allowing SQL to write to a different database:

```yaml
transform:
  transfer:
    dest:
      type: duckdb           # duckdb | sqlite3
      db_path: other.duckdb
```

- SQL can reference `dest.schema.table` to write to the attached DB
- Only DuckDB↔DuckDB and SQLite↔SQLite combinations supported (ATTACH mechanism)
- Configurable via GUI checkbox in the Transform section

---

## 6. Report Stage (`stages/report_stage.py`)

Generates CSV reports and converts them to Excel files.

### Two Modes

**Normal mode** (`skip_sql: false`):
1. Connect to target (or source) DB
2. Execute report SQL → save as CSV
3. Convert CSV → Excel (.xlsx)

**Skip-SQL mode** (`skip_sql: true`):
1. Read existing CSV files from `csv_union_dir`
2. Convert directly to Excel (no DB connection needed)

### Excel Generation

- Each SQL/CSV becomes a sheet in the workbook
- `max_files` limits sheets per Excel file (creates new file when exceeded)
- Column widths auto-calculated from data

---

## 7. SQL Utilities (`engine/sql_utils.py`)

### Parameter Syntax (4 types)

| Syntax | Behavior | Example |
|--------|----------|---------|
| `:param` | Auto-quoted, outside literals only | `:clsYymm` → `'202303'` |
| `${param}` | Raw substitution everywhere | `'${clsYymm}'` → `'202303'` |
| `{#param}` | Same as `${}` (alias) | `{#clsYymm}` → `202303` |
| `@{param}` | Dot-prefix (adds `.` if value exists) | `@{src}TABLE` → `SRCSCHEMA.TABLE` |

### Table Name Resolution

```sql
-- First non-empty line in SQL file:
--[MY_CUSTOM_TABLE]    ← uses this as table name
SELECT * FROM ...

-- If no hint found → uses filename stem:
-- 01_contract.sql → table name = "01_contract"
```

### SQL File Sorting

Files sorted by numeric prefix: `01_a.sql`, `02_b.sql`, `10_c.sql`.
Files without prefix sorted alphabetically after numbered files.

---

## 8. Target Adapters

### DuckDB (`adapters/targets/duckdb_target.py`)

- Fast CSV import via temporary Parquet conversion
- Schema support (`CREATE SCHEMA IF NOT EXISTS`)
- `_LOAD_HISTORY` dedup table
- `VACUUM` for file optimization

### Oracle (`adapters/targets/oracle_target.py`)

- Auto schema (user) creation with `CREATE USER` + `GRANT`
- Metadata-aware table creation (preserves source column types)
- NLS_DATE_FORMAT session setting
- Bulk insert with array binding
- All 4 load modes supported (replace/truncate/delete/append)

### SQLite3 (`adapters/targets/sqlite_target.py`)

- Auto type inference from data (INTEGER/REAL/TEXT)
- No metadata support (DBAPI limitation)
- replace/truncate/delete/append modes

---

## 9. Connection Factory (`engine/connection.py`)

```python
conn, conn_type, label = connect_target(ctx, target_cfg)
# conn_type: "duckdb" | "sqlite3" | "oracle"
# label: "duckdb (C:\data\result.duckdb)"
```

Dispatches to the appropriate adapter's `connect()` function
based on `target.type` in job config.

### DuckDB Performance Settings (Auto-applied)

When connecting to DuckDB, `_apply_duckdb_settings()` automatically configures `memory_limit` and `threads`:

| Setting | YAML Key | Default (when omitted) | Example |
|---------|----------|----------------------|---------|
| Memory limit | `target.memory_limit` | System RAM × 75% | `12GB` |
| Thread count | `target.threads` | Logical CPUs ÷ 2 | `4` |

Cross-platform detection order:
1. `psutil` (if installed)
2. Windows: `ctypes` Win32 API (`GetPhysicallyInstalledSystemMemory`)
3. Linux/macOS: `os.sysconf`
4. Fallback: `memory_limit=4GB`, `threads=1`

---

## 10. CLI Usage (`runner.py`)

```bash
# Basic run (job-centric or global path both work)
python runner.py --job jobs/job_duckdb/job_duckdb.yml --env config/env.yml
python runner.py --job jobs/job_duckdb.yml --env config/env.yml  # also works

# Plan mode (dry run)
python runner.py --job jobs/job_duckdb.yml --mode plan

# Retry failed tasks
python runner.py --job jobs/job_duckdb.yml --mode retry

# Override params
python runner.py --job jobs/job_duckdb.yml --param clsYymm=202301:202312

# Override config values
python runner.py --job jobs/job_duckdb.yml \
  --set export.compression=none \
  --set target.db_path=data/custom.duckdb

# Filter specific SQL files
python runner.py --job jobs/job_duckdb.yml --include contract --include payment

# Run specific stages only
python runner.py --job jobs/job_duckdb.yml --stage export --stage load_local

# Debug mode (verbose logging)
python runner.py --job jobs/job_duckdb.yml --debug
```

---

## 11. Job YAML Structure

Job-centric layout (`jobs/{name}/{name}.yml`). Path fields can be omitted
when using job-centric conventions — defaults are resolved automatically
based on `jobs/{job_name}/` folder existence.

```yaml
job_name: my_job

pipeline:
  stages: [export, load_local, transform, report]

source:
  type: oracle              # oracle | vertica
  host: local               # key from env.yml

export:
  # sql_dir / out_dir omitted → job-centric defaults:
  #   sql_dir: jobs/my_job/sql/export
  #   out_dir: jobs/my_job/data/export
  overwrite: true
  parallel_workers: 4
  compression: gzip         # gzip | none
  format: csv
  csv_name_style: full      # full | compact
  csv_strip_prefix: false   # strip numeric prefix (separators: _ . - space)

load:
  mode: replace             # replace | truncate | delete | append

target:
  type: duckdb              # duckdb | sqlite3 | oracle
  # db_path omitted → jobs/my_job/data/my_job.duckdb
  schema: MY_SCHEMA         # optional
  memory_limit: 12GB        # optional (default: 75% of system RAM)
  threads: 4                # optional (default: logical CPUs / 2)

transform:
  # sql_dir omitted → jobs/my_job/sql/transform
  on_error: stop            # stop | continue
  schema: MY_SCHEMA         # optional (overrides target.schema)
  # transfer:               # optional DB→DB transfer
  #   dest:
  #     type: duckdb
  #     db_path: other.duckdb

report:
  source: target            # target | oracle | vertica
  skip_sql: false
  csv_union_dir: data/export
  export_csv:
    enabled: true
    # sql_dir / out_dir omitted → job-centric defaults
  excel:
    enabled: true
    max_files: 10

params:
  clsYymm: "202301:202312"
```

### Path resolution priority

```
yml explicit path  >  job-centric convention  >  global defaults
```

---

## 12. Environment Config (`config/env.yml`)

```yaml
sources:
  oracle:
    thick:
      instant_client: "C:\\oracle\\instantclient_21_3"  # optional
    run:
      hosts: [local]
    export:
      fetch_size: 20000
      timeout_seconds: 1800
    hosts:
      local:
        dsn: 127.0.0.1:1521/ORCLPDB
        user: my_user
        password: "my_password"

  vertica:
    run:
      hosts: [pdwvdbs]
    export:
      fetch_size: 50000
    hosts:
      pdwvdbs:
        host: 10.0.0.200
        port: 5433
        database: MYDB
        user: my_user
        password: "my_password"
```
