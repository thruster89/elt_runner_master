"""
gui/mixins/state_job.py  ─  스냅샷, Job 관리, 설정 저장/로드
"""

from __future__ import annotations

import json
import tkinter as tk
from tkinter import messagebox
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from gui.constants import C, FONTS, THEMES, APP_VERSION, _CONF_PATH, STAGE_CONFIG
from gui.utils import load_jobs, load_env_hosts, _scan_params_from_files, clear_sql_tree_cache
from gui.utils import collect_sql_tree as _collect_sql_tree
from gui.utils import flatten_sql_tree as _flatten_sql_tree
from gui.widgets import SqlSelectorDialog
from engine.path_utils import get_job_defaults

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI


class StateJobMixin:

    @staticmethod
    def _safe_int(val, default: int = 0) -> int:
        """문자열을 int로 변환. 실패 시 default 반환."""
        try:
            return int(val)
        except (ValueError, TypeError):
            return default

    # ── 스냅샷 ────────────────────────────────────────────────
    def _snapshot(self: "BatchRunnerGUI") -> dict:
        """현재 GUI 설정 전체를 dict로 반환"""
        return {
            "job":         self.job_var.get(),
            "mode":        self.mode_var.get(),
            "env_path":    self._env_path_var.get(),
            "source_type": self._source_type_var.get(),
            "source_host": self._source_host_var.get(),
            "target_type": self._target_type_var.get(),
            "target_db_path": self._target_db_path.get(),
            "target_schema":  self._target_schema.get(),
            "export_sql_dir": self._export_sql_dir.get(),
            "export_out_dir": self._export_out_dir.get(),
            "load_csv_dir":   self._load_csv_dir.get(),
            "transform_target_type": self._transform_target_type.get(),
            "transform_db_path":    self._transform_db_path.get(),
            "transfer_enabled":     self._transfer_enabled.get(),
            "transfer_dest_type":   self._transfer_dest_type.get(),
            "transfer_dest_db_path": self._transfer_dest_db_path.get(),
            "transform_schema":  self._transform_schema.get(),
            "transform_sql_dir": self._transform_sql_dir.get(),
            "transform_out_dir": self._transform_out_dir.get(),
            "report_sql_dir":    self._report_sql_dir.get(),
            "report_out_dir":    self._report_out_dir.get(),
            "report_schema":     self._report_schema.get(),
            "stage_export":     self._stage_export.get(),
            "stage_load_local": self._stage_load_local.get(),
            "stage_transform":  self._stage_transform.get(),
            "stage_report":     self._stage_report.get(),
            "param_mode":  self._param_mode_var.get(),
            "transform_param_mode": self._transform_param_mode_var.get(),
            "report_param_mode":    self._report_param_mode_var.get(),
            "stage_params": {
                stage: [(k.get(), v.get()) for k, v in entries]
                for stage, entries in self._stage_param_entries.items()
            },
            "overrides": {
                "overwrite":    self._ov_overwrite.get(),
                "workers":      self._ov_workers.get(),
                "compression":  self._ov_compression.get(),
                "load_mode":    self._ov_load_mode.get(),
                "delimiter":    self._ov_delimiter.get(),
                "encoding":     self._ov_encoding.get(),
                "on_error":     self._ov_on_error.get(),
                "excel":        self._ov_excel.get(),
                "csv":          self._ov_csv.get(),
                "max_files":    self._ov_max_files.get(),
                "csv_filter":   self._ov_csv_filter.get(),
                "sheet_mode":   self._ov_sheet_mode.get(),
                "skip_sql":     self._ov_skip_sql.get(),
                "union_dir":    self._ov_union_dir.get(),
                "timeout":      self._ov_timeout.get(),
                "name_style":   self._ov_name_style.get(),
                "strip_prefix": self._ov_strip_prefix.get(),
            },
        }

    def _restore_snapshot(self: "BatchRunnerGUI", snap: dict):
        """스냅샷으로 GUI 설정 복원 (저장된 경로를 그대로 복원)"""
        self._source_type_var.set(snap.get("source_type", "oracle"))
        if hasattr(self, "_source_type_combo"):
            self._on_source_type_change()
        self._source_host_var.set(snap.get("source_host", ""))

        self._target_type_var.set(snap.get("target_type", "duckdb"))
        self._target_db_path.set(snap.get("target_db_path", "data/local/result.duckdb"))
        self._target_schema.set(snap.get("target_schema", ""))
        if hasattr(self, "_db_path_row"):
            self._update_target_visibility()
        self._update_load_mode_options()

        self._export_sql_dir.set(snap.get("export_sql_dir", "sql/export"))
        self._export_out_dir.set(snap.get("export_out_dir", "data/export"))
        self._load_csv_dir.set(snap.get("load_csv_dir", ""))
        self._transform_target_type.set(snap.get("transform_target_type", "(global)"))
        self._transform_db_path.set(snap.get("transform_db_path", ""))
        self._transfer_enabled.set(snap.get("transfer_enabled", False))
        self._transfer_dest_type.set(snap.get("transfer_dest_type", "duckdb"))
        self._transfer_dest_db_path.set(snap.get("transfer_dest_db_path", ""))
        self._transform_schema.set(snap.get("transform_schema", ""))
        self._transform_sql_dir.set(snap.get("transform_sql_dir", "sql/transform"))
        self._transform_out_dir.set(snap.get("transform_out_dir", ""))
        self._report_sql_dir.set(snap.get("report_sql_dir", "sql/report"))
        self._report_out_dir.set(snap.get("report_out_dir", "data/report"))
        self._report_schema.set(snap.get("report_schema", ""))

        self._stage_export.set(snap.get("stage_export", True))
        self._stage_load_local.set(snap.get("stage_load_local", True))
        self._stage_transform.set(snap.get("stage_transform", True))
        self._stage_report.set(snap.get("stage_report", True))
        if self._stage_buttons:
            self._refresh_stage_buttons()

        self._param_mode_var.set(snap.get("param_mode", "product"))
        self._transform_param_mode_var.set(snap.get("transform_param_mode", "product"))
        self._report_param_mode_var.set(snap.get("report_param_mode", "product"))
        stage_params = snap.get("stage_params")
        if stage_params and isinstance(stage_params, dict):
            self._refresh_param_rows_from_stages(stage_params)
        else:
            # 구 형식 호환: flat list → export 스테이지에 배치
            self._refresh_param_rows(snap.get("params", []))

        self.mode_var.set(snap.get("mode", "run"))
        self._env_path_var.set(snap.get("env_path", "config/env.yml"))

        job = snap.get("job", "")
        if job and job in self._jobs:
            self.job_var.set(job)

        ov = snap.get("overrides", {})
        self._ov_overwrite.set(ov.get("overwrite", False))
        self._ov_workers.set(ov.get("workers", 1))
        self._ov_compression.set(ov.get("compression", "gzip"))
        self._ov_load_mode.set(ov.get("load_mode", "replace"))
        self._ov_delimiter.set(ov.get("delimiter", "auto"))
        self._ov_encoding.set(ov.get("encoding", "auto"))
        self._ov_on_error.set(ov.get("on_error", "stop"))
        self._ov_excel.set(ov.get("excel", True))
        self._ov_csv.set(ov.get("csv", True))
        self._ov_max_files.set(ov.get("max_files", 10))
        self._ov_csv_filter.set(ov.get("csv_filter", ""))
        self._ov_sheet_mode.set(ov.get("sheet_mode", "merge"))
        self._ov_skip_sql.set(ov.get("skip_sql", False))
        self._ov_union_dir.set(ov.get("union_dir", ""))
        self._ov_timeout.set(ov.get("timeout", "1800"))
        self._ov_name_style.set(ov.get("name_style", "full"))
        self._ov_strip_prefix.set(ov.get("strip_prefix", False))
        # NOTE: _refresh_preview()는 호출자가 _restoring_job=False 후 직접 호출

    def _is_dirty(self: "BatchRunnerGUI") -> bool:
        """현재 상태가 로드 시점 스냅샷과 다른지 확인.
        고빈도 경로 (_update_title_dirty) 에서는 캐시된 플래그 사용,
        저빈도 경로 (save 확인) 에서만 전체 비교."""
        if self._job_loaded_snapshot is None:
            return False
        # 캐시된 dirty 플래그가 있으면 즉시 반환 (고빈도 경로 최적화)
        cached = getattr(self, "_dirty_cached", None)
        if cached is not None:
            return cached
        result = self._snapshot() != self._job_loaded_snapshot
        self._dirty_cached = result
        return result

    def _get_changed_fields(self: "BatchRunnerGUI") -> list[str]:
        """변경된 필드명 리스트 반환"""
        if self._job_loaded_snapshot is None:
            return []
        cur = self._snapshot()
        old = self._job_loaded_snapshot
        changed = []
        for key in cur:
            if key == "overrides":
                for ok in cur.get("overrides", {}):
                    if cur["overrides"].get(ok) != old.get("overrides", {}).get(ok):
                        changed.append(f"overrides.{ok}")
            elif cur.get(key) != old.get(key):
                changed.append(key)
        return changed

    def _base_title(self: "BatchRunnerGUI") -> str:
        """현재 Job 기준 기본 타이틀 (큐/dirty 표시 제외)."""
        base = f"ELT Runner  v{APP_VERSION}"
        fname = self.job_var.get()
        if fname:
            base = f"{fname} - {base}"
        return base

    def _update_title_dirty(self: "BatchRunnerGUI"):
        """타이틀 바에 변경 표시(*) 업데이트 (이전과 동일하면 스킵)"""
        # Queue 실행 중에는 큐 타이틀이 우선 (덮어쓰지 않음)
        if getattr(self, "_job_queue_total", 0) > 0 and getattr(self, "_job_queue", []):
            return
        base = self._base_title()
        dirty = self._is_dirty()
        new_title = f"* {base}" if dirty else base
        if getattr(self, "_cached_title", None) != new_title:
            self._cached_title = new_title
            self.title(new_title)

    def _capture_loaded_snapshot(self: "BatchRunnerGUI"):
        """현재 GUI 상태를 로드 시점 스냅샷으로 캡처 (after로 지연)"""
        self._job_loaded_snapshot = self._snapshot()
        self._dirty_cached = False  # 방금 캡처했으므로 clean
        self._update_title_dirty()

    def _build_csv_filter_cfg(self: "BatchRunnerGUI") -> dict:
        """csv_filter GUI 값을 config dict로 변환 (빈값이면 빈 dict)"""
        raw = self._ov_csv_filter.get().strip()
        if not raw:
            return {}
        parts = [p.strip() for p in raw.split(",") if p.strip()]
        if len(parts) == 1:
            return {"csv_filter": parts[0]}
        return {"csv_filter": parts}

    # ── 경로 내 job_name 교체 ────────────────────────────────────
    @staticmethod
    def _remap_job_paths(cfg: dict, old_name: str, new_name: str) -> dict:
        """config dict 내 경로 문자열에서 old job_name을 new job_name으로 교체.

        jobs/{old}/... → jobs/{new}/...  및  {old}.duckdb → {new}.duckdb 등을 변환.
        """
        if not old_name or old_name == new_name:
            return cfg

        def _replace(val: str) -> str:
            if not isinstance(val, str):
                return val
            val = val.replace(f"jobs/{old_name}/", f"jobs/{new_name}/")
            val = val.replace(f"jobs\\{old_name}\\", f"jobs\\{new_name}\\")
            val = val.replace(f"/{old_name}.duckdb", f"/{new_name}.duckdb")
            val = val.replace(f"\\{old_name}.duckdb", f"\\{new_name}.duckdb")
            val = val.replace(f"/{old_name}.sqlite", f"/{new_name}.sqlite")
            val = val.replace(f"\\{old_name}.sqlite", f"\\{new_name}.sqlite")
            return val

        path_keys = {"sql_dir", "out_dir", "db_path", "csv_dir",
                     "csv_union_dir", "temp_directory"}

        def _walk(d: dict):
            for k, v in d.items():
                if isinstance(v, dict):
                    _walk(v)
                elif isinstance(v, str) and k in path_keys:
                    d[k] = _replace(v)

        _walk(cfg)
        return cfg

    # ── GUI config 빌드 ────────────────────────────────────────
    def _build_gui_config(self: "BatchRunnerGUI") -> dict:
        """GUI 전체 상태를 job yml dict로 조립"""
        stages = [s for s in ("export", "load_local", "transform", "report")
                  if getattr(self, f"_stage_{s}").get()]
        cfg = {
            "job_name": Path(self.job_var.get()).stem if self.job_var.get() else "gui_run",
            "pipeline": {"stages": stages},
            "source": {
                "type": self._source_type_var.get(),
                "host": self._source_host_var.get(),
            },
            "export": {
                "sql_dir": self._export_sql_dir.get(),
                "out_dir": self._export_out_dir.get(),
                "overwrite": self._ov_overwrite.get(),
                "parallel_workers": self._ov_workers.get(),
                "compression": self._ov_compression.get(),
                "csv_name_style": self._ov_name_style.get(),
                "csv_strip_prefix": self._ov_strip_prefix.get(),
                "timeout_seconds": self._safe_int(self._ov_timeout.get(), 1800),
                "format": "csv",
                **({"params": {k.get().strip(): v.get().strip()
                    for k, v in self._stage_param_entries.get("export", [])
                    if k.get().strip()}}
                   if any(k.get().strip() for k, _ in self._stage_param_entries.get("export", [])) else {}),
                "param_mode": self._param_mode_var.get(),
            },
            "load": {
                "mode": self._ov_load_mode.get(),
                **({"delimiter": self._ov_delimiter.get().strip()}
                   if self._ov_delimiter.get().strip()
                   and self._ov_delimiter.get().strip() != "auto" else {}),
                **({"encoding": self._ov_encoding.get().strip()}
                   if self._ov_encoding.get().strip()
                   and self._ov_encoding.get().strip() not in ("auto", "utf-8") else {}),
                **({"csv_dir": self._load_csv_dir.get().strip()}
                   if self._load_csv_dir.get().strip() else {}),
            },
            "target": {
                "type": self._target_type_var.get(),
            },
            "transform": {
                "sql_dir": self._transform_sql_dir.get(),
                "on_error": self._ov_on_error.get(),
                **({"out_dir": self._transform_out_dir.get().strip()}
                   if self._transform_out_dir.get().strip() else {}),
                **({"schema": self._transform_schema.get().strip()}
                   if self._transform_schema.get().strip() else {}),
                **({"params": {k.get().strip(): v.get().strip()
                    for k, v in self._stage_param_entries.get("transform", [])
                    if k.get().strip()}}
                   if any(k.get().strip() for k, _ in self._stage_param_entries.get("transform", [])) else {}),
                "param_mode": self._transform_param_mode_var.get(),
            },
            "report": {
                "source": "target",
                **({"schema": self._report_schema.get().strip()}
                   if self._report_schema.get().strip() else {}),
                "export_csv": {
                    "enabled": self._ov_csv.get(),
                    "sql_dir": self._report_sql_dir.get(),
                    "out_dir": self._report_out_dir.get(),
                },
                "excel": {
                    "enabled": self._ov_excel.get(),
                    "out_dir": self._report_out_dir.get(),
                    "max_files": self._ov_max_files.get(),
                    "sheet_mode": self._ov_sheet_mode.get(),
                    **(_csv_filter_cfg if (_csv_filter_cfg := self._build_csv_filter_cfg()) else {}),
                },
                **({"params": {k.get().strip(): v.get().strip()
                    for k, v in self._stage_param_entries.get("report", [])
                    if k.get().strip()}}
                   if any(k.get().strip() for k, _ in self._stage_param_entries.get("report", [])) else {}),
                "param_mode": self._report_param_mode_var.get(),
            },
        }
        # export params를 글로벌 params로 승격 (transform/report에서 fallback 사용 가능)
        export_params = cfg["export"].get("params", {})
        if export_params:
            cfg["params"] = dict(export_params)

        # target specifics
        tgt_type = self._target_type_var.get()
        if tgt_type in ("duckdb", "sqlite3") and self._target_db_path.get().strip():
            cfg["target"]["db_path"] = self._target_db_path.get().strip()
        if self._target_schema.get().strip():
            cfg["target"]["schema"] = self._target_schema.get().strip()
        # transform 전용 target (글로벌과 다를 때만 설정)
        tfm_type = self._transform_target_type.get()
        if tfm_type and tfm_type != "(global)":
            cfg["transform"]["target"] = {"type": tfm_type}
            if tfm_type in ("duckdb", "sqlite3") and self._transform_db_path.get().strip():
                cfg["transform"]["target"]["db_path"] = self._transform_db_path.get().strip()
        # transfer (DB→DB 전송)
        if self._transfer_enabled.get():
            dest_type = self._transfer_dest_type.get()
            transfer_cfg: dict = {"dest": {"type": dest_type}}
            if dest_type in ("duckdb", "sqlite3") and self._transfer_dest_db_path.get().strip():
                transfer_cfg["dest"]["db_path"] = self._transfer_dest_db_path.get().strip()
            cfg["transform"]["transfer"] = transfer_cfg
        if not self._ov_csv.get():
            cfg["report"]["skip_sql"] = True
        if self._ov_union_dir.get().strip():
            cfg["report"]["csv_union_dir"] = self._ov_union_dir.get().strip()
        return cfg

    # ── 설정 저장/복원 ─────────────────────────────────────────
    def _save_geometry(self: "BatchRunnerGUI"):
        try:
            conf = {
                "geometry": self.geometry(),
                "recent_dirs": self._recent_dirs[:10],
                "snapshot": self._snapshot(),
            }
            if not getattr(self, "_theme_from_env", False):
                conf["theme"] = self._theme_var.get()
            _CONF_PATH.write_text(
                json.dumps(conf, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

    def _load_geometry(self: "BatchRunnerGUI"):
        try:
            if _CONF_PATH.exists():
                conf = json.loads(_CONF_PATH.read_text(encoding="utf-8"))
                if "geometry" in conf:
                    self.geometry(conf["geometry"])
                if "theme" in conf and conf["theme"] in THEMES:
                    if not getattr(self, "_theme_from_env", False) and conf["theme"] != self._theme_var.get():
                        self._theme_var.set(conf["theme"])
                        self._apply_theme()
                # 최근 디렉토리 복원
                if "recent_dirs" in conf:
                    self._recent_dirs = conf["recent_dirs"][:10]
                    if hasattr(self, "_wd_entry"):
                        self._wd_entry["values"] = self._recent_dirs
                # 마지막 설정 복원 (새 창이면 스킵 → _default.yml 사용)
                if "snapshot" in conf and not getattr(self, "_is_new_window", False):
                    self._restore_snapshot(conf["snapshot"])
        except Exception:
            pass

    def _on_close(self: "BatchRunnerGUI"):
        if self._process and self._process.poll() is None:
            messagebox.showwarning("Running", "A task is currently running.\nStop it before closing.")
            return
        if self._is_dirty():
            ans = messagebox.askyesnocancel(
                "Unsaved Changes",
                "You have unsaved changes.\nSave before exit?")
            if ans is None:  # Cancel
                return
            if ans:  # Yes
                self._on_save_yml()
        # 예약/타이머 정리 (destroy 후 TclError 방지)
        self._closing = True
        for attr in ("_schedule_id", "_clock_timer_id",
                      "_anim_id", "_elapsed_job_id", "_preview_debounce_id"):
            tid = getattr(self, attr, None)
            if tid:
                self.after_cancel(tid)
                setattr(self, attr, None)
        self._stop_idle_timer()
        self._save_geometry()
        self.destroy()

    # ── Job 관리 ────────────────────────────────────────────────
    def _on_job_change(self: "BatchRunnerGUI", *_):
        fname = self.job_var.get()
        cfg = self._jobs.get(fname, {})
        if not cfg:
            return

        # 미저장 변경 경고 — job 필드 자체의 변경은 제외하고 비교
        if not self._restoring_job and self._job_loaded_snapshot is not None:
            prev_job = self._job_loaded_snapshot.get("job", "")
            cur_snap = self._snapshot()
            cur_snap["job"] = prev_job  # job 전환 자체는 dirty가 아님
            if cur_snap != self._job_loaded_snapshot:
                ans = messagebox.askyesnocancel(
                    "Unsaved Changes",
                    "You have unsaved changes.\nSave now?")
                if ans is None:  # Cancel → 이전 job으로 롤백
                    self.job_var.set(prev_job)
                    return
                if ans:  # Yes → 저장 (이전 job 이름으로 복원 후 저장)
                    self.job_var.set(prev_job)
                    self._on_save_yml()
                    self.job_var.set(fname)

        self._restoring_job = True

        # ── Job convention 기본값 결정 ──
        job_name = cfg.get("job_name", Path(fname).stem if fname else "")
        tgt = cfg.get("target", {})
        tgt_type = tgt.get("type", "duckdb")
        work_dir = Path(self._work_dir.get())
        defaults = get_job_defaults(work_dir, job_name, tgt_type)

        # Source
        src = cfg.get("source", {})
        src_type = src.get("type", "oracle")
        self._source_type_var.set(src_type)
        self._on_source_type_change()
        self._source_host_var.set(src.get("host", ""))

        # Target
        self._target_type_var.set(tgt_type)
        self._target_db_path.set(tgt.get("db_path", defaults["target_db_path"]))
        self._target_schema.set(tgt.get("schema", ""))
        self._update_target_visibility()
        self._update_load_mode_options()

        # Export paths
        exp = cfg.get("export", {})
        self._export_sql_dir.set(exp.get("sql_dir", defaults["export_sql_dir"]))
        self._export_out_dir.set(exp.get("out_dir", defaults["export_out_dir"]))

        # Load
        load_cfg = cfg.get("load", {})
        self._load_csv_dir.set(load_cfg.get("csv_dir", ""))

        # Transform target / paths
        tfm = cfg.get("transform", {})
        tfm_tgt = tfm.get("target", {})
        tfm_tgt_type = tfm_tgt.get("type", "").strip()
        self._transform_target_type.set(tfm_tgt_type if tfm_tgt_type else "(global)")
        self._transform_db_path.set(tfm_tgt.get("db_path", ""))
        self._transform_schema.set(tfm.get("schema", ""))
        # Transfer 설정 복원
        xfr = tfm.get("transfer", {})
        self._transfer_enabled.set(bool(xfr.get("dest")))
        xfr_dest = xfr.get("dest", {})
        self._transfer_dest_type.set(xfr_dest.get("type", "duckdb"))
        self._transfer_dest_db_path.set(xfr_dest.get("db_path", ""))
        # sql_dir: job convention 우선, 없으면 기존 로직 (transform 전용 target type 기반)
        effective_type = tfm_tgt_type if tfm_tgt_type else tgt_type
        if defaults["job_dir_exists"]:
            transform_sql_default = defaults["transform_sql_dir"]
        else:
            transform_sql_default = f"sql/transform/{effective_type}"
        self._transform_sql_dir.set(tfm.get("sql_dir", transform_sql_default))
        self._transform_out_dir.set(tfm.get("out_dir", ""))
        rep = cfg.get("report", {})
        rep_csv = rep.get("export_csv", {})
        self._report_sql_dir.set(rep_csv.get("sql_dir", defaults["report_sql_dir"]))
        self._report_out_dir.set(rep_csv.get("out_dir", rep.get("excel", {}).get("out_dir", defaults["report_out_dir"])))
        self._report_schema.set(rep.get("schema", ""))

        # Stages
        stages = cfg.get("pipeline", {}).get("stages", [])
        self._stage_export.set("export" in stages)
        self._stage_load_local.set("load_local" in stages)
        self._stage_transform.set("transform" in stages)
        self._stage_report.set("report" in stages)
        self._refresh_stage_buttons()

        # Advanced overrides
        self._ov_overwrite.set(bool(exp.get("overwrite", False)))
        self._ov_workers.set(int(exp.get("parallel_workers", 1)))
        self._ov_compression.set(str(exp.get("compression", "gzip")))
        self._ov_name_style.set(str(exp.get("csv_name_style", "full")))
        self._ov_strip_prefix.set(bool(exp.get("csv_strip_prefix", False)))
        self._ov_timeout.set(str(exp.get("timeout_seconds", "1800")))
        self._ov_load_mode.set(str(cfg.get("load", {}).get("mode", "replace")))
        self._ov_delimiter.set(str(cfg.get("load", {}).get("delimiter", "auto")) or "auto")
        self._ov_encoding.set(str(cfg.get("load", {}).get("encoding", "auto")) or "auto")
        self._ov_on_error.set(str(tfm.get("on_error", "stop")))
        self._ov_excel.set(bool(rep.get("excel", {}).get("enabled", True)))
        csv_enabled = bool(rep_csv.get("enabled", True))
        if rep.get("skip_sql", False):
            csv_enabled = False
        self._ov_csv.set(csv_enabled)
        self._ov_max_files.set(int(rep.get("excel", {}).get("max_files", 10)))
        csv_filter = rep.get("excel", {}).get("csv_filter", "")
        if isinstance(csv_filter, list):
            csv_filter = ", ".join(str(x) for x in csv_filter)
        self._ov_csv_filter.set(str(csv_filter))
        self._ov_sheet_mode.set(str(rep.get("excel", {}).get("sheet_mode", "merge")))
        self._ov_union_dir.set(str(rep.get("csv_union_dir", "")))

        # Param mode (per-stage)
        self._param_mode_var.set(exp.get("param_mode", cfg.get("param_mode", "product")))
        self._transform_param_mode_var.set(tfm.get("param_mode", "product"))
        self._report_param_mode_var.set(rep.get("param_mode", "product"))

        # Params — per-stage 우선, 없으면 top-level fallback
        exp_params = exp.get("params", {})
        tfm_params = tfm.get("params", {})
        rep_params = rep.get("params", {})
        if exp_params or tfm_params or rep_params:
            grouped = []
            if exp_params:
                grouped.append(("export", list(exp_params.items())))
            if tfm_params:
                grouped.append(("transform", list(tfm_params.items())))
            if rep_params:
                grouped.append(("report", list(rep_params.items())))
            self._refresh_param_rows_grouped(grouped)
        else:
            # 구 형식: top-level params → export에 배치 후 scan으로 재분배
            params = cfg.get("params", {})
            self._refresh_param_rows(list(params.items()))
        self.after(50, self._scan_and_suggest_params)

        # SQL 선택 초기화 — Refresh(같은 job 재로드) 시에는 유지
        if not self._restoring_job:
            self._selected_sqls = set()
            self._selected_transform_sqls = set()
            self._selected_report_sqls = set()
        self._update_sql_preview()
        self._update_transform_sql_preview()
        self._update_report_sql_preview()

        self._restoring_job = False
        # 복원 중 스킵된 콜백 일괄 1회 실행
        self._update_section_visibility()
        self._update_transform_target_visibility()
        self._update_transfer_visibility()
        self._refresh_preview()
        self.after(100, self._capture_loaded_snapshot)

    def _on_save_yml(self: "BatchRunnerGUI"):
        """현재 선택된 job 파일을 덮어쓰기. 선택된 job 없으면 save as fallback."""
        fname = self.job_var.get()
        if not fname:
            self._on_save_yml_as()
            return
        # 변경 없으면 스킵
        if not self._is_dirty():
            self._log_sys("No changes to save.")
            return
        # 확인 팝업
        changed = self._get_changed_fields()
        if not self._show_save_confirm(fname, changed):
            return
        jobs_dir = self._jobs_dir()
        # job-centric: jobs/{name}/{name}.yml 우선
        stem = Path(fname).stem
        jc_dir = jobs_dir / stem
        if jc_dir.is_dir():
            out_path = jc_dir / fname
        else:
            out_path = jobs_dir / fname
        new_cfg = self._build_gui_config()
        # db_path 안에 다른 job_name이 들어있으면 경고 + 자동 수정 제안
        db_path = new_cfg.get("target", {}).get("db_path", "")
        if db_path and f"/{stem}." not in db_path and f"\\{stem}." not in db_path:
            if messagebox.askyesno(
                    "Path Mismatch",
                    f"target.db_path 경로가 현재 job 이름({stem})과 "
                    f"일치하지 않습니다.\n\n"
                    f"현재: {db_path}\n\n"
                    f"경로를 job 이름에 맞게 자동 수정할까요?"):
                work_dir = Path(self._work_dir.get())
                tgt_type = new_cfg.get("target", {}).get("type", "duckdb")
                from engine.path_utils import get_job_defaults
                defaults = get_job_defaults(work_dir, stem, tgt_type)
                new_cfg["target"]["db_path"] = defaults["target_db_path"]
                self._target_db_path.set(defaults["target_db_path"])
                self._log_sys(f"[Save] db_path auto-fixed → {defaults['target_db_path']}")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                      sort_keys=False),
            encoding="utf-8"
        )
        self._jobs[fname] = new_cfg
        self._log_sys(f"Saved: {out_path.name}")
        self._job_loaded_snapshot = self._snapshot()
        self._dirty_cached = False
        self._update_title_dirty()

    def _show_save_confirm(self: "BatchRunnerGUI", fname, changed_fields) -> bool:
        """변경 내역을 보여주는 저장 확인 팝업"""
        def build(body):
            tk.Label(body, text=f"Save: {fname}", bg=C["base"],
                     fg=C["text"], font=FONTS["h2"]).pack(pady=(0, 8))
            if changed_fields:
                tk.Label(body, text="Changed fields:", bg=C["base"],
                         fg=C["subtext"], font=FONTS["body"]).pack(anchor="w", padx=16)
                for f in changed_fields[:10]:
                    tk.Label(body, text=f"  • {f}", bg=C["base"],
                             fg=C["yellow"], font=FONTS["mono_small"]).pack(anchor="w", padx=20)
                if len(changed_fields) > 10:
                    tk.Label(body, text=f"  ... and {len(changed_fields) - 10} more",
                             bg=C["base"], fg=C["subtext"],
                             font=FONTS["small"]).pack(anchor="w", padx=20)
        return self._themed_confirm("━ Save Confirm", build,
                                    ok_text="Save", ok_color="green", ok_active="teal")

    def _on_save_yml_as(self: "BatchRunnerGUI"):
        """새 이름으로 저장 (다이얼로그)"""
        fname = self.job_var.get()
        suggest = fname.replace(".yml", "") if fname else "new_job"
        self._save_yml_dialog(suggest, "Save as yml")

    def _on_new_job(self: "BatchRunnerGUI"):
        """새 Job 생성: 이름 입력 → _default.yml 기반 yml 생성 + job-centric 디렉토리 자동 구성"""
        from gui.constants import C, FONTS

        dlg = tk.Toplevel(self)
        dlg.title("New Job")
        dlg.configure(bg=C["base"])
        dlg.geometry("380x130")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)
        tk.Label(dlg, text="Job name", font=FONTS["mono"],
                 bg=C["base"], fg=C["text"]).pack(pady=(18, 6))
        name_var = tk.StringVar()
        entry = tk.Entry(dlg, textvariable=name_var, font=FONTS["mono"],
                         bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                         relief="flat", width=30)
        entry.pack()
        entry.focus_set()

        def do_create(*_):
            raw = name_var.get().strip()
            if not raw:
                return
            # .yml 확장자 제거하여 순수 이름 추출
            job_name = raw.replace(".yml", "").replace(".yaml", "")
            if not job_name or job_name in ("default", "_default"):
                messagebox.showwarning("Invalid", "사용할 수 없는 이름입니다.", parent=dlg)
                return
            fname = f"{job_name}.yml"
            jobs_dir = self._jobs_dir()

            # 이미 존재하면 경고
            jc_path = jobs_dir / job_name / fname
            flat_path = jobs_dir / fname
            if jc_path.exists() or flat_path.exists():
                messagebox.showwarning("Exists", f"'{fname}'이 이미 존재합니다.", parent=dlg)
                return

            # _default.yml 기반으로 새 config 생성
            default_cfg = self._jobs.get("_default.yml") or {}
            new_cfg = dict(default_cfg)
            new_cfg["job_name"] = job_name

            # job-centric 경로로 하드코딩 제거 (defaults에 위임)
            for section in ("export", "transform", "target"):
                sec = new_cfg.get(section, {})
                for k in ("sql_dir", "out_dir", "db_path", "params"):
                    sec.pop(k, None)
            new_cfg.pop("params", None)
            rep = new_cfg.get("report", {})
            for sub_key in ("export_csv", "excel"):
                sub = rep.get(sub_key, {})
                for k in ("sql_dir", "out_dir"):
                    sub.pop(k, None)
            rep.pop("csv_union_dir", None)

            # job-centric 디렉토리 생성
            wd = Path(self._work_dir.get())
            tgt_type = new_cfg.get("target", {}).get("type", "duckdb")
            db_ext = "duckdb" if tgt_type == "duckdb" else "sqlite"
            base = f"jobs/{job_name}"
            dir_list = [
                f"{base}/sql/export",
                f"{base}/sql/transform",
                f"{base}/sql/report",
                f"{base}/data/export",
                f"{base}/data/report",
                f"{base}/data/transform",
                f"{base}/data/report_tracking",
            ]
            # db 파일 부모 디렉토리
            db_parent = str(Path(f"{base}/data/{job_name}.{db_ext}").parent)
            if db_parent not in dir_list:
                dir_list.append(db_parent)

            for d in dir_list:
                (wd / d).mkdir(parents=True, exist_ok=True)

            # yml 저장 (job-centric 위치)
            jc_path.parent.mkdir(parents=True, exist_ok=True)
            jc_path.write_text(
                yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                          sort_keys=False),
                encoding="utf-8"
            )

            self._log_sys(f"[New Job] '{job_name}' 생성 완료 (job-centric)")
            dlg.destroy()

            # reload & 선택
            self._reload_project()
            self.job_var.set(fname)
            self._on_job_change()

        entry.bind("<Return>", do_create)
        tk.Button(dlg, text="Create", font=FONTS["body_bold"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=16, pady=4,
                  activebackground=C["teal"],
                  command=do_create).pack(pady=10)

    def _on_job_duplicate(self: "BatchRunnerGUI"):
        """현재 선택된 job을 복제하여 _copy.yml로 저장"""
        fname = self.job_var.get()
        if not fname:
            return
        jobs_dir = self._jobs_dir()
        jobs_dir.mkdir(parents=True, exist_ok=True)
        old_stem = Path(fname).stem
        # 중복 방지: _copy, _copy2, _copy3 ...
        new_name = f"{old_stem}_copy.yml"
        counter = 2
        while (jobs_dir / new_name).exists():
            new_name = f"{old_stem}_copy{counter}.yml"
            counter += 1
        new_stem = Path(new_name).stem
        new_cfg = self._build_gui_config()
        self._remap_job_paths(new_cfg, old_stem, new_stem)
        (jobs_dir / new_name).write_text(
            yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                      sort_keys=False),
            encoding="utf-8"
        )
        self._log_sys(f"Duplicated: {fname} → {new_name}")
        self._reload_project()
        self.job_var.set(new_name)
        self._on_job_change()

    def _on_job_delete(self: "BatchRunnerGUI"):
        """현재 선택된 job 파일 삭제"""
        fname = self.job_var.get()
        if not fname:
            return
        jobs_dir = self._jobs_dir()
        yml_path = jobs_dir / fname
        if not yml_path.exists():
            return
        if not messagebox.askyesno("Delete", f"Delete '{fname}'?\nThis will permanently delete the file."):
            return
        try:
            yml_path.unlink()
            self._log_sys(f"Deleted: {fname}")
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return
        self.job_var.set("")
        self._reload_project()

    def _save_yml_dialog(self: "BatchRunnerGUI", suggest: str, title: str = "Save as yml"):
        """jobs/<name>.yml 저장 공통 다이얼로그. 저장 후 reload + 선택."""
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.configure(bg=C["base"])
        dlg.geometry("380x130")
        dlg.transient(self)
        dlg.grab_set()
        dlg.resizable(False, False)
        tk.Label(dlg, text="Job filename (.yml)", font=FONTS["mono"],
                 bg=C["base"], fg=C["text"]).pack(pady=(18, 6))
        name_var = tk.StringVar(value=suggest)
        entry = tk.Entry(dlg, textvariable=name_var, font=FONTS["mono"],
                         bg=C["surface0"], fg=C["text"], insertbackground=C["text"],
                         relief="flat", width=30)
        entry.pack()
        entry.select_range(0, "end")
        entry.focus_set()

        def do_save(*_):
            raw = name_var.get().strip()
            if not raw:
                return
            if not raw.endswith(".yml"):
                raw += ".yml"
            jobs_dir = self._jobs_dir()
            jobs_dir.mkdir(parents=True, exist_ok=True)
            out_path = jobs_dir / raw
            if out_path.exists():
                if not messagebox.askyesno("Overwrite", f"{raw} already exists. Overwrite?",
                                           parent=dlg):
                    return
            # Save As: job_name을 새 파일명 기준으로 갱신
            prev_job = self.job_var.get()
            old_stem = Path(prev_job).stem if prev_job else ""
            new_stem = Path(raw).stem
            self.job_var.set(raw)
            new_cfg = self._build_gui_config()
            self._remap_job_paths(new_cfg, old_stem, new_stem)
            self.job_var.set(prev_job)  # combo 표시 복원 (reload에서 재설정)
            out_path.write_text(
                yaml.dump(new_cfg, allow_unicode=True, default_flow_style=False,
                          sort_keys=False),
                encoding="utf-8"
            )
            self._log_sys(f"Saved: {out_path.name}")
            self._reload_project()
            self.job_var.set(raw)
            self._on_job_change()
            dlg.destroy()

        entry.bind("<Return>", do_save)
        tk.Button(dlg, text="💾  Save", font=FONTS["body_bold"],
                  bg=C["green"], fg=C["crust"], relief="flat", padx=16, pady=4,
                  activebackground=C["teal"],
                  command=do_save).pack(pady=10)

    # ── 프로젝트 로드 ────────────────────────────────────────
    def _reload_project(self: "BatchRunnerGUI"):
        wd = Path(self._work_dir.get())
        self._add_recent_dir(str(wd))
        clear_sql_tree_cache()
        self._jobs = load_jobs(wd)
        self._env_hosts = load_env_hosts(wd, self._env_path_var.get()
                                         if hasattr(self, "_env_path_var") else "config/env.yml")
        self._sync_combos()

        self._log_write(f"Project loaded: {wd}  (jobs={len(self._jobs)}, "
                        f"env hosts={sum(len(v) for v in self._env_hosts.values())})", "INFO")

    def _sync_combos(self: "BatchRunnerGUI"):
        """콤보박스 values 재설정 + 현재 job/source 반영 (파일 I/O 없음)"""
        job_names = list(self._jobs.keys())
        job_loaded = False
        if hasattr(self, "_job_combo"):
            self._job_combo["values"] = job_names
            # reload 시 미저장 경고 팝업 방지 — snapshot을 None으로 초기화
            saved_snapshot = self._job_loaded_snapshot
            self._job_loaded_snapshot = None
            if self.job_var.get() in job_names:
                self._on_job_change()
                job_loaded = True
            elif not self.job_var.get() and "_default.yml" in job_names:
                self.job_var.set("_default.yml")
                self._on_job_change()
                job_loaded = True
            if not job_loaded:
                self._job_loaded_snapshot = saved_snapshot

        # source type combo 갱신
        if hasattr(self, "_source_type_combo"):
            src_types = list(self._env_hosts.keys())
            if src_types:
                self._source_type_combo["values"] = src_types
                if self._source_type_var.get() not in src_types:
                    self._source_type_var.set(src_types[0])
                # job 로드 시 _on_job_change()가 이미 host를 설정했으므로 중복 호출 방지
                if not job_loaded:
                    self._on_source_type_change()

    def _browse_workdir(self: "BatchRunnerGUI"):
        from tkinter import filedialog
        d = filedialog.askdirectory(initialdir=self._work_dir.get())
        if d:
            self._work_dir.set(d)
            self._add_recent_dir(d)
            self._reload_project()

    def _add_recent_dir(self: "BatchRunnerGUI", dir_path):
        """최근 디렉토리 히스토리에 추가 (최대 10개)"""
        d = str(dir_path)
        if d in self._recent_dirs:
            self._recent_dirs.remove(d)
        self._recent_dirs.insert(0, d)
        self._recent_dirs = self._recent_dirs[:10]
        if hasattr(self, "_wd_entry"):
            self._wd_entry["values"] = self._recent_dirs

    def _jobs_dir(self: "BatchRunnerGUI") -> Path:
        return Path(self._work_dir.get()) / "jobs"

    # ── Source/Target 변경 핸들러 ──────────────────────────────
    def _on_source_type_change(self: "BatchRunnerGUI", *_):
        src_type = self._source_type_var.get()
        hosts = self._env_hosts.get(src_type, [])
        if hasattr(self, "_host_combo"):
            self._host_combo["values"] = hosts
        # 복원 중에는 host를 강제 리셋하지 않음 (호출자가 직접 설정)
        if not getattr(self, "_restoring_job", False):
            # Type 변경 시 항상 첫 번째 host로 초기화
            self._source_host_var.set(hosts[0] if hosts else "")
        self._refresh_preview()

    def _on_target_type_change(self: "BatchRunnerGUI", *_):
        tgt = self._target_type_var.get()
        # job convention 폴더가 있으면 transform sql_dir은 convention 우선
        import re
        job_name = (self._jobs.get(self.job_var.get()) or {}).get(
            "job_name", Path(self.job_var.get()).stem if self.job_var.get() else "")
        work_dir = Path(self._work_dir.get())
        defaults = get_job_defaults(work_dir, job_name, tgt)
        tfm_type = self._transform_target_type.get()
        effective_type = tgt if (not tfm_type or tfm_type == "(global)") else tfm_type
        cur = self._transform_sql_dir.get()
        # 자동 sql_dir 갱신: 비어있거나, 이전 자동 설정값(sql/transform/{db_type})일 때만
        # 사용자가 직접 커스텀 경로를 입력한 경우에는 덮어쓰지 않음
        auto_patterns = {f"sql/transform/{t}" for t in ("duckdb", "sqlite3", "oracle", "vertica", "mysql", "postgresql")}
        auto_patterns.add("sql/transform")
        is_auto_value = not cur or cur in auto_patterns
        if defaults["job_dir_exists"]:
            job_convention_patterns = {defaults["transform_sql_dir"]}
            # job-centric convention 패턴도 자동값으로 인정
            if is_auto_value or cur in job_convention_patterns or re.match(r"^jobs/\w+/sql/transform$", cur or ""):
                self._transform_sql_dir.set(defaults["transform_sql_dir"])
        else:
            if is_auto_value:
                self._transform_sql_dir.set(f"sql/transform/{effective_type}")
        self._update_target_visibility()
        self._update_transform_target_visibility()
        self._update_transfer_src_label()
        self._update_load_mode_options()
        self._refresh_preview()

    def _update_target_visibility(self: "BatchRunnerGUI"):
        tgt = self._target_type_var.get()
        if not hasattr(self, "_db_path_row"):
            return
        # forget all dynamic rows to ensure correct pack order
        self._db_path_row.pack_forget()
        if hasattr(self, "_csv_dir_row"):
            self._csv_dir_row.pack_forget()
        self._schema_row.pack_forget()

        if tgt in ("duckdb", "sqlite3"):
            self._db_path_row.pack(fill="x", padx=12, pady=2)
        # csv_dir — export 스테이지 OFF 시만 표시 (load 단독 모드용)
        if hasattr(self, "_csv_dir_row") and not self._stage_export.get():
            self._csv_dir_row.pack(fill="x", padx=12, pady=2)
        # Schema — 모든 target type에서 표시 (DuckDB: main 외 스키마, Oracle: 대상 스키마)
        self._schema_row.pack(fill="x", padx=12, pady=2)

    def _update_transform_target_visibility(self: "BatchRunnerGUI"):
        """Transform 섹션의 target 선택 UI 가시성 관리.
        Load OFF 시 표시, (global) 이외의 duckdb/sqlite3 선택 시 DB Path 행 표시."""
        if not hasattr(self, "_transform_target_frame"):
            return
        self._transform_target_frame.pack_forget()
        if not self._stage_load_local.get():
            self._transform_target_frame.pack(fill="x", before=self._transform_sql_dir_row)
            # db_path 행: transform 전용 type이 duckdb/sqlite3일 때만 표시
            tfm_type = self._transform_target_type.get()
            if tfm_type in ("duckdb", "sqlite3"):
                self._tfm_db_row.pack(fill="x", padx=12, pady=2)
            else:
                self._tfm_db_row.pack_forget()

    def _update_transfer_visibility(self: "BatchRunnerGUI"):
        """Transfer 옵션의 가시성 관리.
        - Transfer 프레임은 항상 표시 (체크박스 포함)
        - 체크 ON → Source/Dest 행 표시
        - Source 라벨: load ON → 글로벌 target 자동 표시, load OFF → transform target 표시
        """
        if not hasattr(self, "_transfer_frame"):
            return
        self._dirty_cached = None  # transfer 변경 → dirty 캐시 무효화

        # transfer 프레임 자체는 transform 섹션에 항상 표시
        self._transfer_frame.pack(fill="x", before=self._transform_sql_dir_row)

        enabled = self._transfer_enabled.get()
        if enabled:
            self._transfer_src_row.pack(fill="x", padx=12, pady=2)
            self._transfer_dest_row.pack(fill="x", padx=12, pady=2)
            self._transfer_dest_db_row.pack(fill="x", padx=12, pady=2)
            self._transfer_sep.pack(fill="x", padx=12, pady=4)
            self._update_transfer_src_label()
        else:
            self._transfer_src_row.pack_forget()
            self._transfer_dest_row.pack_forget()
            self._transfer_dest_db_row.pack_forget()
            self._transfer_sep.pack_forget()

    def _update_transfer_src_label(self: "BatchRunnerGUI"):
        """Transfer Source DB 라벨을 현재 상태에 맞게 갱신."""
        if not hasattr(self, "_transfer_src_label"):
            return
        if self._stage_load_local.get():
            # Load ON → 글로벌 target 승계
            tgt_type = self._target_type_var.get()
            db_path = self._target_db_path.get()
            if tgt_type in ("duckdb", "sqlite3") and db_path:
                text = f"{tgt_type} ({db_path})"
            else:
                text = f"{tgt_type}" if tgt_type else "(global target)"
            self._transfer_src_label.config(text=f"← global target: {text}")
        else:
            # Load OFF → transform 전용 target
            tfm_type = self._transform_target_type.get()
            if tfm_type and tfm_type != "(global)":
                db_path = self._transform_db_path.get()
                if tfm_type in ("duckdb", "sqlite3") and db_path:
                    text = f"{tfm_type} ({db_path})"
                else:
                    text = tfm_type
                self._transfer_src_label.config(text=f"← transform target: {text}")
            else:
                tgt_type = self._target_type_var.get()
                db_path = self._target_db_path.get()
                if tgt_type in ("duckdb", "sqlite3") and db_path:
                    text = f"{tgt_type} ({db_path})"
                else:
                    text = f"{tgt_type}" if tgt_type else "(global target)"
                self._transfer_src_label.config(text=f"← global target: {text}")

    def _update_load_mode_options(self: "BatchRunnerGUI"):
        """target type에 따라 load.mode 선택지 자동 전환"""
        if not hasattr(self, "_load_mode_combo"):
            return
        tgt = self._target_type_var.get()
        if tgt == "oracle":
            self._load_mode_combo["values"] = ["replace", "truncate", "delete", "append"]
            if self._ov_load_mode.get() not in ("replace", "truncate", "delete", "append"):
                self._ov_load_mode.set("delete")
        else:
            self._load_mode_combo["values"] = ["replace", "truncate", "append"]
            if self._ov_load_mode.get() not in ("replace", "truncate", "append"):
                self._ov_load_mode.set("replace")

    def _on_export_sql_dir_change(self: "BatchRunnerGUI"):
        if self._restoring_job:
            return
        sql_dir = self._export_sql_dir.get()
        if sql_dir:
            suggested = sql_dir.replace("sql/", "data/", 1)
            if suggested != sql_dir:
                self._export_out_dir.set(suggested)
        self._scan_and_suggest_params()

    def _on_export_out_dir_change(self: "BatchRunnerGUI"):
        """export out_dir 변경 시 report 경로 자동 제안"""
        if self._restoring_job:
            return
        out = self._export_out_dir.get()
        if not out:
            return
        # report_out: data/export/… → data/report/…
        if "data/export" in out:
            self._report_out_dir.set(out.replace("data/export", "data/report", 1))
        # csv_source: export out_dir 그대로 (CSV가 생성되는 경로)
        self._ov_union_dir.set(out)

    # ── SQL 파라미터 자동 감지 ─────────────────────────────────
    def _scan_and_suggest_params(self: "BatchRunnerGUI"):
        """
        export / transform / report SQL 디렉토리를 스테이지별로 스캔해서
        (참고: _restoring_job 중이면 스킵 — 테마 전환 시 불필요한 재스캔 방지)
        발견된 파라미터를 Params 섹션에 그룹별 자동 제시.
        이미 사용자가 입력한 값은 항상 유지.
        """
        if self._restoring_job:
            return
        wd = Path(self._work_dir.get())

        def _resolve(rel):
            if not rel:
                return None
            p = Path(rel) if Path(rel).is_absolute() else wd / rel
            return p if p.exists() else None

        # ── 스테이지별 SQL 파일 수집 + 파라미터 감지 (활성 스테이지만) ──
        stage_params: dict[str, set[str]] = {}

        stage_cfg = [
            ("export",    self._export_sql_dir,    self._selected_sqls,
             self._stage_export),
            ("transform", self._transform_sql_dir, getattr(self, "_selected_transform_sqls", set()),
             self._stage_transform),
            ("report",    self._report_sql_dir,    getattr(self, "_selected_report_sqls", set()),
             self._stage_report),
        ]
        for stage_key, dir_var, selected, stage_toggle in stage_cfg:
            if not stage_toggle.get():
                continue
            sql_dir = _resolve(dir_var.get().strip())
            if not sql_dir:
                continue
            if selected:
                files = [sql_dir / p for p in selected if (sql_dir / p).exists()]
            else:
                # collect_sql_tree 캐시 활용 (rglob 대체)
                tree = _collect_sql_tree(sql_dir)
                files = _flatten_sql_tree(sql_dir, tree)
            if files:
                stage_params[stage_key] = set(_scan_params_from_files(files))

        all_detected = set()
        for s in stage_params.values():
            all_detected |= s

        # 캐시: _check_missing_params() 에서 재사용
        self._cached_detected_params = all_detected

        if not all_detected:
            return

        # 현재 params (사용자 입력값) — 스테이지별로 수집
        current_by_stage: dict[str, dict[str, str]] = {}
        for stage, entries in self._stage_param_entries.items():
            current_by_stage[stage] = {k.get(): v.get() for k, v in entries if k.get()}
        # flat fallback (yml 값 lookup용)
        current_flat: dict[str, str] = {}
        for d in current_by_stage.values():
            current_flat.update(d)

        # yml 기본값 (job 선택 시 참조) — per-stage + top-level 병합
        fname = self.job_var.get()
        yml_cfg = self._jobs.get(fname, {}) if fname else {}
        yml_params = dict(yml_cfg.get("params", {}))
        for s in ("export", "transform", "report"):
            yml_params.update(yml_cfg.get(s, {}).get("params", {}))

        def _val(p, stage=None):
            # 해당 스테이지에 이미 입력된 값 우선
            if stage and p in current_by_stage.get(stage, {}):
                return current_by_stage[stage][p]
            if p in current_flat:
                return current_flat[p]
            if p in yml_params:
                return str(yml_params[p])
            return ""

        # 스테이지별 (key, value) 리스트 구성 — 중복 제거 없이 각 스테이지 독립
        grouped: list[tuple[str, list[tuple[str, str]]]] = []
        for stage in ("export", "transform", "report"):
            detected = stage_params.get(stage, set())
            pairs = [(p, _val(p, stage)) for p in sorted(detected)]
            # 사용자가 값을 입력한 파라미터만 보존 (빈값 skip 상태는 제거)
            user_cur = current_by_stage.get(stage, {})
            for k, v in user_cur.items():
                if k and k not in detected and v.strip():
                    pairs.append((k, v))
            if pairs:
                grouped.append((stage, pairs))

        self._refresh_param_rows_grouped(grouped)
        if all_detected != getattr(self, "_last_detected_params", set()):
            self._last_detected_params = set(all_detected)
            self._log_write(f"SQL params detected: {', '.join(sorted(all_detected))}", "INFO")

    # ── 파라미터 히스토리 ─────────────────────────────────────
    _PARAM_HISTORY_MAX = 20

    def _param_history_path(self: "BatchRunnerGUI") -> Path:
        fname = self.job_var.get()
        stem = Path(fname).stem if fname else "_default"
        wd = Path(self._work_dir.get())
        job_dir = wd / "jobs" / stem
        if not job_dir.is_dir():
            job_dir = wd / "jobs"
        return job_dir / "param_history.json"

    def _save_param_history(self: "BatchRunnerGUI"):
        """현재 파라미터 조합을 히스토리에 저장 (Run 시 호출)."""
        params: dict[str, dict[str, str]] = {}
        for stage, entries in self._stage_param_entries.items():
            stage_p = {k.get().strip(): v.get().strip()
                       for k, v in entries if k.get().strip() and v.get().strip()}
            if stage_p:
                params[stage] = stage_p
        if not params:
            return

        from datetime import datetime
        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": params,
        }

        path = self._param_history_path()
        history = []
        if path.exists():
            try:
                history = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass

        # 동일 파라미터 조합 중복 제거
        history = [h for h in history if h.get("params") != params]
        history.insert(0, entry)
        history = history[:self._PARAM_HISTORY_MAX]

        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(history, ensure_ascii=False, indent=2),
                        encoding="utf-8")

    def _show_param_history(self: "BatchRunnerGUI"):
        """파라미터 히스토리 팝업."""
        path = self._param_history_path()
        if not path.exists():
            messagebox.showinfo("Params", "저장된 파라미터 히스토리가 없습니다.")
            return
        try:
            history = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            messagebox.showinfo("Params", "히스토리 파일을 읽을 수 없습니다.")
            return
        if not history:
            messagebox.showinfo("Params", "저장된 파라미터 히스토리가 없습니다.")
            return

        dlg = tk.Toplevel(self)
        dlg.title("Recent Params")
        dlg.configure(bg=C["base"])
        dlg.geometry("620x400")
        dlg.transient(self)
        dlg.grab_set()

        tk.Label(dlg, text=f"Recent Params — {self.job_var.get()}",
                 font=FONTS["h2"], bg=C["base"], fg=C["text"]).pack(pady=(12, 6))

        list_frame = tk.Frame(dlg, bg=C["base"])
        list_frame.pack(fill="both", expand=True, padx=12, pady=6)

        canvas = tk.Canvas(list_frame, bg=C["base"], highlightthickness=0)
        scrollbar = tk.Scrollbar(list_frame, orient="vertical", command=canvas.yview)
        inner = tk.Frame(canvas, bg=C["base"])
        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        def _apply(entry):
            params = entry.get("params", {})
            grouped = [(stage, list(p.items())) for stage, p in params.items() if p]
            if grouped:
                self._refresh_param_rows_grouped(grouped)
                self._log_sys(f"[Params] loaded: {entry.get('timestamp', '?')}")
            dlg.destroy()

        for i, entry in enumerate(history):
            ts = entry.get("timestamp", "?")
            params = entry.get("params", {})
            # 요약 텍스트: stage별 key=value
            parts = []
            for stage, p in params.items():
                for k, v in p.items():
                    display_v = v if len(v) <= 20 else v[:17] + "..."
                    parts.append(f"{k}={display_v}")
            summary = ", ".join(parts)
            if len(summary) > 80:
                summary = summary[:77] + "..."

            row = tk.Frame(inner, bg=C["surface0"], cursor="hand2")
            row.pack(fill="x", pady=1, padx=2)
            tk.Label(row, text=ts, font=FONTS["mono_small"],
                     bg=C["surface0"], fg=C["subtext"], width=19,
                     anchor="w").pack(side="left", padx=(8, 4))
            tk.Label(row, text=summary, font=FONTS["mono_small"],
                     bg=C["surface0"], fg=C["text"],
                     anchor="w").pack(side="left", fill="x", expand=True, padx=(0, 8))
            for widget in (row,):
                widget.bind("<Button-1>", lambda e, ent=entry: _apply(ent))
            for child in row.winfo_children():
                child.bind("<Button-1>", lambda e, ent=entry: _apply(ent))

        tk.Button(dlg, text="Close", font=FONTS["body"],
                  bg=C["surface0"], fg=C["text"], relief="flat",
                  command=dlg.destroy).pack(pady=(4, 12))

    # ── SQL 선택 (공통 헬퍼) ─────────────────────────────────
    _SQL_SELECTOR_CFG = {
        "export":    {"dir_var": "_export_sql_dir",    "default": "sql/export",
                      "selected": "_selected_sqls",
                      "label": "_sql_count_label",     "tip": "_sql_count_tip"},
        "transform": {"dir_var": "_transform_sql_dir", "default": "sql/transform",
                      "selected": "_selected_transform_sqls",
                      "label": "_transform_sql_count_label", "tip": "_transform_sql_count_tip"},
        "report":    {"dir_var": "_report_sql_dir",    "default": "sql/report",
                      "selected": "_selected_report_sqls",
                      "label": "_report_sql_count_label",    "tip": "_report_sql_count_tip"},
    }

    def _open_sql_selector_for(self: "BatchRunnerGUI", stage: str):
        """stage별 SQL 선택 다이얼로그 (공통)"""
        cfg = self._SQL_SELECTOR_CFG[stage]
        sql_dir_rel = getattr(self, cfg["dir_var"]).get() or cfg["default"]
        wd = Path(self._work_dir.get())
        sql_dir = wd / sql_dir_rel
        if not sql_dir.exists():
            messagebox.showwarning("SQL Filter",
                                   f"{stage}.sql_dir path not found:\n{sql_dir}",
                                   parent=self)
            return
        pre = set(getattr(self, cfg["selected"]))
        dlg = SqlSelectorDialog(self, sql_dir, pre_selected=pre)
        self.wait_window(dlg)
        setattr(self, cfg["selected"], set(dlg.selected))
        self._update_sql_preview_for(stage)
        self._scan_and_suggest_params()
        self._refresh_preview()

    def _update_sql_preview_for(self: "BatchRunnerGUI", stage: str):
        """stage별 SQL 선택 라벨 갱신 (공통)"""
        cfg = self._SQL_SELECTOR_CFG[stage]
        label_attr = cfg["label"]
        if not hasattr(self, label_attr):
            return
        selected = getattr(self, cfg["selected"])
        count = len(selected)
        if count == 0:
            getattr(self, label_attr).config(text="(all)", fg=C["subtext"])
            tip_text = "전체 SQL 실행 (필터 없음)"
        else:
            sorted_names = sorted(selected)
            # 라벨: 파일 수 + 첫 번째 파일명 힌트 (길면 잘라서 표시)
            first = sorted_names[0].replace(".sql", "")
            if len(first) > 18:
                first = first[:15] + "…"
            if count == 1:
                getattr(self, label_attr).config(text=f"(1: {first})", fg=C["green"])
            else:
                getattr(self, label_attr).config(
                    text=f"({count}: {first}+{count-1})", fg=C["green"])
            tip_text = f"선택된 SQL ({count}개):\n" + "\n".join(sorted_names)
        tip_attr = cfg["tip"]
        if hasattr(self, tip_attr):
            getattr(self, tip_attr)._text = tip_text

    # 기존 호출자 호환 래퍼
    def _open_sql_selector(self: "BatchRunnerGUI"):
        self._open_sql_selector_for("export")

    def _update_sql_preview(self: "BatchRunnerGUI"):
        self._update_sql_preview_for("export")

    def _open_transform_sql_selector(self: "BatchRunnerGUI"):
        self._open_sql_selector_for("transform")

    def _update_transform_sql_preview(self: "BatchRunnerGUI"):
        self._update_sql_preview_for("transform")

    def _open_report_sql_selector(self: "BatchRunnerGUI"):
        self._open_sql_selector_for("report")

    def _update_report_sql_preview(self: "BatchRunnerGUI"):
        self._update_sql_preview_for("report")

    # ── CSV File Selector (CSV→Excel 필터) ───────────────────

    def _open_csv_selector(self: "BatchRunnerGUI"):
        """CSV 파일 선택 다이얼로그 (Report CSV→Excel)"""
        from gui.widgets import CsvSelectorDialog
        wd = Path(self._work_dir.get())

        # CSV 소스 디렉토리 결정: union_dir 우선, 없으면 report_out_dir
        union_dir = self._ov_union_dir.get().strip()
        if union_dir:
            csv_dir = Path(union_dir) if Path(union_dir).is_absolute() else wd / union_dir
        else:
            out_dir = self._report_out_dir.get().strip()
            csv_dir = Path(out_dir) if Path(out_dir).is_absolute() else wd / out_dir

        pre = set()
        # csv_filter 텍스트에서 기존 선택 파싱 (파일명이면 그대로 사용)
        cur_filter = self._ov_csv_filter.get().strip()
        if cur_filter:
            # 파일명 목록인지 키워드인지 판별: .csv 포함 여부
            parts = [p.strip() for p in cur_filter.split(",") if p.strip()]
            if any(".csv" in p for p in parts):
                pre = set(parts)

        dlg = CsvSelectorDialog(self, csv_dir, pre_selected=pre)
        self.wait_window(dlg)

        if dlg.selected:
            self._ov_csv_filter.set(", ".join(sorted(dlg.selected)))
        elif not dlg.selected and hasattr(dlg, '_check_vars') and dlg._check_vars:
            # 전부 해제 = 필터 없음 (전체 포함)
            self._ov_csv_filter.set("")
        self._update_csv_filter_count()

    def _update_csv_filter_count(self: "BatchRunnerGUI"):
        """CSV 필터 선택 개수 표시"""
        if not hasattr(self, "_csv_filter_count"):
            return
        cur = self._ov_csv_filter.get().strip()
        if not cur:
            self._csv_filter_count.config(text="(all)", fg=C["subtext"])
        else:
            count = len([p for p in cur.split(",") if p.strip()])
            self._csv_filter_count.config(text=f"({count})", fg=C["green"])

    # ── Param 행 관리 ────────────────────────────────────────

    def _refresh_param_rows(self: "BatchRunnerGUI", pairs: list):
        """그룹 없이 flat 리스트로 표시 (스냅샷 복원 / job 선택 시 사용)"""
        for frame in self._stage_params_frames.values():
            for w in frame.winfo_children():
                w.destroy()
        self._stage_param_entries = {"export": [], "transform": [], "report": []}
        for k, v in pairs:
            self._add_param_row(k, str(v), stage="export")

    def _refresh_param_rows_grouped(self: "BatchRunnerGUI", grouped: list):
        """스테이지별로 해당 섹션의 params 프레임에 분배"""
        for frame in self._stage_params_frames.values():
            for w in frame.winfo_children():
                w.destroy()
        self._stage_param_entries = {"export": [], "transform": [], "report": []}

        for stage, pairs in grouped:
            target = stage if stage in self._stage_params_frames else "export"
            for k, v in pairs:
                self._add_param_row(k, str(v), stage=target)

    def _refresh_param_rows_from_stages(self: "BatchRunnerGUI", stage_data: dict):
        """per-stage params dict에서 복원 (스냅샷용)"""
        for frame in self._stage_params_frames.values():
            for w in frame.winfo_children():
                w.destroy()
        self._stage_param_entries = {"export": [], "transform": [], "report": []}
        for stage, pairs in stage_data.items():
            target = stage if stage in self._stage_params_frames else "export"
            for k, v in pairs:
                self._add_param_row(k, str(v), stage=target)

    def _add_param_row(self: "BatchRunnerGUI", key="", value="", stage="export"):
        frame = self._stage_params_frames.get(stage, self._export_params_frame)
        k_var = tk.StringVar(value=key)
        v_var = tk.StringVar(value=value)
        if stage not in self._stage_param_entries:
            self._stage_param_entries[stage] = []
        self._stage_param_entries[stage].append((k_var, v_var))

        row = tk.Frame(frame, bg=C["mantle"])
        row.pack(fill="x", pady=1)

        v_entry = tk.Entry(row, textvariable=v_var, bg=C["surface0"], fg=C["text"],
                           insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                           width=8)
        skip_lbl = tk.Label(row, text="", bg=C["mantle"], fg=C["yellow"],
                            font=FONTS["shortcut"], width=4)

        def _update_skip_hint(*_):
            has_key = bool(k_var.get().strip())
            empty_val = not v_var.get().strip()
            if has_key and empty_val:
                skip_lbl.config(text="skip")
                v_entry.config(fg=C["overlay0"])
            else:
                skip_lbl.config(text="")
                v_entry.config(fg=C["text"])
            self._refresh_preview()

        k_var.trace_add("write", _update_skip_hint)
        v_var.trace_add("write", _update_skip_hint)

        tk.Entry(row, textvariable=k_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                 width=15).pack(side="left", padx=(0, 2), ipady=2)
        tk.Label(row, text="=", bg=C["mantle"], fg=C["subtext"],
                 font=FONTS["mono"]).pack(side="left")
        v_entry.pack(side="left", padx=(2, 0), fill="x", expand=True, ipady=2)

        def remove(r=row, pair=(k_var, v_var), s=stage):
            r.destroy()
            if pair in self._stage_param_entries.get(s, []):
                self._stage_param_entries[s].remove(pair)
            self._refresh_preview()
        tk.Button(row, text="X", font=FONTS["shortcut"], bg=C["mantle"],
                  fg=C["subtext"], relief="flat", padx=4,
                  command=remove).pack(side="right")
        skip_lbl.pack(side="right")
        _update_skip_hint()

    # ── Stage 토글 버튼 ──────────────────────────────────────
    def _toggle_stage(self: "BatchRunnerGUI", stage_key):
        var = getattr(self, f"_stage_{stage_key}")
        var.set(not var.get())
        self._refresh_stage_buttons()
        # load 토글 시 transfer source label 갱신
        if stage_key == "load_local":
            self._update_transfer_src_label()
        self._refresh_preview()

    def _refresh_stage_buttons(self: "BatchRunnerGUI"):
        for stage_key, (btn, color_key) in self._stage_buttons.items():
            var = getattr(self, f"_stage_{stage_key}")
            if var.get():
                btn.config(bg=C[color_key], fg=C["crust"],
                           activebackground=C[color_key], activeforeground=C["crust"])
            else:
                btn.config(bg=C["surface0"], fg=C["subtext"],
                           activebackground=C["surface1"], activeforeground=C["subtext"])

    def _stages_all(self: "BatchRunnerGUI"):
        for s, _, _ in STAGE_CONFIG:
            getattr(self, f"_stage_{s}").set(True)
        self._refresh_stage_buttons()
        self._refresh_preview()

    def _stages_none(self: "BatchRunnerGUI"):
        for s, _, _ in STAGE_CONFIG:
            getattr(self, f"_stage_{s}").set(False)
        self._refresh_stage_buttons()
        self._refresh_preview()
