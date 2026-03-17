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
from gui.utils import load_jobs, load_env_hosts, _scan_params_from_files
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
        self._transform_sql_dir.set(snap.get("transform_sql_dir", "sql/transform/duckdb"))
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

    def _update_title_dirty(self: "BatchRunnerGUI"):
        """타이틀 바에 변경 표시(*) 업데이트 (이전과 동일하면 스킵)"""
        base = f"ELT Runner  v{APP_VERSION}"
        fname = self.job_var.get()
        if fname:
            base = f"{fname} - {base}"
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
                    if k.get().strip() and v.get().strip()}}
                   if any(k.get().strip() for k, _ in self._stage_param_entries.get("export", [])) else {}),
                "param_mode": self._param_mode_var.get(),
            },
            "load": {
                "mode": self._ov_load_mode.get(),
                **({"csv_dir": self._load_csv_dir.get().strip()}
                   if self._load_csv_dir.get().strip() else {}),
            },
            "target": {
                "type": self._target_type_var.get(),
            },
            "transform": {
                "sql_dir": self._transform_sql_dir.get(),
                "on_error": self._ov_on_error.get(),
                **({"schema": self._transform_schema.get().strip()}
                   if self._transform_schema.get().strip() else {}),
                **({"params": {k.get().strip(): v.get().strip()
                    for k, v in self._stage_param_entries.get("transform", [])
                    if k.get().strip() and v.get().strip()}}
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
                    if k.get().strip() and v.get().strip()}}
                   if any(k.get().strip() for k, _ in self._stage_param_entries.get("report", [])) else {}),
                "param_mode": self._report_param_mode_var.get(),
            },
        }
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
        if getattr(self, "_schedule_id", None):
            self.after_cancel(self._schedule_id)
            self._schedule_id = None
        if getattr(self, "_clock_timer_id", None):
            self.after_cancel(self._clock_timer_id)
            self._clock_timer_id = None
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

        # SQL 선택 초기화
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
        out_path = jobs_dir / fname
        new_cfg = self._build_gui_config()
        jobs_dir.mkdir(parents=True, exist_ok=True)
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

    def _on_job_duplicate(self: "BatchRunnerGUI"):
        """현재 선택된 job을 복제하여 _copy.yml로 저장"""
        fname = self.job_var.get()
        if not fname:
            return
        jobs_dir = self._jobs_dir()
        jobs_dir.mkdir(parents=True, exist_ok=True)
        base = fname.replace(".yml", "")
        # 중복 방지: _copy, _copy2, _copy3 ...
        new_name = f"{base}_copy.yml"
        counter = 2
        while (jobs_dir / new_name).exists():
            new_name = f"{base}_copy{counter}.yml"
            counter += 1
        new_cfg = self._build_gui_config()
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
            new_cfg = self._build_gui_config()
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
            # reload 시 미저장 경고 팝업 방지 — _restoring_job으로 감싸서 호출
            prev_restoring = self._restoring_job
            self._restoring_job = True
            if self.job_var.get() in job_names:
                self._on_job_change()
                job_loaded = True
            elif not self.job_var.get() and "_default.yml" in job_names:
                self.job_var.set("_default.yml")
                self._on_job_change()
                job_loaded = True
            self._restoring_job = prev_restoring

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
        if defaults["job_dir_exists"]:
            # job-centric: convention 폴더 → DB 엔진별 분리 불필요
            if not cur or re.match(r"^(sql/transform/\w+|jobs/\w+/sql/transform)$", cur):
                self._transform_sql_dir.set(defaults["transform_sql_dir"])
        else:
            if not cur or re.match(r"^sql/transform/\w+$", cur):
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
            params = stage_params.get(stage, set())
            if params:
                grouped.append((stage, [(p, _val(p, stage)) for p in sorted(params)]))

        # 사용자가 직접 추가한 값 (자동감지 아닌 것) 보존
        prev_detected = getattr(self, "_last_detected_params", set())
        custom_pairs = [(k, v) for k, v in current_flat.items()
                        if k not in all_detected and k not in prev_detected]
        if custom_pairs:
            grouped.append(("custom", custom_pairs))

        self._refresh_param_rows_grouped(grouped)
        if all_detected != getattr(self, "_last_detected_params", set()):
            self._last_detected_params = set(all_detected)
            self._log_write(f"SQL params detected: {', '.join(sorted(all_detected))}", "INFO")

    # ── SQL 선택 (공통 헬퍼) ─────────────────────────────────
    _SQL_SELECTOR_CFG = {
        "export":    {"dir_var": "_export_sql_dir",    "default": "sql/export",
                      "selected": "_selected_sqls",
                      "label": "_sql_count_label",     "tip": "_sql_count_tip"},
        "transform": {"dir_var": "_transform_sql_dir", "default": "sql/transform/duckdb",
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
            # 라벨: 파일 수 + 첫 번째 파일명 힌트
            first = sorted_names[0].replace(".sql", "")
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
        k_var.trace_add("write", lambda *_: self._refresh_preview())
        v_var.trace_add("write", lambda *_: self._refresh_preview())

        row = tk.Frame(frame, bg=C["mantle"])
        row.pack(fill="x", pady=1)

        tk.Entry(row, textvariable=k_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                 width=15).pack(side="left", padx=(0, 2), ipady=2)
        tk.Label(row, text="=", bg=C["mantle"], fg=C["subtext"],
                 font=FONTS["mono"]).pack(side="left")
        tk.Entry(row, textvariable=v_var, bg=C["surface0"], fg=C["text"],
                 insertbackground=C["text"], relief="flat", font=FONTS["mono"],
                 width=8).pack(side="left", padx=(2, 0), fill="x", expand=True, ipady=2)

        def remove(r=row, pair=(k_var, v_var), s=stage):
            r.destroy()
            if pair in self._stage_param_entries.get(s, []):
                self._stage_param_entries[s].remove(pair)
            self._refresh_preview()
        tk.Button(row, text="X", font=FONTS["shortcut"], bg=C["mantle"],
                  fg=C["subtext"], relief="flat", padx=4,
                  command=remove).pack(side="right")

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
