"""
gui/mixins/dialogs.py  ─  확인 다이얼로그, 테마 전환, 탐색기 열기, 로그 내보내기
"""

from __future__ import annotations

import os
import sys
import tkinter as tk
from tkinter import messagebox, filedialog
from pathlib import Path
from typing import TYPE_CHECKING

from gui.constants import C, FONTS, THEMES, APP_VERSION, STAGE_CONFIG

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI


class DialogsMixin:

    def _themed_confirm(self: "BatchRunnerGUI", title, body_builder, ok_text="OK",
                        ok_color="green", ok_active="teal") -> bool:
        """테마 통일된 확인 다이얼로그. body_builder(frame)로 본문 구성."""
        dlg = tk.Toplevel(self)
        dlg.title(title)
        dlg.resizable(False, False)
        dlg.configure(bg=C["base"])
        dlg.grab_set()
        dlg.transient(self)

        result = [False]

        body = tk.Frame(dlg, bg=C["base"])
        body.pack(padx=4, pady=(12, 6))
        body_builder(body)

        tk.Frame(dlg, bg=C["surface1"], height=1).pack(fill="x", padx=12, pady=(4, 8))

        btn_frame = tk.Frame(dlg, bg=C["base"])
        btn_frame.pack(pady=(0, 12))

        def on_ok():
            result[0] = True
            dlg.destroy()

        def on_cancel():
            dlg.destroy()

        ok_btn = tk.Button(btn_frame, text=ok_text, width=10, command=on_ok,
                           bg=C[ok_color], fg=C["crust"], font=FONTS["body_bold"],
                           activebackground=C[ok_active], cursor="hand2")
        ok_btn.pack(side="left", padx=8)
        tk.Button(btn_frame, text="Cancel", width=10, command=on_cancel,
                  bg=C["surface0"], fg=C["text"], font=FONTS["body"],
                  activebackground=C["surface1"], cursor="hand2").pack(side="left", padx=8)

        dlg.update_idletasks()
        x = self.winfo_x() + (self.winfo_width() - dlg.winfo_width()) // 2
        y = self.winfo_y() + (self.winfo_height() - dlg.winfo_height()) // 2
        dlg.geometry(f"+{x}+{y}")

        dlg.protocol("WM_DELETE_WINDOW", on_cancel)
        dlg.bind("<Return>", lambda e: on_ok())
        dlg.bind("<Escape>", lambda e: on_cancel())
        ok_btn.focus_set()

        dlg.wait_window()
        return result[0]

    def _show_overwrite_confirm(self: "BatchRunnerGUI") -> bool:
        kw_key = {"bg": C["base"], "fg": C["subtext"], "font": FONTS["body"]}

        def build(body):
            tk.Label(body, text="⚠  export.overwrite = ON", bg=C["base"],
                     fg=C["red"], font=FONTS["h2"]).pack(pady=(0, 8))
            tk.Label(body, text="Existing data may be overwritten.\nDo you want to continue?",
                     **kw_key, justify="center").pack()

        return self._themed_confirm("━ Overwrite Confirm", build,
                                    ok_text="Continue", ok_color="red", ok_active="peach")

    def _show_run_confirm(self: "BatchRunnerGUI") -> bool:
        mode = self.mode_var.get()
        mode_label = {"run": "Run", "retry": "Retry"}.get(mode, mode)
        selected_stages = [s for s in ("export", "load_local", "transform", "report")
                           if getattr(self, f"_stage_{s}").get()]
        stages_str = " → ".join(selected_stages) if selected_stages else "(all)"
        params = {}
        for stage in ("export", "transform", "report"):
            if getattr(self, f"_stage_{stage}").get():
                for k_var, v_var in self._stage_param_entries.get(stage, []):
                    k, v = k_var.get().strip(), v_var.get().strip()
                    if k and v:
                        params[k] = v
        params_str = ", ".join(f"{k}={v}" for k, v in params.items())
        timeout_val = self._ov_timeout.get().strip() or "1800"
        ov_on = self._ov_overwrite.get()

        kw_key = {"bg": C["base"], "fg": C["subtext"], "font": FONTS["body"]}
        kw_val = {"bg": C["base"], "fg": C["text"], "font": FONTS["body_bold"]}
        pad_k = {"padx": (16, 4), "pady": 3}
        pad_v = {"padx": (0, 16), "pady": 3}

        has_export = self._stage_export.get()
        has_load   = self._stage_load_local.get()
        work_dir   = self._work_dir.get()

        def build(body):
            row = 0
            tk.Label(body, text="Mode", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            mode_fg = C["blue"] if mode == "run" else C["peach"]
            tk.Label(body, text=mode_label, bg=C["base"], fg=mode_fg,
                     font=FONTS["body_bold"]).grid(row=row, column=1, sticky="w", **pad_v)
            row += 1

            if has_export:
                tk.Label(body, text="Source", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
                src_frame = tk.Frame(body, bg=C["base"])
                src_frame.grid(row=row, column=1, sticky="w", **pad_v)
                tk.Label(src_frame, text=self._source_type_var.get(), **kw_val).pack(side="left")
                src_host = self._source_host_var.get()
                if src_host:
                    tk.Label(src_frame, text=f"  [{src_host}]", bg=C["base"],
                             fg=C["peach"], font=FONTS["body_bold"]).pack(side="left")
                tk.Label(body, text="Overwrite", **kw_key).grid(row=row, column=2, sticky="e", **pad_k)
                tk.Label(body, text="ON" if ov_on else "OFF", bg=C["base"],
                         fg=C["red"] if ov_on else C["subtext"],
                         font=FONTS["body_bold"]).grid(row=row, column=3, sticky="w", **pad_v)
                row += 1

            if has_load:
                tk.Label(body, text="Target", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
                tk.Label(body, text=self._target_type_var.get(), **kw_val).grid(
                    row=row, column=1, sticky="w", **pad_v)
                tk.Label(body, text="Timeout", **kw_key).grid(row=row, column=2, sticky="e", **pad_k)
                tk.Label(body, text=timeout_val, **kw_val).grid(row=row, column=3, sticky="w", **pad_v)
                row += 1

            tk.Label(body, text="Stages", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=stages_str, **kw_val).grid(
                row=row, column=1, columnspan=3, sticky="w", **pad_v)
            row += 1

            tk.Label(body, text="Work Dir", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=work_dir, **kw_val).grid(
                row=row, column=1, columnspan=3, sticky="w", **pad_v)
            row += 1

            if params_str:
                tk.Label(body, text="Params", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
                tk.Label(body, text=params_str, **kw_val).grid(
                    row=row, column=1, columnspan=3, sticky="w", **pad_v)
                row += 1

            # Retry 모드: 이전 실행 실패 task 수 표시
            if mode == "retry":
                retry_info = self._count_retry_targets()
                if retry_info:
                    tk.Label(body, text="Retry", **kw_key).grid(
                        row=row, column=0, sticky="e", **pad_k)
                    tk.Label(body, text=retry_info, bg=C["base"],
                             fg=C["peach"], font=FONTS["body_bold"]).grid(
                        row=row, column=1, columnspan=3, sticky="w", **pad_v)
                    row += 1

            # Pre-flight: SQL 파라미터 누락 경고
            missing = self._check_missing_params()
            if missing:
                tk.Label(body, text="Warning", **kw_key).grid(
                    row=row, column=0, sticky="e", **pad_k)
                tk.Label(body, text=f"Param 누락: {', '.join(sorted(missing))}",
                         bg=C["base"], fg=C["red"],
                         font=FONTS["body_bold"]).grid(
                    row=row, column=1, columnspan=3, sticky="w", **pad_v)

        return self._themed_confirm("━ Run Confirm", build)

    def _check_missing_params(self: "BatchRunnerGUI") -> set[str]:
        """SQL에서 필요한 파라미터 vs GUI 입력 파라미터 비교 → 누락 목록 반환."""
        from gui.utils import scan_sql_params
        wd = Path(self._work_dir.get())

        # SQL 디렉토리에서 필요한 파라미터 수집
        required: set[str] = set()
        dir_vars = {
            "export": self._export_sql_dir,
            "transform": self._transform_sql_dir,
            "report": self._report_sql_dir,
        }
        for stage_key, dir_var in dir_vars.items():
            if not getattr(self, f"_stage_{stage_key}").get():
                continue
            sql_dir = dir_var.get().strip()
            if not sql_dir:
                continue
            p = Path(sql_dir)
            if not p.is_absolute():
                p = wd / p
            if p.is_dir():
                # scan_sql_params는 하위 폴더 포함하므로, 해당 디렉토리만 직접 스캔
                from gui.utils import _extract_params
                for sql_file in p.rglob("*.sql"):
                    try:
                        text = sql_file.read_text(encoding="utf-8", errors="ignore")
                        required |= _extract_params(text)
                    except Exception:
                        pass

        if not required:
            return set()

        # GUI에서 입력된 파라미터 키 수집
        provided: set[str] = set()
        for stage in ("export", "transform", "report"):
            for k_var, v_var in self._stage_param_entries.get(stage, []):
                k = k_var.get().strip()
                if k:
                    provided.add(k)

        return required - provided

    def _count_retry_targets(self: "BatchRunnerGUI") -> str:
        """이전 실행의 failed/pending task 수를 stage별로 집계하여 문자열 반환."""
        import json
        wd = Path(self._work_dir.get())
        job_name = (self.job_var.get() or "").replace(".yml", "").replace(".yaml", "")
        if not job_name:
            return ""
        parts = []
        for stage_key, stage_label, _ in [
            ("export", "Export", "blue"),
            ("load_local", "Load", "teal"),
            ("transform", "Transform", "mauve"),
            ("report", "Report", "peach"),
        ]:
            if not getattr(self, f"_stage_{stage_key}").get():
                continue
            # data/{stage}/runs/{job_name} 하위에서 가장 최근 run_info.json 탐색
            stage_dir_name = {"export": "export", "load_local": "load",
                              "transform": "transform", "report": "report"}.get(stage_key, stage_key)
            base = wd / "data" / stage_dir_name / "runs"
            job_dir = base / job_name
            if not job_dir.exists():
                continue
            # 가장 최근 run_info.json
            for d in sorted(job_dir.iterdir(), reverse=True):
                run_info = d / "run_info.json"
                if not run_info.exists():
                    continue
                try:
                    with open(run_info, encoding="utf-8") as f:
                        info = json.load(f)
                    tasks = info.get("tasks", {})
                    failed = sum(1 for v in tasks.values()
                                 if v.get("status") in ("failed", "pending"))
                    total = len(tasks)
                    if failed > 0:
                        parts.append(f"{stage_label}: {failed}/{total} retry")
                    break
                except Exception:
                    continue
        return " | ".join(parts) if parts else "이전 실패 기록 없음"

    def _apply_theme(self: "BatchRunnerGUI"):
        """테마 전환: C 딕셔너리 업데이트 후 앱 전체 재빌드"""
        theme_name = self._theme_var.get()
        C.update(THEMES.get(theme_name, THEMES["Mocha"]))
        # 현재 상태 스냅샷
        snap = self._snapshot()
        wd = self._work_dir.get()
        # 모든 자식 위젯 제거
        for w in self.winfo_children():
            w.destroy()
        # ttk 스타일 재적용
        self._build_style()
        # UI 재빌드
        self._build_ui()
        # 상태 복원 — 파일 I/O·job 재파싱 없이 콤보 옵션만 복원
        self._restoring_job = True
        self._work_dir.set(wd)
        if hasattr(self, "_job_combo"):
            self._job_combo["values"] = list(self._jobs.keys())
        if hasattr(self, "_source_type_combo"):
            src_types = list(self._env_hosts.keys())
            if src_types:
                self._source_type_combo["values"] = src_types
        self._theme_var.set(theme_name)
        self._restore_snapshot(snap)
        self._restoring_job = False
        self._capture_loaded_snapshot()
        # 테마 전환 후 로그 내용 복원 (위젯 재생성으로 Text가 비어 있음)
        self._refilter_log()
        # S-4: 예약 실행 상태 복원 (위젯 재생성으로 초기화됨)
        self._restore_schedule_ui()

    def _open_job_file(self: "BatchRunnerGUI"):
        """현재 선택된 job yml 파일을 OS 기본 편집기로 연다"""
        job = self.job_var.get() if hasattr(self, "job_var") else ""
        if not job:
            messagebox.showinfo("Info", "No job selected.")
            return
        p = self._jobs_dir() / job
        if not p.exists():
            messagebox.showwarning("Not Found", f"File not found:\n{p}")
            return
        self._open_in_explorer(str(p))

    def _open_in_explorer(self: "BatchRunnerGUI", path_str):
        """OS 파일 탐색기에서 경로를 연다"""
        import subprocess as sp
        p = Path(path_str)
        if not p.is_absolute():
            p = Path(self._work_dir.get()) / p
        # 경로가 없으면 부모 폴백
        if not p.exists():
            p = p.parent
        if not p.exists():
            messagebox.showwarning("Path Not Found", f"Cannot find path:\n{path_str}")
            return
        target = str(p)
        try:
            if sys.platform == "win32":
                os.startfile(target)
            elif sys.platform == "darwin":
                sp.Popen(["open", target])
            else:
                sp.Popen(["xdg-open", target])
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def _export_log(self: "BatchRunnerGUI"):
        """로그 내용을 .txt 파일로 저장"""
        import datetime
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = (self.job_var.get().replace(".yml","") if hasattr(self, "job_var") and self.job_var.get() else "") or "elt_runner"
        init_file = f"{fname}_log_{ts}.txt"
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=init_file,
            title="Save Log"
        )
        if not path:
            return
        content = self._log.get("1.0", "end")
        Path(path).write_text(content, encoding="utf-8")
        self._log_sys(f"Log saved: {path}")
