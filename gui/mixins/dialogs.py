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

from gui.constants import C, FONTS, THEMES, APP_VERSION

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
            tk.Label(body, text="기존 데이터를 덮어쓸 수 있습니다.\n계속하시겠습니까?",
                     **kw_key, justify="center").pack()

        return self._themed_confirm("━ Overwrite 확인", build,
                                    ok_text="Continue", ok_color="red", ok_active="peach")

    def _show_run_confirm(self: "BatchRunnerGUI") -> bool:
        mode = self.mode_var.get()
        mode_label = {"run": "Run", "retry": "Retry"}.get(mode, mode)
        selected_stages = [s for s in ("export", "load_local", "transform", "report")
                           if getattr(self, f"_stage_{s}").get()]
        stages_str = " → ".join(selected_stages) if selected_stages else "(all)"
        params = {k.get().strip(): v.get().strip()
                  for k, v in self._param_entries
                  if k.get().strip() and v.get().strip()}
        params_str = ", ".join(f"{k}={v}" for k, v in params.items())
        timeout_val = self._ov_timeout.get().strip() or "1800"
        ov_on = self._ov_overwrite.get()

        kw_key = {"bg": C["base"], "fg": C["subtext"], "font": FONTS["body"]}
        kw_val = {"bg": C["base"], "fg": C["text"], "font": FONTS["body_bold"]}
        pad_k = {"padx": (16, 4), "pady": 3}
        pad_v = {"padx": (0, 16), "pady": 3}

        def build(body):
            row = 0
            tk.Label(body, text="Mode", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            mode_fg = C["blue"] if mode == "run" else C["peach"]
            tk.Label(body, text=mode_label, bg=C["base"], fg=mode_fg,
                     font=FONTS["body_bold"]).grid(row=row, column=1, sticky="w", **pad_v)
            row += 1

            tk.Label(body, text="Source", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
            tk.Label(body, text=self._source_type_var.get(), **kw_val).grid(
                row=row, column=1, sticky="w", **pad_v)
            tk.Label(body, text="Overwrite", **kw_key).grid(row=row, column=2, sticky="e", **pad_k)
            tk.Label(body, text="ON" if ov_on else "OFF", bg=C["base"],
                     fg=C["red"] if ov_on else C["subtext"],
                     font=FONTS["body_bold"]).grid(row=row, column=3, sticky="w", **pad_v)
            row += 1

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

            if params_str:
                tk.Label(body, text="Params", **kw_key).grid(row=row, column=0, sticky="e", **pad_k)
                tk.Label(body, text=params_str, **kw_val).grid(
                    row=row, column=1, columnspan=3, sticky="w", **pad_v)

        return self._themed_confirm("━ 실행 확인", build)

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
        # 상태 복원
        self._work_dir.set(wd)
        self._reload_project()
        # 테마 드롭다운을 새 theme_var에 연결
        self._theme_var.set(theme_name)
        self._restore_snapshot(snap)

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
            messagebox.showwarning("경로 없음", f"경로를 찾을 수 없습니다:\n{path_str}")
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
