"""
gui/mixins/run_control.py  ─  실행/중지, 프로세스 제어, 프로그레스
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
import sys
import threading
import time
import tkinter as tk
from datetime import datetime
from tkinter import ttk
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from gui.constants import C, FONTS, APP_VERSION

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI


class RunControlMixin:

    def _on_run(self: "BatchRunnerGUI", *, scheduled=False):
        mode = self.mode_var.get()

        # overwrite=true 확인 (Dryrun은 실제 덮어쓰기 없으므로 스킵)
        if not scheduled and self._ov_overwrite.get() and mode != "plan":
            if not self._show_overwrite_confirm():
                return

        # 실행 전 확인 다이얼로그 (Dryrun/예약은 확인 없이 바로 실행)
        if not scheduled and mode != "plan":
            if not self._show_run_confirm():
                return

        cmd = self._build_command()
        self._log_sys(f"Run: {chr(32).join(cmd)}")
        self._set_status("● running", C["green"])
        self._elapsed_start = time.time()
        self._progress_bar["value"] = 0
        self._progress_label.config(text="Starting...")
        self._elapsed_job_id = self.after(1000, self._tick_elapsed)

        self._dryrun_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._run_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._retry_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="disabled")
        self._stop_btn.config(state="normal", bg=C["red"], fg=C["crust"],
                              activebackground=C["peach"])
        self._set_left_panel_state(False)
        self._anim_dots = 0
        self._anim_id = self.after(500, self._animate_run_btn)

        env = os.environ.copy()
        # Windows에서 한글 깨짐 방지: PYTHONIOENCODING, PYTHONUTF8 강제 설정
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"

        try:
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                encoding="utf-8",
                errors="replace",
                cwd=self._work_dir.get(),  # workdir 기준 실행
                env=env,
                creationflags=(subprocess.CREATE_NEW_PROCESS_GROUP
                               if sys.platform == "win32" else 0),
            )
        except FileNotFoundError as e:
            self._log_write(f"[Error] Failed to start: {e}", "ERROR")
            self._reset_buttons()
            return

        threading.Thread(target=self._stream_output, daemon=True).start()

    def _stream_output(self: "BatchRunnerGUI"):
        stage_pat = re.compile(r"\[(\d+)/(\d+)\]")
        for line in self._process.stdout:
            line = line.rstrip("\n")
            tag = self._guess_tag(line)
            self.after(0, self._log_write, line, tag)
            # [N/M] 패턴 파싱 → progress 업데이트
            m = stage_pat.search(line)
            if m:
                cur, total = int(m.group(1)), int(m.group(2))
                pct = int((cur - 1) / total * 100)
                label = f"Stage {cur}/{total}"
                self.after(0, self._update_progress, pct, label)
        ret = self._process.wait()
        self.after(0, self._on_done, ret)

    def _on_done(self: "BatchRunnerGUI", ret: int):
        # 애니메이션 취소
        if self._anim_id:
            self.after_cancel(self._anim_id)
            self._anim_id = None
        # elapsed 타이머 정지
        if self._elapsed_job_id:
            self.after_cancel(self._elapsed_job_id)
            self._elapsed_job_id = None
        if self._elapsed_start:
            elapsed = time.time() - self._elapsed_start
            self._elapsed_start = None
        else:
            elapsed = 0.0
        if elapsed < 60:
            elapsed_str = f"{elapsed:.1f}s"
        else:
            m, s = divmod(int(elapsed), 60)
            elapsed_str = f"{m:02d}:{s:02d}"

        if ret == 0:
            self._log_write(f"Done  ({elapsed_str})", "SUCCESS")
            self._set_status("● done", C["green"])
            self._progress_bar["value"] = 100
            self._progress_label.config(text=f"Done  {elapsed_str}")
        elif ret < 0:
            self._log_write(f"Stopped  ({elapsed_str})", "WARN")
            self._set_status("● stopped", C["yellow"])
            self._progress_label.config(text=f"Stopped  {elapsed_str}")
        else:
            self._log_write(f"Error (code={ret})  ({elapsed_str})", "ERROR")
            self._set_status(f"● error (code={ret})", C["red"])
            self._progress_label.config(text=f"Error  {elapsed_str}")
        # 완료 알림
        self.bell()
        self._flash_title()
        job_name = self.job_var.get().replace(".yml", "") if self.job_var.get() else "ELT"
        if ret == 0:
            self._notify_os("ELT Runner ✔", f"{job_name} Done ({elapsed_str})")
        elif ret < 0:
            self._notify_os("ELT Runner", f"{job_name} Stopped ({elapsed_str})")
        else:
            self._notify_os("ELT Runner ✖", f"{job_name} Error (code={ret}, {elapsed_str})")
        self._reset_buttons()

    def _on_stop(self: "BatchRunnerGUI"):
        if self._process and self._process.poll() is None:
            self._log_write("Stopping process...", "WARN")
            if sys.platform == "win32":
                self._process.send_signal(signal.CTRL_BREAK_EVENT)
            else:
                self._process.terminate()
        self._reset_buttons()
        self._set_status("● stopped", C["yellow"])

    def _reset_buttons(self: "BatchRunnerGUI"):
        self._dryrun_btn.config(state="normal", bg=C["yellow"], fg=C["crust"],
                                text="Dryrun")
        self._run_btn.config(state="normal", bg=C["blue"], fg=C["crust"],
                             text="▶  Run")
        self._retry_btn.config(state="normal", bg=C["peach"], fg=C["crust"],
                               text="Retry")
        self._stop_btn.config(state="disabled", bg=C["surface0"], fg=C["overlay0"])
        self._theme_combo.config(state="readonly")
        self._set_left_panel_state(True)
        if hasattr(self, '_stage_status'):
            self._stage_status.config(text="")

    def _build_command(self: "BatchRunnerGUI") -> list[str]:
        """실행용: yml 쓰기 + CLI 인자"""
        cfg = self._build_gui_config()
        wd = Path(self._work_dir.get())
        jobs_dir = wd / "jobs"
        jobs_dir.mkdir(parents=True, exist_ok=True)
        job_name = self.job_var.get() or "_gui_temp.yml"
        temp_path = jobs_dir / job_name
        temp_path.write_text(
            yaml.dump(cfg, allow_unicode=True, default_flow_style=False, sort_keys=False),
            encoding="utf-8"
        )
        return self._build_command_args()

    def _build_command_args(self: "BatchRunnerGUI") -> list[str]:
        """yml 쓰기 없이 CLI 인자만 조립 (preview용)"""
        job_name = self.job_var.get() or "_gui_temp.yml"
        cmd = ["python", "runner.py", "--job", job_name]

        # env
        env_path = self._env_path_var.get().strip()
        if env_path and env_path != "config/env.yml":
            cmd += ["--env", env_path]

        # mode
        mode = self.mode_var.get()
        if mode != "run":
            cmd += ["--mode", mode]

        # debug
        if self._debug_var.get():
            cmd.append("--debug")

        # params (yml에도 있지만 CLI 전달도 유지)
        for k_var, v_var in self._param_entries:
            k, v = k_var.get().strip(), v_var.get().strip()
            if k and v:
                cmd += ["--param", f"{k}={v}"]

        # stages (전부 선택이거나 아무것도 없으면 생략)
        selected_stages = [s for s in ("export", "load_local", "transform", "report")
                           if getattr(self, f"_stage_{s}").get()]
        all_stages = ["export", "load_local", "transform", "report"]
        if selected_stages and selected_stages != all_stages:
            for stage in selected_stages:
                cmd += ["--stage", stage]

        # SQL 파일 선택 → --include 패턴 (sql_dir 기준 상대경로)
        for rel_path in sorted(self._selected_sqls):
            pattern = Path(rel_path).with_suffix("").as_posix()
            cmd += ["--include", str(pattern)]

        # --timeout
        timeout_val = self._ov_timeout.get().strip()
        if timeout_val:
            cmd += ["--timeout", timeout_val]

        return [str(x) for x in cmd]

    def _guess_tag(self: "BatchRunnerGUI", line: str) -> str:
        low = line.lower()
        # STAGE 시작/끝 패턴 (최우선)
        if any(k in low for k in ("=== stage", "--- stage", "stage start", "[stage")):
            return "STAGE_HEADER"
        if any(k in low for k in ("stage done", "stage complete", "stage finish")):
            return "STAGE_DONE"
        # 구분선 / 배너
        if any(k in low for k in ("===", "---", "pipeline", "job start", "job finish")):
            return "SYS"
        # summary 줄: failed=N 값으로 판단 (failed=0이면 SUCCESS)
        if "summary" in low:
            m = re.search(r"failed=(\d+)", low)
            if m and int(m.group(1)) > 0:
                return "ERROR"
            return "SUCCESS"
        # 에러 (명확한 에러 패턴)
        if any(k in low for k in ("exception", "traceback")):
            return "ERROR"
        if re.search(r"\bfailed\b", low) and "failed=" not in low:
            return "ERROR"
        if re.search(r"\berror\b", low):
            return "ERROR"
        # 경고
        if any(k in low for k in ("warn", "warning")):
            return "WARN"
        # 성공
        if any(k in low for k in ("done", "success", "completed")):
            return "SUCCESS"
        return "INFO"

    def _animate_run_btn(self: "BatchRunnerGUI"):
        if self._process is None or self._process.poll() is not None:
            return
        self._anim_dots = (self._anim_dots % 3) + 1
        mode = self.mode_var.get()
        btn = {"plan": self._dryrun_btn, "retry": self._retry_btn}.get(mode, self._run_btn)
        btn.config(text="Running" + "." * self._anim_dots)
        self._anim_id = self.after(500, self._animate_run_btn)

    def _update_progress(self: "BatchRunnerGUI", pct: int, label: str):
        self._progress_bar["value"] = pct
        elapsed = ""
        if self._elapsed_start:
            secs = int(time.time() - self._elapsed_start)
            elapsed = f"  {secs//60:02d}:{secs%60:02d}"
        self._progress_label.config(text=f"{label}{elapsed}")
        if hasattr(self, '_stage_status'):
            self._stage_status.config(text=label)

    def _tick_elapsed(self: "BatchRunnerGUI"):
        if self._elapsed_start is None:
            return
        secs = int(time.time() - self._elapsed_start)
        cur_label = self._progress_label.cget("text").split("  ")[0]
        self._progress_label.config(text=f"{cur_label}  {secs//60:02d}:{secs%60:02d}")
        self._elapsed_job_id = self.after(1000, self._tick_elapsed)

    def _tick_clock(self: "BatchRunnerGUI"):
        import datetime
        days_kr = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        now = datetime.datetime.now()
        day = days_kr[now.weekday()]
        self._clock_label.config(text=now.strftime(f"%Y-%m-%d ({day}) %H:%M"))
        self.after(30000, self._tick_clock)

    def _notify_os(self: "BatchRunnerGUI", title, message):
        """OS 레벨 알림 전송 (실패 시 조용히 무시)"""
        try:
            if sys.platform == "win32":
                import subprocess as sp
                ps_script = (
                    f'[Windows.UI.Notifications.ToastNotificationManager,'
                    f' Windows.UI.Notifications, ContentType = WindowsRuntime] > $null;'
                    f'$xml = [Windows.UI.Notifications.ToastNotificationManager]::GetTemplateContent(1);'
                    f'$text = $xml.GetElementsByTagName("text");'
                    f'$text.Item(0).AppendChild($xml.CreateTextNode("{title}")) > $null;'
                    f'$text.Item(1).AppendChild($xml.CreateTextNode("{message}")) > $null;'
                    f'$toast = [Windows.UI.Notifications.ToastNotification]::new($xml);'
                    f'[Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier("ELT Runner").Show($toast)'
                )
                sp.Popen(["powershell", "-Command", ps_script],
                         creationflags=0x08000000,  # CREATE_NO_WINDOW
                         stdout=sp.DEVNULL, stderr=sp.DEVNULL)
            elif sys.platform == "darwin":
                import subprocess as sp
                sp.Popen(["osascript", "-e",
                          f'display notification "{message}" with title "{title}"'])
            else:
                import subprocess as sp
                sp.Popen(["notify-send", title, message])
        except Exception:
            pass

    def _flash_title(self: "BatchRunnerGUI", count=6):
        if count <= 0:
            self._update_title_dirty()
            return
        if count % 2 == 0:
            self.title(f">> Done -- ELT Runner  v{APP_VERSION}")
        else:
            self.title(f"ELT Runner  v{APP_VERSION}")
        self.after(500, self._flash_title, count - 1)

    def _set_left_panel_state(self: "BatchRunnerGUI", enabled: bool):
        def _recurse(widget):
            for child in widget.winfo_children():
                try:
                    if isinstance(child, ttk.Combobox):
                        child.config(state="readonly" if enabled else "disabled")
                    elif isinstance(child, (tk.Entry, tk.Spinbox, tk.Checkbutton,
                                           tk.Radiobutton, tk.Button, tk.Listbox)):
                        child.config(state="normal" if enabled else "disabled")
                except Exception:
                    pass
                _recurse(child)
        if hasattr(self, '_left_inner'):
            _recurse(self._left_inner)

    def _refresh_preview(self: "BatchRunnerGUI"):
        try:
            cmd = self._build_command_args()
            text = " ".join(cmd)
        except Exception as e:
            text = f"(error: {e})"

        self._cmd_preview.config(state="normal")
        self._cmd_preview.delete("1.0", "end")
        self._cmd_preview.insert("end", text)
        self._cmd_preview.config(state="disabled")

        if not self._restoring_job:
            self._update_title_dirty()

    # ── 예약 실행 ─────────────────────────────────────────────

    def _on_schedule_focus_in(self: "BatchRunnerGUI", _event=None):
        if self._schedule_time.get() == "+30m / 18:00":
            self._schedule_time.set("")
            self._schedule_entry.config(fg=C["text"])

    def _on_schedule_focus_out(self: "BatchRunnerGUI", _event=None):
        if not self._schedule_time.get().strip():
            self._schedule_entry.config(fg=C["overlay0"])
            self._schedule_time.set("+30m / 18:00")

    def _parse_schedule_input(self, raw: str):
        """예약 시각 파싱. 지원 형식: 18:00, +30m, +2h, 0302 18:00"""
        from datetime import timedelta
        now = datetime.now()
        raw = raw.strip()

        # +Nm (분) / +Nh (시간)
        if raw.startswith("+"):
            val = raw[1:].strip()
            if val.endswith("h"):
                return now + timedelta(hours=float(val[:-1]))
            if val.endswith("m"):
                val = val[:-1]
            return now + timedelta(minutes=float(val))

        # MMDD HH:MM
        if " " in raw and len(raw.split()[0]) == 4:
            parts = raw.split(None, 1)
            md, hm = parts[0], parts[1]
            target = datetime.strptime(f"{now.year}{md} {hm}", "%Y%m%d %H:%M")
            if target <= now:
                target = target.replace(year=now.year + 1)
            return target

        # HH:MM (기존)
        target = datetime.strptime(raw, "%H:%M").replace(
            year=now.year, month=now.month, day=now.day)
        if target <= now:
            target += timedelta(days=1)
        return target

    def _on_schedule(self: "BatchRunnerGUI"):
        if self._schedule_id is not None:
            self._cancel_schedule()
            return
        raw = self._schedule_time.get().strip()
        if not raw or raw == "+30m / 18:00":
            self._log_write("[Schedule] Format: +30m, +2h, 18:00, 0302 18:00", "WARN")
            return
        try:
            target = self._parse_schedule_input(raw)
        except (ValueError, IndexError):
            self._log_write("[Schedule] Format: +30m, +2h, 18:00, 0302 18:00", "WARN")
            return
        self._schedule_target = target
        self._schedule_id = self.after(1000, self._tick_schedule)
        self._schedule_btn.config(text="✕ Cancel", bg=C["red"], fg=C["crust"],
                                  activebackground=C["peach"])
        self._schedule_entry.config(state="disabled")
        remaining = int((target - datetime.now()).total_seconds())
        m, s = divmod(remaining, 60)
        h, m = divmod(m, 60)
        label = target.strftime("%m/%d %H:%M")
        self._schedule_label.config(
            text=f" {label} reserved ({h:02d}:{m:02d}:{s:02d})", fg=C["green"])
        self._log_sys(f"[Schedule] Reserved for {label}")

    def _cancel_schedule(self: "BatchRunnerGUI"):
        if self._schedule_id is not None:
            self.after_cancel(self._schedule_id)
            self._schedule_id = None
        self._schedule_btn.config(text="⏱ Reserve", bg=C["surface0"], fg=C["subtext"],
                                  activebackground=C["surface1"])
        self._schedule_entry.config(state="normal")
        self._schedule_label.config(text="")
        self._log_sys("[Schedule] Cancelled")

    def _tick_schedule(self: "BatchRunnerGUI"):
        now = datetime.now()
        remaining = int((self._schedule_target - now).total_seconds())
        if remaining <= 0:
            self._schedule_id = None
            self._schedule_btn.config(text="⏱ Reserve", bg=C["surface0"],
                                      fg=C["subtext"],
                                      activebackground=C["surface1"])
            self._schedule_entry.config(state="normal")
            self._schedule_label.config(text="")
            self._log_sys(f"[Schedule] {self._schedule_target.strftime('%H:%M')} Starting")
            self.mode_var.set("run")
            self._on_run(scheduled=True)
            return
        m, s = divmod(remaining, 60)
        h, m = divmod(m, 60)
        label = self._schedule_target.strftime("%m/%d %H:%M")
        self._schedule_label.config(
            text=f" {label} reserved ({h:02d}:{m:02d}:{s:02d})")
        self._schedule_id = self.after(1000, self._tick_schedule)
