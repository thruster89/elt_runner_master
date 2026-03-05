"""
gui/mixins/log_panel.py  ─  로그 쓰기/필터/컨텍스트메뉴
"""

from __future__ import annotations

import tkinter as tk
from datetime import datetime
from typing import TYPE_CHECKING

from gui.constants import C, FONTS

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI

# 로그 라인 최대 보관 수 (메모리 보호)
LOG_MAX_LINES = 20_000
LOG_TRIM_CHUNK = 2_000  # 초과 시 한 번에 제거할 줄 수


class LogPanelMixin:

    def _log_write(self: "BatchRunnerGUI", text: str, tag="INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        self._log_raw_lines.append((text, tag, ts))
        if self._should_show_line(tag):
            self._insert_log_line(text, tag, ts)
            self._log.see("end")
        self._trim_log_if_needed()

    def _log_write_batch(self: "BatchRunnerGUI", lines: list[tuple[str, str]]):
        """여러 줄을 한 번에 기록 (고속 출력 시 GUI 멈춤 방지)"""
        for text, tag in lines:
            ts = datetime.now().strftime("%H:%M:%S")
            self._log_raw_lines.append((text, tag, ts))
            if self._should_show_line(tag):
                self._insert_log_line(text, tag, ts)
        self._log.see("end")
        self._trim_log_if_needed()

    def _trim_log_if_needed(self: "BatchRunnerGUI"):
        """로그 라인 수가 상한 초과 시 오래된 줄 제거"""
        if len(self._log_raw_lines) <= LOG_MAX_LINES:
            return
        # 오래된 줄 제거
        del self._log_raw_lines[:LOG_TRIM_CHUNK]
        # Text 위젯 갱신
        self._refilter_log()

    def _log_sys(self: "BatchRunnerGUI", msg):
        self._log_write(msg, "SYS")

    def _clear_log(self: "BatchRunnerGUI"):
        self._log.delete("1.0", "end")
        self._log_raw_lines.clear()

    def _set_log_filter(self: "BatchRunnerGUI", level):
        self._log_filter.set(level)
        self._refresh_log_filter_btns()
        self._apply_log_filter_elide()

    def _refresh_log_filter_btns(self: "BatchRunnerGUI"):
        cur = self._log_filter.get()
        for lv, btn in self._log_filter_btns.items():
            if lv == cur:
                btn.config(bg=C["text"], fg=C["crust"], activebackground=C["subtext"])
            else:
                btn.config(bg=C["surface0"], fg=C["subtext"], activebackground=C["surface1"])

    def _should_show_line(self: "BatchRunnerGUI", tag: str) -> bool:
        level = self._log_filter.get()
        if level == "ALL":
            return True
        if level == "SUM":
            return tag in ("SYS", "JOB_INFO", "STAGE_HEADER", "STAGE_DONE", "SUMMARY", "SUCCESS", "ERROR", "WARN")
        if level == "WARN+":
            return tag in ("WARN", "ERROR", "STAGE_HEADER", "STAGE_DONE", "SYS", "JOB_INFO")
        if level == "ERR":
            return tag in ("ERROR",)
        return True

    def _refilter_log(self: "BatchRunnerGUI"):
        self._log.delete("1.0", "end")
        for text, tag, ts in self._log_raw_lines:
            if self._should_show_line(tag):
                self._insert_log_line(text, tag, ts)
        self._log.see("end")

    # 모든 로그 tag 목록 (elide 토글 대상)
    _LOG_ALL_TAGS = ("INFO", "DEBUG", "WARN", "ERROR", "SYS",
                     "JOB_INFO", "STAGE_HEADER", "STAGE_DONE",
                     "SUMMARY", "SUCCESS", "TIME")

    def _apply_log_filter_elide(self: "BatchRunnerGUI"):
        """필터 변경 시 tag elide 토글로 숨김/표시 (delete+re-insert 없이)"""
        for tag in self._LOG_ALL_TAGS:
            show = self._should_show_line(tag) if tag != "TIME" else self._show_time.get()
            try:
                self._log.tag_configure(tag, elide=not show)
            except tk.TclError:
                pass  # 해당 tag가 아직 없을 수 있음
        self._log.see("end")

    def _insert_log_line(self: "BatchRunnerGUI", text: str, tag: str, ts: str):
        if self._show_time.get():
            self._log.insert("end", ts + "  ", "TIME")
        self._log.insert("end", text + "\n", tag)

    def _toggle_show_time(self: "BatchRunnerGUI"):
        self._show_time.set(not self._show_time.get())
        self._refresh_time_btn()
        self._refilter_log()

    def _refresh_time_btn(self: "BatchRunnerGUI"):
        if self._show_time.get():
            self._time_btn.config(bg=C["blue"], fg=C["crust"],
                                  activebackground=C["sky"])
        else:
            self._time_btn.config(bg=C["surface0"], fg=C["subtext"],
                                  activebackground=C["surface1"])

    def _build_log_context_menu(self: "BatchRunnerGUI"):
        self._log_menu = tk.Menu(self._log, tearoff=0,
                                 bg=C["surface0"], fg=C["text"],
                                 activebackground=C["blue"], activeforeground=C["crust"],
                                 font=FONTS["body"])
        self._log_menu.add_command(label="Copy", command=self._log_copy)
        self._log_menu.add_command(label="Select All", command=self._log_select_all)
        self._log_menu.add_separator()
        self._log_menu.add_command(label="Copy Errors", command=self._log_copy_errors)
        self._log_menu.add_separator()
        self._log_menu.add_command(label="Save Log...", command=self._export_log)
        self._log_menu.add_command(label="Clear", command=self._clear_log)

    def _show_log_context_menu(self: "BatchRunnerGUI", event):
        try:
            self._log_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self._log_menu.grab_release()

    def _log_copy(self: "BatchRunnerGUI"):
        try:
            sel = self._log.get("sel.first", "sel.last")
        except tk.TclError:
            sel = self._log.get("1.0", "end-1c")
        self.clipboard_clear()
        self.clipboard_append(sel)

    def _log_select_all(self: "BatchRunnerGUI"):
        self._log.tag_add("sel", "1.0", "end")

    def _log_copy_errors(self: "BatchRunnerGUI"):
        """ERROR 태그가 적용된 줄만 추출하여 클립보드에 복사"""
        errors = []
        ranges = self._log.tag_ranges("ERROR")
        for i in range(0, len(ranges), 2):
            errors.append(self._log.get(ranges[i], ranges[i + 1]))
        text = "\n".join(errors)
        self.clipboard_clear()
        self.clipboard_append(text if text else "(no errors)")

    def _set_status(self: "BatchRunnerGUI", text, color):
        self._status_label.config(text=text, fg=color)

    def _start_idle_timer(self: "BatchRunnerGUI"):
        """유휴 타이머 시작 — 1시간마다 상태 라벨 갱신"""
        self._stop_idle_timer()
        self._idle_hours = 0
        self._idle_timer_id = self.after(3_600_000, self._tick_idle)

    def _stop_idle_timer(self: "BatchRunnerGUI"):
        if self._idle_timer_id:
            self.after_cancel(self._idle_timer_id)
            self._idle_timer_id = None

    def _tick_idle(self: "BatchRunnerGUI"):
        self._idle_hours += 1
        self._set_status(f"● idle ({self._idle_hours}h)", C["overlay1"])
        self._idle_timer_id = self.after(3_600_000, self._tick_idle)
