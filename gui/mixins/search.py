"""
gui/mixins/search.py  ─  Ctrl+F 로그 검색
"""

from __future__ import annotations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI


class SearchMixin:

    def _toggle_search(self: "BatchRunnerGUI"):
        if self._search_frame.winfo_viewable():
            self._search_frame.pack_forget()
            self._clear_search_highlights()
        else:
            self._search_frame.pack(fill="x", padx=8, pady=(0, 4), before=self._log)
            self._search_entry.focus_set()

    def _on_search_change(self: "BatchRunnerGUI", *_):
        self._clear_search_highlights()
        query = self._search_var.get()
        if not query:
            self._search_count_label.config(text="")
            return
        self._search_matches = []
        start = "1.0"
        while True:
            pos = self._log.search(query, start, stopindex="end", nocase=True)
            if not pos:
                break
            end = f"{pos}+{len(query)}c"
            self._log.tag_add("HIGHLIGHT", pos, end)
            self._search_matches.append(pos)
            start = end
        count = len(self._search_matches)
        self._search_match_idx = 0
        self._search_count_label.config(text=f"{count} found" if count else "not found")
        if self._search_matches:
            self._log.see(self._search_matches[0])

    def _search_next(self: "BatchRunnerGUI"):
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx + 1) % len(self._search_matches)
        self._log.see(self._search_matches[self._search_match_idx])

    def _search_prev(self: "BatchRunnerGUI"):
        if not self._search_matches:
            return
        self._search_match_idx = (self._search_match_idx - 1) % len(self._search_matches)
        self._log.see(self._search_matches[self._search_match_idx])

    def _clear_search_highlights(self: "BatchRunnerGUI"):
        self._log.tag_remove("HIGHLIGHT", "1.0", "end")
        self._search_matches = []
        self._search_match_idx = 0
