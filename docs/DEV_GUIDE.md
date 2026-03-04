# ELT Runner GUI — 코드 수정 가이드

> AI 도움 없이 핫픽스/기능 추가를 직접 수행하기 위한 개발자 참조 문서

---

## 1. 프로젝트 구조 개요

```
elt_runner_master/
├── batch_runner_gui.py          # 진입점 (~10줄, gui.BatchRunnerGUI 실행)
├── runner.py                    # CLI 진입점 (argparse → 파이프라인 실행)
├── VERSION                      # 버전 문자열
├── gui/
│   ├── __init__.py              # BatchRunnerGUI re-export
│   ├── constants.py             # THEMES, C, FONTS, TOOLTIPS, STAGE_CONFIG
│   ├── utils.py                 # load_jobs, load_env_hosts, scan_sql_params, collect_sql_tree
│   ├── widgets.py               # SqlSelectorDialog, CollapsibleSection, Tooltip
│   ├── app.py                   # BatchRunnerGUI 클래스 (Mixin 조립 + __init__)
│   └── mixins/
│       ├── ui_build.py          # _build_* 메서드 (UI 레이아웃 전체)
│       ├── state_job.py         # 스냅샷, Job 저장/로드, Source/Target 핸들러
│       ├── run_control.py       # 실행/중지, 프로세스, 프로그레스바
│       ├── dialogs.py           # 확인 다이얼로그, 테마, 탐색기, 로그 내보내기
│       ├── log_panel.py         # 로그 쓰기/필터/컨텍스트메뉴
│       └── search.py            # Ctrl+F 검색
├── stages/                      # 파이프라인 스테이지 (export, load, transform, report)
├── config/                      # env.yml 등 설정
├── jobs/                        # Job YAML 파일들
└── docs/                        # 문서
```

---

## 2. Mixin 아키텍처 이해

### 2.1 클래스 상속 구조

`BatchRunnerGUI`는 6개 Mixin + `tk.Tk`를 조합합니다:

```python
# gui/app.py
class BatchRunnerGUI(
    UiBuildMixin,      # UI 레이아웃 빌드
    RunControlMixin,   # 실행/중지 제어
    StateJobMixin,     # 상태 저장/복원, Job 관리
    LogPanelMixin,     # 로그 출력/필터
    SearchMixin,       # 검색 기능
    DialogsMixin,      # 다이얼로그, 테마
    tk.Tk,             # Tkinter 루트 윈도우
):
```

### 2.2 MRO (Method Resolution Order)

Python MRO에 의해 동일 메서드명이 여러 Mixin에 있으면 **위에서 아래** 순서로 우선됩니다.
- `UiBuildMixin` > `RunControlMixin` > `StateJobMixin` > `LogPanelMixin` > `SearchMixin` > `DialogsMixin` > `tk.Tk`

### 2.3 핵심 규칙

| 규칙 | 설명 |
|------|------|
| **C dict 공유** | `constants.py`의 `C = dict(THEMES["Mocha"])`를 모든 모듈이 참조. 테마 변경 시 `C.update()`(in-place) |
| **import 방향** | `app.py → mixins/* → constants/widgets/utils` (단방향, 순환 금지) |
| **TYPE_CHECKING** | Mixin에서 `if TYPE_CHECKING: from gui.app import BatchRunnerGUI` → IDE 자동완성용 |
| **self 타입 힌트** | 모든 Mixin 메서드는 `self: "BatchRunnerGUI"` 타입 힌트 사용 |

### 2.4 Mixin 간 메서드 호출

모든 Mixin 메서드는 런타임에서 하나의 `BatchRunnerGUI` 인스턴스에 바인딩되므로, 어떤 Mixin에서든 다른 Mixin의 메서드를 `self.xxx()`로 호출할 수 있습니다:

```python
# run_control.py에서 log_panel.py의 메서드 호출
class RunControlMixin:
    def _on_run(self: "BatchRunnerGUI", *, scheduled=False):
        self._clear_log()       # ← LogPanelMixin의 메서드
        self._log_sys("...")    # ← LogPanelMixin의 메서드
        cmd = self._build_command()
```

---

## 3. 수정 시나리오별 가이드

### 3.1 새 설정 옵션 추가하기

**예: Export에 `chunk_size` 옵션 추가**

#### Step 1: `app.py` — 변수 선언 (약 88행 부근)

```python
# Advanced overrides
self._ov_overwrite    = tk.BooleanVar(value=False)
self._ov_workers      = tk.IntVar(value=1)
self._ov_chunk_size   = tk.IntVar(value=5000)  # ← 새 변수 추가
```

#### Step 2: `ui_build.py` — UI 위젯 추가

해당 Stage 섹션의 `_build_export_section()` 등에서 `_ov_row()` 헬퍼 사용:

```python
# Export 섹션 내부 (_build_export_section)
self._ov_row(body, "Chunk Size",
    lambda r: tk.Spinbox(r, from_=100, to=100000,
                         textvariable=self._ov_chunk_size,
                         width=8, bg=C["surface0"], fg=C["text"],
                         font=FONTS["mono"]).pack(side="left"),
    note="rows/chunk",
    tooltip="Export 시 한 번에 처리할 행 수")
```

#### Step 3: `state_job.py` — 스냅샷에 포함

`_snapshot()` 메서드의 `overrides` dict에 추가:

```python
"overrides": {
    "overwrite":    self._ov_overwrite.get(),
    "chunk_size":   self._ov_chunk_size.get(),  # ← 추가
    ...
}
```

`_restore_snapshot()` 메서드에도 복원 코드 추가:

```python
self._ov_chunk_size.set(ov.get("chunk_size", 5000))  # ← 추가
```

`_build_gui_config()` 메서드에서 YAML dict에 포함:

```python
"export": {
    ...
    "chunk_size": self._ov_chunk_size.get(),  # ← 추가
}
```

#### Step 4: 툴팁 텍스트 (`constants.py`)

```python
TOOLTIPS = {
    "chunk_size": "Export 시 한 번에 처리할 행 수\n클수록 빠르지만 메모리 사용 증가",
    ...
}
```

> **체크리스트**: 변수 선언(app.py) → UI(ui_build.py) → 스냅샷/복원(state_job.py) → YAML 빌드(state_job.py) → 툴팁(constants.py)

---

### 3.2 새 버튼 추가하기

#### 상단 타이틀 바에 버튼 추가

`ui_build.py`의 `_build_title_bar()` (약 120행~) 참조:

```python
# 기존 버튼들 옆에 추가 (pack side="right"이므로 오른쪽부터 채워짐)
tk.Button(top_btn_frame, text="My Btn",
          font=FONTS["button_sm"],
          bg=C["surface0"], fg=C["text"],
          activebackground=C["surface1"],
          relief="flat", padx=6,
          command=self._my_new_handler
).pack(side="right", padx=2)
```

#### 로그 패널 헤더에 버튼 추가

`ui_build.py`의 `_build_right()` (약 979행~) 참조. 헤더 영역에서 `pack(side="right")` 순서를 확인합니다.

> **주의**: `pack(side="right")`는 오른쪽에서 왼쪽으로 채워집니다. 먼저 pack한 것이 더 오른쪽에 위치합니다.

---

### 3.3 새 Mixin 메서드 추가하기

1. **어느 파일에 넣을지 결정**:
   - UI 레이아웃 관련 → `ui_build.py`
   - 실행/프로세스 관련 → `run_control.py`
   - Job/설정 저장/복원 → `state_job.py`
   - 로그 관련 → `log_panel.py`
   - 다이얼로그/유틸 → `dialogs.py`
   - 검색 관련 → `search.py`

2. **메서드 작성 규칙**:

```python
# 예: dialogs.py에 새 메서드 추가
class DialogsMixin:

    def _my_new_feature(self: "BatchRunnerGUI"):
        """내 새 기능 설명"""
        # self를 통해 다른 Mixin의 메서드/변수에 자유롭게 접근
        self._log_sys("새 기능 실행됨")
        wd = Path(self._work_dir.get())
        ...
```

3. **TYPE_CHECKING import 확인** (파일 상단에 이미 있으면 생략):

```python
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from gui.app import BatchRunnerGUI
```

---

### 3.4 테마/색상 수정하기

#### 새 테마 추가

`constants.py`의 `THEMES` dict에 18개 색상 키를 모두 포함하는 dict 추가:

```python
THEMES = {
    ...
    "My Theme": {
        "base": "#...", "mantle": "#...", "crust": "#...",
        "surface0": "#...", "surface1": "#...", "surface2": "#...",
        "overlay0": "#...", "overlay1": "#...",
        "text": "#...", "subtext": "#...",
        "blue": "#...", "green": "#...", "yellow": "#...",
        "red": "#...", "peach": "#...", "mauve": "#...",
        "teal": "#...", "sky": "#...",
    },
}
```

추가만 하면 자동으로 Theme Combobox에 나타납니다.

#### 색상 키 용도

| 키 | 용도 |
|---|---|
| `base` | 메인 배경색 |
| `mantle` | 섹션 내부 배경, 패널 배경 |
| `crust` | 가장 어두운 배경 (버튼 활성 상태) |
| `surface0~2` | 입력 필드, 버튼 배경 (밝기 단계) |
| `overlay0~1` | 비활성 텍스트, 구분선 |
| `text` | 기본 텍스트 색 |
| `subtext` | 보조 텍스트, 라벨 |
| `blue` | Export 관련, 강조색 |
| `green` | 성공, 실행 버튼 |
| `yellow` | 경고 |
| `red` | 에러, 중지 버튼 |
| `peach` | Report 관련 |
| `mauve` | Transform 관련 |
| `teal` | Load 관련 |
| `sky` | 링크, 보조 강조 |

---

### 3.5 `_on_run` 실행 흐름 수정하기

`run_control.py`의 `_on_run()` 실행 순서:

```
1. 확인 다이얼로그 (overwrite/run)        ← scheduled=True면 건너뜀
2. _clear_log()                           ← scheduled=True면 건너뜀
3. _cleanup_old_logs()                    ← 30일 이상 로그 파일 삭제
4. _build_command()                       ← GUI 상태 → yml + CLI 인자 조립
5. _log_sys(명령어)                       ← 로그에 실행 명령 출력
6. UI 상태 변경 (버튼 비활성, 프로그레스바)
7. subprocess.Popen(cmd, ...)             ← runner.py 프로세스 시작
8. _stream_output() (별도 스레드)          ← stdout 실시간 파싱 → 로그+프로그레스
```

**주의사항**:
- `scheduled=True`는 Queue/Schedule 실행 시 사용 — 확인 다이얼로그와 로그 클리어를 건너뜀
- `_build_command()`는 내부에서 `_build_gui_config()` → YAML 파일 저장 → CLI 인자 반환

---

### 3.6 새 Stage 추가하기

현재 4개 Stage: Export → Load → Transform → Report

#### Step 1: `constants.py` — STAGE_CONFIG에 추가

```python
STAGE_CONFIG = [
    ("export",     "Export",    "blue"),
    ("load_local", "Load",      "teal"),
    ("transform",  "Transform", "mauve"),
    ("report",     "Report",    "peach"),
    ("validate",   "Validate",  "sky"),    # ← 새 Stage
]
```

#### Step 2: `app.py` — BooleanVar 추가

```python
self._stage_validate = tk.BooleanVar(value=True)
```

#### Step 3: `ui_build.py` — 섹션 빌드 메서드 추가

```python
def _build_validate_section(self: "BatchRunnerGUI", parent):
    sec = CollapsibleSection(parent, "Validate", color_key="sky")
    sec.pack(fill="x")
    body = sec.body
    # 옵션 위젯들 추가...
    self._validate_section = sec
```

`_build_option_sections()`에서 호출 추가.

#### Step 4: `state_job.py` — 스냅샷/복원/GUI Config에 추가

스냅샷, 복원, `_build_gui_config()`에 새 Stage 관련 필드 추가.

#### Step 5: `run_control.py` — `_build_command_args()`에 Stage 포함

`selected_stages` 리스트에 `"validate"` 포함 가능하도록.

---

## 4. UI 헬퍼 메서드 레퍼런스

### 4.1 `_entry_row(parent, label, var, **kw)`

텍스트 입력 행. 라벨(좌) + Entry(우).

```python
self._entry_row(sec.body, "Schema", self._transform_schema)
```

### 4.2 `_path_row(parent, label, var, browse_title)`

경로 입력 행. 라벨(좌) + Entry + `...`버튼 + `📂`버튼.

```python
self._path_row(sec.body, "SQL Dir", self._export_sql_dir, "Select SQL folder")
```

### 4.3 `_ov_row(parent, label, widget_fn, note, tooltip)`

커스텀 위젯 행. `widget_fn(frame)`으로 위젯을 주입.

```python
# Checkbox
self._ov_row(body, "Overwrite",
    lambda r: tk.Checkbutton(r, variable=self._ov_overwrite,
                             bg=C["mantle"], fg=C["green"],
                             selectcolor=C["surface0"],
                             activebackground=C["mantle"]).pack(side="left"),
    tooltip=TOOLTIPS.get("overwrite", ""))

# Combobox
self._ov_row(body, "Compression",
    lambda r: ttk.Combobox(r, textvariable=self._ov_compression,
                           values=["gzip", "none"], width=10,
                           state="readonly").pack(side="left"),
    tooltip=TOOLTIPS.get("compression", ""))

# Spinbox
self._ov_row(body, "Workers",
    lambda r: tk.Spinbox(r, from_=1, to=4,
                         textvariable=self._ov_workers,
                         width=4, bg=C["surface0"], fg=C["text"],
                         font=FONTS["mono"]).pack(side="left"),
    note="1~4", tooltip=TOOLTIPS.get("workers", ""))
```

### 4.4 `CollapsibleSection(parent, title, color_key, expanded)`

접기/펼치기 가능한 섹션. `.body` 프로퍼티로 내용 프레임 접근:

```python
sec = CollapsibleSection(parent, "My Section", color_key="blue", expanded=True)
sec.pack(fill="x")
# sec.body에 위젯 추가
self._entry_row(sec.body, "Field", some_var)
```

### 4.5 `Tooltip(widget, text)`

위젯에 마우스 호버 시 툴팁 표시:

```python
lbl = tk.Label(frame, text="Option")
Tooltip(lbl, "이 옵션에 대한 설명")
```

---

## 5. 상태 관리 (스냅샷/복원)

### 5.1 흐름

```
[Job 로드] → _restore_snapshot(snap)
           → _capture_loaded_snapshot()  # 기준점 저장

[사용자 변경] → _is_dirty() == True     # 스냅샷과 비교
             → 타이틀에 "*" 표시

[Job 저장] → _build_gui_config() → YAML 파일 기록
           → _capture_loaded_snapshot()  # 기준점 리셋
```

### 5.2 새 옵션 추가 시 체크포인트

새 설정 변수를 추가하면 **반드시** 아래 3곳을 모두 수정해야 합니다:

1. **`_snapshot()`** — 현재 값 읽기
2. **`_restore_snapshot()`** — dict에서 값 복원
3. **`_build_gui_config()`** — YAML dict에 포함

하나라도 빠지면:
- `_snapshot` 누락 → dirty check가 변경 감지 못함
- `_restore_snapshot` 누락 → Job 로드 시 기본값으로 리셋됨
- `_build_gui_config` 누락 → CLI 실행 시 옵션이 반영되지 않음

---

## 6. 로그 시스템

### 6.1 로그 쓰기 메서드

| 메서드 | 용도 |
|--------|------|
| `_log_write(text, tag)` | 단일 줄 로그. tag으로 색상 결정 |
| `_log_write_batch(lines)` | 여러 줄 배치 쓰기. `lines = [(text, tag), ...]` |
| `_log_sys(msg)` | 시스템 메시지 (`SYS` 태그) |
| `_clear_log()` | 로그 전체 삭제 |

### 6.2 태그 종류와 색상

| 태그 | 색상 | 용도 |
|------|------|------|
| `INFO` | text (기본) | 일반 로그 |
| `SUCCESS` | green | 성공 메시지 |
| `WARN` | yellow | 경고 |
| `ERROR` | red | 에러 |
| `SYS` | sky | 시스템 메시지 (실행 시작, 종료 등) |
| `JOB_INFO` | subtext | Job 정보 |
| `STAGE_HEADER` | blue, 굵음 | `[1/4] EXPORT` 같은 스테이지 시작 |
| `STAGE_DONE` | teal, 굵음 | 스테이지 완료 |
| `SUMMARY` | text, 굵음 | 최종 요약 |

### 6.3 로그 필터

필터 레벨: `ALL` → `SUM` → `WARN+` → `ERR`
- `_should_show_line(tag)` 메서드에서 각 레벨별 표시 태그 결정
- `_set_log_filter(level)` → 필터 변경 + 전체 리필터

### 6.4 성능 보호

- **라인 상한**: `LOG_MAX_LINES = 20,000` (초과 시 오래된 2,000줄 자동 삭제)
- **실행 시 클리어**: `_on_run()`에서 수동 실행 시 로그 초기화 (Queue/Schedule은 보존)
- **파일 정리**: `_cleanup_old_logs()`로 30일 이상 로그 파일 삭제

---

## 7. 자주 하는 수정 패턴

### 7.1 기존 옵션의 기본값 변경

`app.py`에서 해당 변수의 초기값만 수정:

```python
# 예: workers 기본값 1 → 2
self._ov_workers = tk.IntVar(value=2)  # 변경
```

### 7.2 Combobox 선택지 추가

`ui_build.py`에서 해당 Combobox의 `values` 리스트 수정:

```python
# 예: compression에 "zstd" 추가
ttk.Combobox(..., values=["gzip", "none", "zstd"], ...)
```

### 7.3 확인 다이얼로그 추가

`dialogs.py`의 `_themed_confirm()` 사용:

```python
def _my_confirm(self: "BatchRunnerGUI") -> bool:
    return self._themed_confirm(
        "확인",
        lambda body: tk.Label(body, text="정말 실행하시겠습니까?",
                              font=FONTS["body"], bg=C["base"], fg=C["text"]).pack(),
        ok_text="실행",
        ok_color="green",
    )
```

### 7.4 단축키 추가

`ui_build.py`의 `_bind_shortcuts()` 메서드에서:

```python
def _bind_shortcuts(self: "BatchRunnerGUI"):
    self.bind("<Control-r>", lambda e: self._on_run())
    self.bind("<Control-f>", lambda e: self._toggle_search())
    self.bind("<Control-n>", lambda e: self._my_shortcut())  # ← 추가
```

### 7.5 OS 탐색기에서 폴더/파일 열기

`dialogs.py`의 `_open_in_explorer()` 사용:

```python
self._open_in_explorer(str(some_path))
# Windows: explorer, macOS: open, Linux: xdg-open
```

---

## 8. 테마 전환 구조

### 8.1 전환 흐름

```
Theme Combobox 변경
  → _apply_theme(theme_name)          # dialogs.py
     → C.update(THEMES[theme_name])   # 전역 색상 dict 갱신
     → _rebuild_all()                 # 전체 UI 재구성
        → _build_style()
        → _build_ui()
        → 위젯 참조 재바인딩
```

### 8.2 주의사항

- `C`는 **mutable dict** — `C.update()`로 in-place 변경해야 참조 유지됨
- `C = THEMES[name]` 같은 **재할당은 금지** (다른 모듈의 참조가 끊어짐)
- 테마 변경 후 `_build_style()` → `_build_ui()` 전체 재빌드 필요

---

## 9. 디버깅 팁

### 9.1 컴파일 검증

모든 `.py` 파일의 구문 오류 확인:

```bash
python -m py_compile gui/app.py
python -m py_compile gui/mixins/ui_build.py
python -m py_compile gui/mixins/state_job.py
python -m py_compile gui/mixins/run_control.py
python -m py_compile gui/mixins/dialogs.py
python -m py_compile gui/mixins/log_panel.py
python -m py_compile gui/mixins/search.py
```

### 9.2 MRO 검증

```bash
python -c "
import sys; sys.path.insert(0, '.')
from gui.app import BatchRunnerGUI
print([c.__name__ for c in BatchRunnerGUI.__mro__])
"
```

출력 예:
```
['BatchRunnerGUI', 'UiBuildMixin', 'RunControlMixin', 'StateJobMixin',
 'LogPanelMixin', 'SearchMixin', 'DialogsMixin', 'Tk', 'Misc', 'Wm', 'object']
```

### 9.3 AST 기반 메서드 존재 확인

tkinter 없는 환경에서도 메서드 존재 확인:

```python
import ast
tree = ast.parse(open("gui/mixins/run_control.py").read())
methods = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
print(methods)
```

### 9.4 실행 테스트

```bash
# GUI 실행
python batch_runner_gui.py

# CLI 실행 (dryrun)
python runner.py --job my_job.yml --mode plan

# 특정 테마로 실행
ELT_GUI_THEME="Nord" python batch_runner_gui.py
```

---

## 10. 파일별 수정 빈도 가이드

| 수정 목적 | 주 수정 파일 | 보조 수정 파일 |
|-----------|-------------|---------------|
| 새 옵션/설정 추가 | `app.py`, `ui_build.py`, `state_job.py` | `constants.py` (툴팁) |
| 버튼 추가 | `ui_build.py` | `dialogs.py` (핸들러) |
| 실행 로직 변경 | `run_control.py` | `state_job.py` (CLI args) |
| 로그 동작 변경 | `log_panel.py` | — |
| 다이얼로그 추가 | `dialogs.py` | `ui_build.py` (호출부) |
| 테마/색상 | `constants.py` | — |
| 위젯 클래스 | `widgets.py` | — |
| 유틸/파서 | `utils.py` | — |

---

## 11. 주의사항 (실수 방지)

1. **C dict를 재할당하지 마세요**: `C = {...}` 대신 `C.update({...})`
2. **순환 import 금지**: Mixin → `constants`/`widgets`/`utils`만 import 가능. Mixin끼리 직접 import 하지 않음
3. **`_snapshot`과 `_restore_snapshot` 동기화**: 새 필드는 반드시 양쪽 모두에 추가
4. **`scheduled=True` 분기 주의**: `_on_run()`에서 Queue/Schedule 실행 경로를 건드릴 때는 `scheduled` 파라미터 확인
5. **pack 순서 주의**: `side="right"`는 먼저 pack한 것이 더 오른쪽에 위치
6. **Entry/Combobox 변수 연결**: `textvariable=` 파라미터 누락 시 값이 저장되지 않음
7. **`after()` 사용**: UI 업데이트는 메인 스레드에서만 가능. 백그라운드 스레드에서는 `self.after(0, callback)` 사용
