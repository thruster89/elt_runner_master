# Project Instructions

- 모든 응답은 한국어로 작성할 것

## GUI 패키지 구조 (v1.97.0~)

`batch_runner_gui.py`(진입점 ~10줄) → `gui/` 패키지로 분리됨.

```
gui/
├── __init__.py              # BatchRunnerGUI re-export
├── constants.py             # THEMES, C(전역 mutable dict), FONTS, APP_VERSION, TOOLTIPS, STAGE_CONFIG
├── utils.py                 # load_jobs, load_env_hosts, scan_sql_params, collect_sql_tree
├── widgets.py               # SqlSelectorDialog, CollapsibleSection, Tooltip
├── app.py                   # BatchRunnerGUI 클래스 (Mixin 조립 + __init__)
└── mixins/
    ├── ui_build.py          # _build_* 메서드 전체 (UI 빌드)
    ├── state_job.py         # 스냅샷, Job 관리, 설정 저장/로드, Source/Target 핸들러, Param 관리
    ├── run_control.py       # 실행/중지, 프로세스 제어, 프로그레스, _refresh_preview
    ├── dialogs.py           # _themed_confirm, _apply_theme, _open_in_explorer, _export_log
    ├── log_panel.py         # 로그 쓰기/필터/컨텍스트메뉴
    └── search.py            # Ctrl+F 검색
```

### 핵심 규칙
- **C dict 공유**: `constants.py`에서 `C = dict(THEMES["Mocha"])` 정의, 모든 모듈이 동일 객체 참조. 테마 전환은 `C.update()`(in-place).
- **Mixin cross-call**: 런타임 `self`가 `BatchRunnerGUI` 인스턴스이므로 MRO로 자동 해소.
- **import 방향**: `app.py → mixins/* → constants/widgets/utils` (단방향 DAG, 순환 없음).
- **TYPE_CHECKING 패턴**: Mixin에서 `if TYPE_CHECKING: from gui.app import BatchRunnerGUI` → IDE 힌트용.
