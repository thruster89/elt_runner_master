"""
gui/utils.py  ─  순수 유틸리티 함수 (프로젝트 데이터 읽기)
"""

import logging
import re
import yaml
from pathlib import Path

_log = logging.getLogger(__name__)


def load_jobs(work_dir: Path) -> dict:
    """jobs/ 폴더의 *.yml 파싱 → {filename: parsed_dict}

    탐색 순서:
      1) jobs/*.yml          (글로벌)
      2) jobs/{name}/{name}.yml  (job-centric)
    동일 이름이면 job-centric(하위 폴더) 우선.
    """
    jobs = {}
    jobs_dir = work_dir / "jobs"
    if not jobs_dir.exists():
        return jobs

    # 1) 글로벌: jobs/*.yml
    for f in sorted(jobs_dir.glob("*.yml")):
        try:
            data = yaml.safe_load(f.read_text(encoding="utf-8"))
            jobs[f.name] = data
        except Exception:
            _log.debug("job yml 파싱 실패: %s", f, exc_info=True)

    # 2) job-centric: jobs/{name}/{name}.yml
    for d in sorted(jobs_dir.iterdir()):
        if not d.is_dir():
            continue
        f = d / f"{d.name}.yml"
        if f.is_file():
            try:
                data = yaml.safe_load(f.read_text(encoding="utf-8"))
                jobs[f.name] = data          # 동일 이름이면 job-centric 우선
            except Exception:
                _log.debug("job yml 파싱 실패: %s", f, exc_info=True)

    return jobs


def load_env_hosts(work_dir: Path, env_path: str = "config/env.yml") -> dict:
    """env.yml 에서 source type → host 목록 반환 {type: [host, ...]}"""
    result = {}
    p = Path(env_path) if Path(env_path).is_absolute() else work_dir / env_path
    if not p.exists():
        return result
    try:
        env = yaml.safe_load(p.read_text(encoding="utf-8"))
        sources = env.get("sources", {})
        for src_type, cfg in sources.items():
            hosts = list((cfg.get("hosts") or {}).keys())
            if hosts:
                result[src_type] = hosts
    except Exception:
        _log.debug("env.yml 파싱 실패: %s", p, exc_info=True)
    return result


# ── SQL 파라미터 스캔 공통 ──────────────────────────────────

_PAT_DOLLAR = re.compile(r'\$\{(\w+)\}')
_PAT_HASH   = re.compile(r'\{#(\w+)\}')
_PAT_AT     = re.compile(r'@\{(\w+)\}')
_PAT_COLON  = re.compile(r'(?<![:\w]):(\w+)\b')
_EXCLUDE = {
    "null","true","false","and","or","not","in","is","as","by","on",
    "MI","SS","HH","HH12","HH24","DD","MM","MON","MONTH","YY","YYYY",
    "RR","DY","DAY","WW","IW","Q","J","FF","TZH","TZM","TZR","TZD",
}


def _non_literal_chunks(text):
    """싱글쿼트 리터럴 밖 텍스트 청크 yield"""
    i, n, buf_start = 0, len(text), 0
    while i < n:
        if text[i] == "'":
            yield text[buf_start:i]
            i += 1
            while i < n:
                if text[i] == "'":
                    if i + 1 < n and text[i+1] == "'":
                        i += 2; continue
                    else:
                        i += 1; break
                i += 1
            buf_start = i
        else:
            i += 1
    yield text[buf_start:]


def _extract_params(text: str) -> set[str]:
    """단일 SQL 텍스트에서 파라미터 추출"""
    found: set[str] = set()
    for pat in (_PAT_DOLLAR, _PAT_HASH, _PAT_AT):
        for m in pat.finditer(text):
            if m.group(1) not in _EXCLUDE:
                found.add(m.group(1))
    for chunk in _non_literal_chunks(text):
        for m in _PAT_COLON.finditer(chunk):
            if m.group(1) not in _EXCLUDE:
                found.add(m.group(1))
    return found


def scan_sql_params(sql_dir: Path, extra_dirs: list[Path] | None = None) -> list[str]:
    """
    sql_dir 하위 .sql 파일 전체 스캔,
    :param  {#param}  ${param} 세 가지 패턴으로 파라미터 이름 추출 → 정렬된 리스트 반환.
    싱글쿼트 문자열 리터럴 내부의 :word 는 파라미터로 인식하지 않음.

    extra_dirs: 추가로 스캔할 디렉토리 리스트 (명시적 지정만 탐색, 부모 자동 탐색 없음).
    """
    found: set[str] = set()
    if not sql_dir.exists():
        return []

    scan_dirs = [sql_dir]
    if extra_dirs:
        for d in extra_dirs:
            if d.exists() and d not in scan_dirs:
                scan_dirs.append(d)

    for scan_dir in scan_dirs:
        for sql_file in scan_dir.rglob("*.sql"):
            try:
                text = sql_file.read_text(encoding="utf-8", errors="ignore")
                found |= _extract_params(text)
            except Exception:
                _log.debug("SQL 파일 파싱 실패: %s", sql_file, exc_info=True)
    return sorted(found)


def _scan_params_from_files(files: list) -> list[str]:
    """지정 파일 목록만 스캔해서 파라미터 추출 (sql filter 선택 시 사용)"""
    found: set[str] = set()
    for sql_file in files:
        try:
            text = Path(sql_file).read_text(encoding="utf-8", errors="ignore")
            found |= _extract_params(text)
        except Exception:
            _log.debug("SQL 파일 파싱 실패: %s", sql_file, exc_info=True)
    return sorted(found)


_sql_tree_cache: dict[str, tuple[float, dict]] = {}  # path → (mtime, tree)


def clear_sql_tree_cache():
    """SQL tree 캐시 초기화 (Refresh 시 호출)."""
    _sql_tree_cache.clear()


def collect_sql_tree(sql_dir: Path) -> dict:
    """
    sql_dir 하위 폴더/파일 트리 반환 (디렉토리 mtime 캐시).
    {
      "export": {
          "__files__": ["01_contract.sql", "02_payment.sql"],
          "A": {"__files__": ["a1.sql", "a2.sql"]},
          "B": {"__files__": ["rate.sql"]},
      },
      ...
    }
    """
    if not sql_dir.exists():
        return {}

    cache_key = str(sql_dir.resolve())
    try:
        dir_mtime = sql_dir.stat().st_mtime
    except OSError:
        dir_mtime = 0.0
    cached = _sql_tree_cache.get(cache_key)
    if cached and cached[0] == dir_mtime:
        return cached[1]

    def _walk(path: Path) -> dict:
        node = {"__files__": []}
        try:
            entries = sorted(path.iterdir())
        except (PermissionError, OSError):
            return node
        for item in entries:
            if item.is_file() and item.suffix.lower() == ".sql":
                node["__files__"].append(item.name)
            elif item.is_dir():
                node[item.name] = _walk(item)
        return node

    tree = {"__files__": []}
    try:
        entries = sorted(sql_dir.iterdir())
    except (PermissionError, OSError):
        _sql_tree_cache[cache_key] = (dir_mtime, tree)
        return tree
    for item in entries:
        if item.is_dir():
            tree[item.name] = _walk(item)
        elif item.is_file() and item.suffix.lower() == ".sql":
            tree["__files__"].append(item.name)
    _sql_tree_cache[cache_key] = (dir_mtime, tree)
    return tree


def flatten_sql_tree(sql_dir: Path, tree: dict, prefix: str = "") -> list[Path]:
    """collect_sql_tree 결과를 평탄화하여 모든 SQL 파일의 절대 경로 리스트 반환.
    rglob("*.sql") 대체용 — 캐시된 tree를 재활용하여 파일시스템 재순회 회피."""
    result: list[Path] = []
    for fname in tree.get("__files__", []):
        result.append(sql_dir / prefix / fname if prefix else sql_dir / fname)
    for key, sub in tree.items():
        if key == "__files__" or key == "__root__":
            continue
        child_prefix = f"{prefix}/{key}" if prefix else key
        result.extend(flatten_sql_tree(sql_dir, sub, child_prefix))
    return result
