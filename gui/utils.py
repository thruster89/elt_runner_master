"""
gui/utils.py  ─  순수 유틸리티 함수 (프로젝트 데이터 읽기)
"""

import logging
import re
import yaml
from pathlib import Path

_log = logging.getLogger(__name__)


def load_jobs(work_dir: Path) -> dict:
    """jobs/ 폴더의 *.yml 파싱 → {filename: parsed_dict}"""
    jobs = {}
    jobs_dir = work_dir / "jobs"
    if jobs_dir.exists():
        for f in sorted(jobs_dir.glob("*.yml")):
            try:
                data = yaml.safe_load(f.read_text(encoding="utf-8"))
                jobs[f.name] = data
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


def scan_sql_params(sql_dir: Path) -> list[str]:
    """
    sql_dir 하위 .sql 파일 전체 스캔,
    :param  {#param}  ${param} 세 가지 패턴으로 파라미터 이름 추출 → 정렬된 리스트 반환.
    싱글쿼트 문자열 리터럴 내부의 :word 는 파라미터로 인식하지 않음.
    sql_dir 의 부모(workdir/sql/)에서 transform/, report/ 도 함께 스캔.
    """
    found: set[str] = set()
    if not sql_dir.exists():
        return []

    # sql_dir 외에 transform/, report/ sql 폴더도 자동 포함
    scan_dirs = [sql_dir]
    sql_root = sql_dir.parent  # 보통 workdir/sql/
    for extra in ("transform", "report"):
        extra_dir = sql_root / extra
        if extra_dir.exists() and extra_dir not in scan_dirs:
            scan_dirs.append(extra_dir)

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


def collect_sql_tree(sql_dir: Path) -> dict:
    """
    sql_dir 하위 폴더/파일 트리 반환
    {
      "export": {
          "__files__": ["01_contract.sql", "02_payment.sql"],
          "A": {"__files__": ["a1.sql", "a2.sql"]},
          "B": {"__files__": ["rate.sql"]},
      },
      ...
    }
    """
    def _walk(path: Path) -> dict:
        node = {"__files__": []}
        for item in sorted(path.iterdir()):
            if item.is_file() and item.suffix.lower() == ".sql":
                node["__files__"].append(item.name)
            elif item.is_dir():
                node[item.name] = _walk(item)
        return node

    if not sql_dir.exists():
        return {}
    tree = {"__files__": []}
    for item in sorted(sql_dir.iterdir()):
        if item.is_dir():
            tree[item.name] = _walk(item)
        elif item.is_file() and item.suffix.lower() == ".sql":
            tree["__files__"].append(item.name)
    return tree
