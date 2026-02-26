# file: v2/engine/sql_utils.py

import re
from pathlib import Path

SQL_PREFIX_PATTERN = re.compile(r"^(\d+)_.*\.sql$", re.IGNORECASE)
TABLE_HINT_PATTERN = re.compile(r"^--\[(.+)\]$")


def sort_sql_files(sql_dir: Path):
    files = list(sql_dir.glob("*.sql"))

    if not files:
        return []

    parsed = []
    prefix_found = False

    for f in files:
        m = SQL_PREFIX_PATTERN.match(f.name)
        if m:
            prefix_found = True
            order = int(m.group(1))
            parsed.append((order, f))
        else:
            parsed.append((None, f))

    if prefix_found:
        parsed.sort(key=lambda x: (x[0] is None, x[0], x[1].name))
        return [f for _, f in parsed]

    return sorted(files, key=lambda f: f.name.lower())


def resolve_table_name(sql_file: Path) -> str:
    """
    SQL 첫 줄(정확히는 첫 non-empty line)에 --[table_name] 이 있으면 그 값을 테이블명으로 사용.
    없으면 sql_file.stem 사용.
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue

            m = TABLE_HINT_PATTERN.match(s)
            if m:
                return m.group(1).strip()

            break

    return sql_file.stem


def extract_sqlname_from_csv(csv_path: Path) -> str:
    """
    csv 파일명 규칙: {sqlname}__{host}__{param}_{value}...
    여기서 sqlname은 첫 '__' 이전.

    .csv.gz의 경우 Path.stem이 '파일명.csv'가 되므로
    확장자를 직접 제거한 뒤 처리.
    """
    name = csv_path.name  # ex) 01_a1__local__clsYymm_202003.csv.gz

    # .csv.gz / .csv 둘 다 처리
    if name.endswith(".csv.gz"):
        stem = name[: -len(".csv.gz")]
    elif name.endswith(".csv"):
        stem = name[: -len(".csv")]
    else:
        stem = csv_path.stem

    return stem.split("__", 1)[0]


def extract_params_from_csv(csv_path: Path) -> dict:
    """
    CSV 파일명에서 파라미터 key=value를 추출.
    파일명 규칙: {sqlname}__{host}__{param1}_{value1}__{param2}_{value2}...
    예: a1__local__clsYymm_202003__productCode_LA0001__rateCode_0000.csv.gz
    → {"clsYymm": "202003", "productCode": "LA0001", "rateCode": "0000"}
    """
    name = csv_path.name
    if name.endswith(".csv.gz"):
        stem = name[: -len(".csv.gz")]
    elif name.endswith(".csv"):
        stem = name[: -len(".csv")]
    else:
        stem = csv_path.stem

    parts = stem.split("__")
    # parts[0] = sqlname, parts[1] = host, parts[2:] = param_value pairs
    params = {}
    for part in parts[2:]:
        idx = part.find("_")
        if idx > 0:
            key = part[:idx]
            val = part[idx + 1:]
            params[key] = val
    return params


def _strip_sql_comments(sql_text: str) -> str:
    """SQL 단일행 주석(-- ...) 제거. 문자열 리터럴 내부의 --는 보존."""
    lines = sql_text.splitlines()
    result = []
    for line in lines:
        stripped = line.lstrip()
        if stripped.startswith("--"):
            continue
        result.append(line)
    return "\n".join(result)


def detect_used_params(sql_text: str, available_params: dict) -> set:
    """SQL 텍스트에서 실제 사용되는 파라미터 키 집합 반환.
    :param, ${param}, {#param}, @{param} 네 가지 문법 감지.
    단일행 주석(-- ...)과 문자열 리터럴('...' 안) 내의 파라미터는 제외."""
    sql_active = _strip_sql_comments(sql_text)
    sql_no_strings = re.sub(r"'[^']*'", "''", sql_active)
    used = set()
    for k in available_params:
        if re.search(rf'(?<![:\w]):{re.escape(k)}\b', sql_no_strings):
            used.add(k)
        if f'${{{k}}}' in sql_active:
            used.add(k)
        if f'{{#{k}}}' in sql_active:
            used.add(k)
        if f'@{{{k}}}' in sql_active:
            used.add(k)
    return used


def _split_sql_tokens(sql_text: str):
    """
    SQL 텍스트를 (token, is_literal) 쌍의 리스트로 분리.
    싱글쿼트 문자열 리터럴 내부(is_literal=True)는 :param 치환 대상에서 제외.
    연속 싱글쿼트 \'\' (escape) 처리 포함.
    """
    tokens = []
    i = 0
    n = len(sql_text)
    buf = []
    in_literal = False

    while i < n:
        ch = sql_text[i]
        if not in_literal and ch == "'":
            # 리터럴 시작 — 현재까지 버퍼 flush
            if buf:
                tokens.append(("".join(buf), False))
                buf = []
            # 리터럴 끝까지 수집
            j = i + 1
            lit = [ch]
            while j < n:
                c2 = sql_text[j]
                lit.append(c2)
                if c2 == "'" :
                    # 연속 '' 이면 escape — 리터럴 계속
                    if j + 1 < n and sql_text[j + 1] == "'":
                        lit.append(sql_text[j + 1])
                        j += 2
                        continue
                    else:
                        break
                j += 1
            tokens.append(("".join(lit), True))
            i = j + 1
        else:
            buf.append(ch)
            i += 1

    if buf:
        tokens.append(("".join(buf), False))
    return tokens


def render_sql(sql_text: str, params: dict) -> str:
    """
    SQL 텍스트에 파라미터 치환. 네 가지 문법 지원:
      ${param}  — 값 그대로 치환 (raw). 리터럴 내부 포함 전체 대상.
                  사용자가 직접 따옴표를 제어: '${setl_ym}' → '202003'
      {#param}  — ${param} 과 동일 (raw).
      @{param}  — 스키마 접두사 전용. 값이 있으면 "값." 으로 치환 (점 자동 추가),
                  값이 없거나 빈 문자열이면 통째로 제거 (빈 문자열 치환).
      :param    — 자동 싱글쿼트 감싸서 치환. 리터럴 외부에서만 동작.
                  :clsYymm → '202003'
                  ::int, 'HH24:MI:SS' 등은 치환하지 않음.
    """
    if not params:
        return sql_text

    # 긴 이름부터 치환 (부분 매칭 방지)
    keys = sorted(params.keys(), key=len, reverse=True)

    # @{param} — 스키마 접두사 치환: 값 있으면 "값.", 없으면 ""
    for k in keys:
        token = f"@{{{k}}}"
        if token in sql_text:
            v = str(params[k]).strip()
            sql_text = sql_text.replace(token, f"{v}." if v else "")

    # ${param}, {#param} 은 리터럴 내부에서도 사용할 일이 없으므로 전체 치환 (raw)
    for k in keys:
        v = str(params[k])
        sql_text = sql_text.replace(f"${{{k}}}", v)
        sql_text = sql_text.replace(f"{{#{k}}}", v)

    # :param 은 리터럴 외부에서만 치환 (자동 싱글쿼트)
    tokens = _split_sql_tokens(sql_text)
    result = []
    for token, is_literal in tokens:
        if not is_literal:
            for k in keys:
                v = str(params[k])
                quoted = "'" + v.replace("'", "''") + "'"
                token = re.sub(rf'(?<![:\w]):{re.escape(k)}\b', lambda _, q=quoted: q, token)
        result.append(token)
    return "".join(result)
