"""sql_utils 단위 테스트 — render_sql, detect_used_params, sort_sql_files 등."""
from pathlib import Path

import pytest
from engine.sql_utils import (
    render_sql,
    detect_used_params,
    sort_sql_files,
    strip_sql_prefix,
    resolve_table_name,
    extract_sqlname_from_csv,
    extract_params_from_csv,
    _strip_sql_comments,
    _remove_empty_param_lines,
    read_sql_file,
)


# =====================================================================
# render_sql
# =====================================================================
class TestRenderSql:

    def test_colon_param_auto_quote(self):
        """:param → 자동 싱글쿼트."""
        sql = "SELECT * FROM t WHERE ym = :critYm"
        result = render_sql(sql, {"critYm": "202301"})
        assert result == "SELECT * FROM t WHERE ym = '202301'"

    def test_dollar_param_raw(self):
        """${param} → 값 그대로 (raw)."""
        sql = "SELECT * FROM t WHERE ym = '${critYm}'"
        result = render_sql(sql, {"critYm": "202301"})
        assert result == "SELECT * FROM t WHERE ym = '202301'"

    def test_hash_param_raw(self):
        """{#param} → 값 그대로 (raw)."""
        sql = "SELECT * FROM {#tableName}"
        result = render_sql(sql, {"tableName": "my_table"})
        assert result == "SELECT * FROM my_table"

    def test_at_param_schema_prefix(self):
        """@{param} → 값이 있으면 "값." 치환."""
        sql = "SELECT * FROM @{schema}my_table"
        result = render_sql(sql, {"schema": "dw"})
        assert result == "SELECT * FROM dw.my_table"

    def test_at_param_empty_schema(self):
        """@{param} → 빈 문자열이면 제거."""
        sql = "SELECT * FROM @{schema}my_table"
        result = render_sql(sql, {"schema": ""})
        assert result == "SELECT * FROM my_table"

    def test_colon_in_literal_not_replaced(self):
        """:param이 문자열 리터럴('...') 안에 있으면 치환하지 않음."""
        sql = "SELECT * FROM t WHERE ts > TO_DATE(:dt, 'YYYY:MM:DD')"
        result = render_sql(sql, {"dt": "20230101"})
        assert "'YYYY:MM:DD'" in result
        assert "'20230101'" in result

    def test_double_colon_not_replaced(self):
        """::type 캐스트는 치환 대상 아님."""
        sql = "SELECT col::int FROM t WHERE ym = :ym"
        result = render_sql(sql, {"ym": "202301"})
        assert "::int" in result
        assert "'202301'" in result

    def test_no_params(self):
        """params 없으면 원문 그대로."""
        sql = "SELECT 1"
        assert render_sql(sql, {}) == sql
        assert render_sql(sql, None) == sql

    def test_single_quote_escape(self):
        """값에 싱글쿼트 포함 시 이스케이프."""
        sql = "SELECT * FROM t WHERE name = :name"
        result = render_sql(sql, {"name": "O'Brien"})
        assert "'O''Brien'" in result

    def test_multiple_params(self):
        """여러 파라미터 동시 치환."""
        sql = "WHERE ym = :ym AND code = :code"
        result = render_sql(sql, {"ym": "202301", "code": "A1"})
        assert "'202301'" in result
        assert "'A1'" in result

    def test_longer_key_first(self):
        """긴 이름부터 치환 (부분 매칭 방지): :critYm vs :crit."""
        sql = "WHERE a = :crit AND b = :critYm"
        result = render_sql(sql, {"crit": "X", "critYm": "202301"})
        assert "'X'" in result
        assert "'202301'" in result


# =====================================================================
# _remove_empty_param_lines (빈 파라미터 라인 제거)
# =====================================================================
class TestRemoveEmptyParamLines:

    def test_empty_param_line_removed(self):
        """빈 파라미터가 있는 라인 제거."""
        sql = "SELECT * FROM t\nWHERE 1=1\n  AND ym = :ym\n  AND region = :region"
        result, params = _remove_empty_param_lines(sql, {"ym": "202301", "region": ""})
        assert "AND region" not in result
        assert "AND ym = :ym" in result
        assert "region" not in params

    def test_all_params_filled(self):
        """모든 파라미터에 값이 있으면 변화 없음."""
        sql = "WHERE 1=1\n  AND ym = :ym\n  AND region = :region"
        result, params = _remove_empty_param_lines(sql, {"ym": "202301", "region": "KR"})
        assert "AND ym" in result
        assert "AND region" in result
        assert len(params) == 2

    def test_mixed_line_kept(self):
        """빈 파라미터 + 비어있지 않은 파라미터가 같은 라인 → 유지."""
        sql = "WHERE ym = :ym AND region = :region"
        result, params = _remove_empty_param_lines(sql, {"ym": "202301", "region": ""})
        assert "WHERE ym = :ym AND region = :region" in result

    def test_dollar_param_empty(self):
        """${param}도 빈값이면 라인 제거."""
        sql = "WHERE 1=1\n  AND code IN (${codeList})"
        result, params = _remove_empty_param_lines(sql, {"codeList": ""})
        assert "code IN" not in result

    def test_hash_param_empty(self):
        """{#param}도 빈값이면 라인 제거."""
        sql = "INSERT INTO {#tbl} VALUES (1)\nSELECT 1"
        result, params = _remove_empty_param_lines(sql, {"tbl": ""})
        assert "INSERT" not in result
        assert "SELECT 1" in result

    def test_at_param_not_affected(self):
        """@{param}은 라인 제거 대상 아님 (기존 동작 유지)."""
        sql = "SELECT * FROM @{schema}my_table"
        result, params = _remove_empty_param_lines(sql, {"schema": ""})
        assert "@{schema}my_table" in result  # 라인 유지됨

    def test_no_empty_params(self):
        """빈 파라미터 없으면 원본 그대로."""
        sql = "SELECT :a"
        result, params = _remove_empty_param_lines(sql, {"a": "1"})
        assert result == sql

    def test_whitespace_only_is_empty(self):
        """공백만 있는 값도 빈값 취급."""
        sql = "WHERE 1=1\n  AND ym = :ym"
        result, params = _remove_empty_param_lines(sql, {"ym": "  "})
        assert "AND ym" not in result

    def test_render_sql_integration(self):
        """render_sql과 통합: 빈 파라미터 라인 제거 후 나머지 치환."""
        sql = "SELECT * FROM t\nWHERE 1=1\n  AND ym = :ym\n  AND region = :region"
        result = render_sql(sql, {"ym": "202301", "region": ""})
        assert result == "SELECT * FROM t\nWHERE 1=1\n  AND ym = '202301'"

    def test_double_colon_not_confused(self):
        """::cast가 빈 파라미터로 오인되지 않음."""
        sql = "SELECT col::int FROM t\nWHERE ym = :ym"
        result, params = _remove_empty_param_lines(sql, {"ym": ""})
        assert "col::int" in result  # ::int 라인 유지
        assert "WHERE ym" not in result


# =====================================================================
# detect_used_params
# =====================================================================
class TestDetectUsedParams:

    def test_colon_param(self):
        sql = "SELECT * FROM t WHERE ym = :critYm"
        used = detect_used_params(sql, {"critYm": "202301", "unused": "val"})
        assert used == {"critYm"}

    def test_dollar_param(self):
        sql = "SELECT * FROM ${schema}.t"
        used = detect_used_params(sql, {"schema": "dw", "unused": "val"})
        assert used == {"schema"}

    def test_hash_param(self):
        sql = "INSERT INTO {#tableName} VALUES (1)"
        used = detect_used_params(sql, {"tableName": "t1"})
        assert used == {"tableName"}

    def test_at_param(self):
        sql = "SELECT * FROM @{schema}table1"
        used = detect_used_params(sql, {"schema": "dw"})
        assert used == {"schema"}

    def test_comment_ignored(self):
        """-- 주석 안의 :param은 무시."""
        sql = "-- WHERE ym = :critYm\nSELECT 1"
        used = detect_used_params(sql, {"critYm": "val"})
        assert used == set()

    def test_string_literal_ignored(self):
        """'...' 리터럴 안의 :param은 무시."""
        sql = "SELECT * FROM t WHERE fmt = ':critYm'"
        used = detect_used_params(sql, {"critYm": "val"})
        assert used == set()

    def test_multiple_syntaxes(self):
        """여러 문법 혼합."""
        sql = "SELECT @{schema}t.* FROM t WHERE ym = :ym AND name = '${name}'"
        params = {"schema": "dw", "ym": "202301", "name": "test", "unused": "x"}
        used = detect_used_params(sql, params)
        assert used == {"schema", "ym", "name"}

    def test_no_params_used(self):
        sql = "SELECT 1"
        used = detect_used_params(sql, {"k": "v"})
        assert used == set()


# =====================================================================
# sort_sql_files
# =====================================================================
class TestSortSqlFiles:

    def test_numeric_prefix_sort(self, tmp_path):
        """숫자 접두사 기준 정렬."""
        (tmp_path / "02_second.sql").write_text("", encoding="utf-8")
        (tmp_path / "01_first.sql").write_text("", encoding="utf-8")
        (tmp_path / "03_third.sql").write_text("", encoding="utf-8")
        result = sort_sql_files(tmp_path)
        names = [f.name for f in result]
        assert names == ["01_first.sql", "02_second.sql", "03_third.sql"]

    def test_alpha_sort_no_prefix(self, tmp_path):
        """접두사 없으면 알파벳 정렬."""
        (tmp_path / "beta.sql").write_text("", encoding="utf-8")
        (tmp_path / "alpha.sql").write_text("", encoding="utf-8")
        result = sort_sql_files(tmp_path)
        names = [f.name for f in result]
        assert names == ["alpha.sql", "beta.sql"]

    def test_empty_dir(self, tmp_path):
        assert sort_sql_files(tmp_path) == []


# =====================================================================
# strip_sql_prefix
# =====================================================================
class TestStripSqlPrefix:

    def test_with_prefix(self):
        assert strip_sql_prefix("01_contract") == "contract"

    def test_without_prefix(self):
        assert strip_sql_prefix("contract") == "contract"

    def test_multiple_digits(self):
        assert strip_sql_prefix("123_big_table") == "big_table"

    def test_dot_space_prefix(self):
        """'3. qpv_005' 형태 접두사 제거."""
        assert strip_sql_prefix("3. qpv_005") == "qpv_005"

    def test_dot_no_space_prefix(self):
        """'3.qpv_005' 형태."""
        assert strip_sql_prefix("3.qpv_005") == "qpv_005"

    def test_dash_prefix(self):
        """'01-contract' 형태."""
        assert strip_sql_prefix("01-contract") == "contract"


# =====================================================================
# resolve_table_name
# =====================================================================
class TestResolveTableName:

    def test_hint_present(self, tmp_path):
        """--[table_name] 힌트가 있으면 그 값."""
        f = tmp_path / "01_query.sql"
        f.write_text("--[custom_table]\nSELECT 1", encoding="utf-8")
        assert resolve_table_name(f) == "custom_table"

    def test_no_hint(self, tmp_path):
        """힌트 없으면 파일 stem 반환."""
        f = tmp_path / "my_query.sql"
        f.write_text("SELECT 1", encoding="utf-8")
        assert resolve_table_name(f) == "my_query"

    def test_empty_lines_before_hint(self, tmp_path):
        """빈 줄 후에 힌트."""
        f = tmp_path / "q.sql"
        f.write_text("\n\n--[target_tbl]\nSELECT 1", encoding="utf-8")
        assert resolve_table_name(f) == "target_tbl"


# =====================================================================
# extract_sqlname_from_csv / extract_params_from_csv
# =====================================================================
class TestCsvNaming:

    def test_extract_sqlname(self):
        p = Path("01_a1__local__clsYymm_202003.csv")
        assert extract_sqlname_from_csv(p) == "01_a1"

    def test_extract_sqlname_gz(self):
        p = Path("01_a1__local__clsYymm_202003.csv.gz")
        assert extract_sqlname_from_csv(p) == "01_a1"

    def test_extract_params(self):
        p = Path("a1__local__clsYymm_202003__productCode_LA0001.csv.gz")
        params = extract_params_from_csv(p)
        assert params == {"clsYymm": "202003", "productCode": "LA0001"}

    def test_extract_params_no_params(self):
        p = Path("a1__local.csv")
        params = extract_params_from_csv(p)
        assert params == {}


# =====================================================================
# _strip_sql_comments
# =====================================================================
class TestStripSqlComments:

    def test_removes_line_comments(self):
        sql = "-- comment\nSELECT 1\n  -- indented comment\nFROM t"
        result = _strip_sql_comments(sql)
        assert "-- comment" not in result
        assert "SELECT 1" in result
        assert "FROM t" in result

    def test_preserves_non_comment(self):
        sql = "SELECT '--not-a-comment' FROM t"
        result = _strip_sql_comments(sql)
        assert "SELECT" in result


# =====================================================================
# read_sql_file (인코딩 자동 감지)
# =====================================================================
class TestReadSqlFile:

    def test_utf8(self, tmp_path):
        """UTF-8 파일 정상 읽기."""
        f = tmp_path / "test.sql"
        f.write_bytes("SELECT '한글' FROM t".encode("utf-8"))
        assert "한글" in read_sql_file(f)

    def test_cp949(self, tmp_path):
        """CP949(EUC-KR) 인코딩 파일도 읽기."""
        f = tmp_path / "test.sql"
        f.write_bytes("-- 계약 테이블\nSELECT 1".encode("cp949"))
        result = read_sql_file(f)
        assert "계약 테이블" in result
        assert "SELECT 1" in result

    def test_euc_kr(self, tmp_path):
        """EUC-KR 인코딩 파일도 읽기."""
        f = tmp_path / "test.sql"
        f.write_bytes("-- 보험료\nSELECT 1".encode("euc-kr"))
        result = read_sql_file(f)
        assert "보험료" in result

    def test_ascii(self, tmp_path):
        """ASCII 파일은 UTF-8으로 문제 없이 읽힘."""
        f = tmp_path / "test.sql"
        f.write_text("SELECT 1 FROM dual", encoding="ascii")
        assert "SELECT 1" in read_sql_file(f)
