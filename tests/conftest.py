"""공통 fixture 정의."""
import sys
import types
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ── 외부 의존 모듈 모킹 (oracledb, vertica 등) ──────────────
def _ensure_mock_modules():
    """테스트 환경에 없는 외부 DB 드라이버를 가짜 모듈로 등록."""
    for mod_name in ("oracledb", "vertica_python"):
        if mod_name not in sys.modules:
            fake = types.ModuleType(mod_name)
            # oracledb에서 사용하는 최소한의 속성
            if mod_name == "oracledb":
                fake.init_oracle_client = MagicMock()
                fake.connect = MagicMock()
                fake.is_thin_mode = MagicMock(return_value=True)
                fake.defaults = MagicMock()
            sys.modules[mod_name] = fake


_ensure_mock_modules()


@pytest.fixture
def tmp_sql_dir(tmp_path):
    """임시 SQL 디렉토리에 샘플 SQL 파일 생성."""
    d = tmp_path / "sql"
    d.mkdir()
    return d


@pytest.fixture
def make_sql_file(tmp_sql_dir):
    """SQL 파일 생성 헬퍼."""
    def _make(name: str, content: str) -> Path:
        p = tmp_sql_dir / name
        p.write_text(content, encoding="utf-8")
        return p
    return _make


@pytest.fixture
def logger():
    """테스트용 로거."""
    return logging.getLogger("test")
