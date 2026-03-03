"""engine/validator.py 단위 테스트."""
import logging
from pathlib import Path

import pytest
from engine.context import RunContext
from engine.validator import validate_pre_run


def _ctx(job_config, env_config=None, params=None, stage_filter=None, work_dir=None):
    """테스트용 RunContext 생성 헬퍼."""
    return RunContext(
        job_name="test_job",
        run_id="test_001",
        job_config=job_config,
        env_config=env_config or {},
        params=params or {},
        work_dir=work_dir or Path("/tmp/test"),
        mode="run",
        logger=logging.getLogger("test"),
        stage_filter=stage_filter or [],
    )


# =====================================================================
# Source 검증
# =====================================================================
class TestValidateSource:

    def test_missing_host(self):
        """source.host 없으면 에러."""
        ctx = _ctx(
            job_config={"source": {"type": "oracle"}, "export": {"sql_dir": "sql"}},
            env_config={"sources": {"oracle": {"hosts": {}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("source.host" in e for e in errors)

    def test_oracle_host_not_in_env(self):
        """env.yml에 Oracle 호스트 미존재."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "prod"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {"oracle": {"hosts": {"dev": {}}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("prod" in e for e in errors)

    def test_oracle_missing_fields(self):
        """Oracle 호스트에 user/password/dsn 누락."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": "sql"},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {"user": "u"}}}}
            },
        )
        errors, _ = validate_pre_run(ctx)
        assert any("password" in e for e in errors)
        assert any("dsn" in e for e in errors)

    def test_oracle_valid(self, tmp_path):
        """올바른 Oracle 설정 → 에러 없음."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "test.sql").write_text("SELECT 1", encoding="utf-8")
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": str(sql_dir)},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "localhost/xe"
                }}}}
            },
        )
        errors, _ = validate_pre_run(ctx)
        assert errors == []

    def test_vertica_host_not_in_env(self):
        """Vertica 호스트 미존재."""
        ctx = _ctx(
            job_config={
                "source": {"type": "vertica", "host": "vhost"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {"vertica": {"hosts": {}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("vhost" in e for e in errors)

    def test_missing_oracle_section(self):
        """env.yml에 sources.oracle 자체가 없음."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("sources.oracle" in e for e in errors)

    def test_unknown_source_type(self, tmp_path):
        """알 수 없는 소스 타입 → 경고."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "t.sql").write_text("SELECT 1", encoding="utf-8")
        ctx = _ctx(
            job_config={
                "source": {"type": "mysql", "host": "h"},
                "export": {"sql_dir": str(sql_dir)},
            },
            env_config={},
        )
        _, warnings = validate_pre_run(ctx)
        assert any("mysql" in w for w in warnings)


# =====================================================================
# Target 검증
# =====================================================================
class TestValidateTarget:

    def test_unsupported_target_type(self):
        """잘못된 target 타입 → 에러."""
        ctx = _ctx(
            job_config={"target": {"type": "postgres"}},
            stage_filter=["load"],
        )
        errors, _ = validate_pre_run(ctx)
        assert any("postgres" in e for e in errors)

    def test_valid_duckdb_target(self):
        """duckdb 타입 → 에러 없음."""
        ctx = _ctx(
            job_config={"target": {"type": "duckdb"}},
            stage_filter=["load"],
        )
        errors, _ = validate_pre_run(ctx)
        assert not any("target" in e.lower() for e in errors)

    def test_no_target_config(self):
        """target 없어도 에러 아님 (스테이지에서 skip)."""
        ctx = _ctx(job_config={}, stage_filter=["load"])
        errors, _ = validate_pre_run(ctx)
        assert errors == []


# =====================================================================
# 경로 검증
# =====================================================================
class TestValidatePaths:

    def test_export_sql_dir_missing(self, tmp_path):
        """export.sql_dir가 존재하지 않는 경로 → 에러."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": str(tmp_path / "nonexistent")},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
        )
        errors, _ = validate_pre_run(ctx)
        assert any("sql_dir" in e and "존재하지 않" in e for e in errors)

    def test_export_sql_dir_empty(self, tmp_path):
        """sql_dir 존재하지만 SQL 파일 없음 → 경고."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": str(sql_dir)},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
        )
        _, warnings = validate_pre_run(ctx)
        assert any("SQL 파일이 없" in w for w in warnings)

    def test_export_sql_dir_not_configured(self):
        """export.sql_dir 미설정 → 에러."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
        )
        errors, _ = validate_pre_run(ctx)
        assert any("sql_dir" in e and "설정되지 않" in e for e in errors)

    def test_transform_sql_dir_missing(self, tmp_path):
        """transform.sql_dir 미존재 → 경고."""
        ctx = _ctx(
            job_config={"transform": {"sql_dir": str(tmp_path / "no_dir")}},
            stage_filter=["transform"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("Transform" in w for w in warnings)

    def test_valid_paths(self, tmp_path):
        """모두 정상 → 에러/경고 없음."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "01_test.sql").write_text("SELECT 1", encoding="utf-8")
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": str(sql_dir)},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
        )
        errors, warnings = validate_pre_run(ctx)
        assert errors == []
        assert warnings == []


# =====================================================================
# 파라미터 검증
# =====================================================================
class TestValidateParams:

    def test_invalid_range_format(self):
        """YYYYMM:YYYYMM 아닌 범위 → 경고."""
        ctx = _ctx(
            job_config={"export": {"params": {"ym": "2023:2024"}}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("YYYYMM" in w for w in warnings)

    def test_reversed_range(self):
        """시작값 > 끝값 → 경고."""
        ctx = _ctx(
            job_config={"export": {"params": {"ym": "202312:202301"}}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("시작값" in w for w in warnings)

    def test_valid_range(self):
        """정상 범위 → 경고 없음."""
        ctx = _ctx(
            job_config={"export": {"params": {"ym": "202301:202312"}}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert warnings == []

    def test_pipe_param_no_warning(self):
        """파이프 파라미터는 별도 검증 없음."""
        ctx = _ctx(
            job_config={"export": {"params": {"ym": "202301|202302"}}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert warnings == []

    def test_range_with_filter_option(self):
        """범위+필터(~Q) → 정상."""
        ctx = _ctx(
            job_config={"export": {"params": {"ym": "202301:202312~Q"}}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert warnings == []


# =====================================================================
# stage_filter 연동
# =====================================================================
class TestStageFilter:

    def test_export_only_skips_target(self):
        """export만 필터링하면 target 검증 안 함."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": "sql"},
                "target": {"type": "invalid_type"},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
            stage_filter=["export"],
        )
        errors, _ = validate_pre_run(ctx)
        # target 검증은 스킵되므로 "invalid_type" 에러 없어야 함
        assert not any("invalid_type" in e for e in errors)

    def test_no_filter_checks_all(self):
        """필터 없으면 전체 검증."""
        ctx = _ctx(
            job_config={
                "source": {"type": "oracle", "host": "dev"},
                "export": {"sql_dir": "sql"},
                "target": {"type": "invalid_type"},
            },
            env_config={
                "sources": {"oracle": {"hosts": {"dev": {
                    "user": "u", "password": "p", "dsn": "d"
                }}}}
            },
        )
        errors, _ = validate_pre_run(ctx)
        assert any("invalid_type" in e for e in errors)


# =====================================================================
# Vertica 필수 필드 검증
# =====================================================================
class TestValidateVerticaFields:

    def test_vertica_missing_host_field(self):
        """Vertica 호스트에 host 필드 누락 → 에러."""
        ctx = _ctx(
            job_config={
                "source": {"type": "vertica", "host": "vhost"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {"vertica": {"hosts": {"vhost": {
                "user": "u", "database": "db",
            }}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("host" in e and "vhost" in e for e in errors)

    def test_vertica_missing_user_field(self):
        """Vertica 호스트에 user 필드 누락 → 에러."""
        ctx = _ctx(
            job_config={
                "source": {"type": "vertica", "host": "vhost"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {"vertica": {"hosts": {"vhost": {
                "host": "10.0.0.1", "database": "db",
            }}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("user" in e for e in errors)

    def test_vertica_missing_database_field(self):
        """Vertica 호스트에 database 필드 누락 → 에러."""
        ctx = _ctx(
            job_config={
                "source": {"type": "vertica", "host": "vhost"},
                "export": {"sql_dir": "sql"},
            },
            env_config={"sources": {"vertica": {"hosts": {"vhost": {
                "host": "10.0.0.1", "user": "u",
            }}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert any("database" in e for e in errors)

    def test_vertica_valid(self, tmp_path):
        """올바른 Vertica 설정 → 에러 없음."""
        sql_dir = tmp_path / "sql"
        sql_dir.mkdir()
        (sql_dir / "test.sql").write_text("SELECT 1", encoding="utf-8")
        ctx = _ctx(
            job_config={
                "source": {"type": "vertica", "host": "vhost"},
                "export": {"sql_dir": str(sql_dir)},
            },
            env_config={"sources": {"vertica": {"hosts": {"vhost": {
                "host": "10.0.0.1", "user": "u", "database": "db",
            }}}}},
        )
        errors, _ = validate_pre_run(ctx)
        assert errors == []


# =====================================================================
# 스테이지 설정값 범위 검증
# =====================================================================
class TestValidateStageOptions:

    def test_parallel_workers_negative(self):
        """export.parallel_workers 음수 → 에러."""
        ctx = _ctx(
            job_config={"export": {"parallel_workers": -1, "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        errors, _ = validate_pre_run(ctx)
        assert any("parallel_workers" in e for e in errors)

    def test_parallel_workers_zero(self):
        """export.parallel_workers 0 → 에러."""
        ctx = _ctx(
            job_config={"export": {"parallel_workers": 0, "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        errors, _ = validate_pre_run(ctx)
        assert any("parallel_workers" in e for e in errors)

    def test_parallel_workers_valid(self):
        """export.parallel_workers 양수 → 에러 없음."""
        ctx = _ctx(
            job_config={"export": {"parallel_workers": 4, "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        errors, _ = validate_pre_run(ctx)
        assert not any("parallel_workers" in e for e in errors)

    def test_parallel_workers_string(self):
        """export.parallel_workers 문자열 → 에러."""
        ctx = _ctx(
            job_config={"export": {"parallel_workers": "abc", "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        errors, _ = validate_pre_run(ctx)
        assert any("parallel_workers" in e for e in errors)

    def test_compression_invalid(self):
        """export.compression 유효하지 않은 값 → 경고."""
        ctx = _ctx(
            job_config={"export": {"compression": "bzip2", "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("compression" in w for w in warnings)

    def test_compression_valid(self):
        """export.compression gzip → 경고 없음."""
        ctx = _ctx(
            job_config={"export": {"compression": "gzip", "sql_dir": "sql"}},
            stage_filter=["export"],
        )
        _, warnings = validate_pre_run(ctx)
        assert not any("compression" in w for w in warnings)

    def test_load_mode_invalid(self):
        """load.mode 유효하지 않은 값 → 경고."""
        ctx = _ctx(
            job_config={"load": {"mode": "upsert"}},
            stage_filter=["load"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("load.mode" in w for w in warnings)

    def test_load_mode_valid(self):
        """load.mode delete → 경고 없음."""
        ctx = _ctx(
            job_config={"load": {"mode": "delete"}},
            stage_filter=["load"],
        )
        _, warnings = validate_pre_run(ctx)
        assert not any("load.mode" in w for w in warnings)

    def test_on_error_invalid(self):
        """transform.on_error 유효하지 않은 값 → 경고."""
        ctx = _ctx(
            job_config={"transform": {"on_error": "ignore"}},
            stage_filter=["transform"],
        )
        _, warnings = validate_pre_run(ctx)
        assert any("on_error" in w for w in warnings)

    def test_on_error_valid(self):
        """transform.on_error continue → 경고 없음."""
        ctx = _ctx(
            job_config={"transform": {"on_error": "continue"}},
            stage_filter=["transform"],
        )
        _, warnings = validate_pre_run(ctx)
        assert not any("on_error" in w for w in warnings)
