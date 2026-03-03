"""연결 관리 (recycle, cleanup) 테스트.

외부 DB 의존성을 mock으로 대체하여 순수 로직만 검증.
"""
import threading
from unittest.mock import MagicMock, patch

import pytest
from stages.export_stage import (
    get_thread_connection,
    _close_all_connections,
    _need_recycle,
    set_recycle_interval,
    _thread_local,
    _thread_connections,
    _conn_list_lock,
)
import stages.export_stage as _mod
import adapters.sources.oracle_client as _oc


@pytest.fixture(autouse=True)
def _reset_thread_state():
    """각 테스트 전후로 스레드 로컬 / 전역 리스트 초기화."""
    _thread_local.__dict__.clear()
    _thread_connections.clear()
    _oc._oracle_client_initialized = False
    _oc._oracle_client_mode = None
    yield
    _thread_local.__dict__.clear()
    _thread_connections.clear()
    _oc._oracle_client_initialized = False
    _oc._oracle_client_mode = None


# =====================================================================
# _need_recycle
# =====================================================================
class TestNeedRecycle:

    def test_non_oracle_never_recycle(self):
        """Oracle 아닌 소스는 recycle 불필요."""
        assert _need_recycle("vertica", 999) is False

    def test_oracle_thin_no_recycle(self):
        """Oracle thin 모드는 recycle 불필요."""
        _oc._oracle_client_mode = "thin"
        assert _need_recycle("oracle", 999) is False

    def test_oracle_thick_below_threshold(self):
        """Oracle thick이지만 아직 한도 미달."""
        _oc._oracle_client_mode = "thick"
        original = _mod._conn_recycle_interval
        try:
            _mod._conn_recycle_interval = 20
            assert _need_recycle("oracle", 19) is False
        finally:
            _mod._conn_recycle_interval = original

    def test_oracle_thick_at_threshold(self):
        """Oracle thick에서 한도 도달 → recycle 필요."""
        _oc._oracle_client_mode = "thick"
        original = _mod._conn_recycle_interval
        try:
            _mod._conn_recycle_interval = 20
            assert _need_recycle("oracle", 20) is True
        finally:
            _mod._conn_recycle_interval = original


# =====================================================================
# set_recycle_interval
# =====================================================================
class TestSetRecycleInterval:

    def test_single_worker(self):
        set_recycle_interval(1)
        assert _mod._conn_recycle_interval == 200

    def test_ten_workers(self):
        set_recycle_interval(10)
        assert _mod._conn_recycle_interval == 20

    def test_minimum_floor(self):
        """최소값 10 보장."""
        set_recycle_interval(100)
        assert _mod._conn_recycle_interval == 10

    def test_zero_workers(self):
        """0이어도 오류 없이 동작."""
        set_recycle_interval(0)
        assert _mod._conn_recycle_interval >= 10


# =====================================================================
# get_thread_connection
# =====================================================================
class TestGetThreadConnection:

    @patch("stages.export_stage._new_connection")
    def test_first_call_creates_connection(self, mock_new):
        """최초 호출 시 새 연결 생성."""
        mock_conn = MagicMock()
        mock_new.return_value = mock_conn

        result = get_thread_connection("oracle", {}, "host1")

        assert result is mock_conn
        assert mock_new.call_count == 1
        assert mock_conn in _thread_connections

    @patch("stages.export_stage._new_connection")
    def test_reuses_existing_connection(self, mock_new):
        """같은 스레드 내 재사용."""
        mock_conn = MagicMock()
        mock_new.return_value = mock_conn
        _oc._oracle_client_mode = "thin"  # recycle 안 하도록

        conn1 = get_thread_connection("oracle", {}, "host1")
        conn2 = get_thread_connection("oracle", {}, "host1")

        assert conn1 is conn2
        assert mock_new.call_count == 1

    @patch("stages.export_stage._new_connection")
    def test_recycle_creates_new(self, mock_new):
        """recycle 조건 충족 시 새 연결로 교체."""
        old_conn = MagicMock()
        new_conn = MagicMock()
        mock_new.side_effect = [old_conn, new_conn]

        _oc._oracle_client_mode = "thick"
        original = _mod._conn_recycle_interval
        try:
            _mod._conn_recycle_interval = 2  # 2회 후 recycle

            # 1회차: 새 연결 생성
            c1 = get_thread_connection("oracle", {}, "host1")
            assert c1 is old_conn
            # 2회차: use_count=1 → 아직 미달
            c2 = get_thread_connection("oracle", {}, "host1")
            assert c2 is old_conn
            # 3회차: use_count=2 → recycle!
            c3 = get_thread_connection("oracle", {}, "host1")
            assert c3 is new_conn

            # 이전 연결이 닫혔는지 확인
            old_conn.close.assert_called_once()
            # 이전 연결이 리스트에서 제거되었는지
            assert old_conn not in _thread_connections
            assert new_conn in _thread_connections
        finally:
            _mod._conn_recycle_interval = original


# =====================================================================
# _close_all_connections
# =====================================================================
class TestCloseAllConnections:

    def test_closes_all(self, logger):
        """모든 연결 닫기."""
        c1 = MagicMock()
        c2 = MagicMock()
        _thread_connections.extend([c1, c2])

        _close_all_connections(logger)

        c1.close.assert_called_once()
        c2.close.assert_called_once()
        assert len(_thread_connections) == 0

    def test_handles_close_error(self, logger):
        """닫기 실패해도 계속 진행."""
        c1 = MagicMock()
        c1.close.side_effect = Exception("close failed")
        c2 = MagicMock()
        _thread_connections.extend([c1, c2])

        _close_all_connections(logger)  # 예외 없이 완료

        c2.close.assert_called_once()
        assert len(_thread_connections) == 0
