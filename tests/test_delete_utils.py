"""engine/delete_utils.py 단위 테스트."""
import pytest
from engine.delete_utils import build_delete_condition


class TestBuildDeleteCondition:

    def test_exact_match(self):
        """컬럼명이 정확히 일치하는 경우."""
        matched, skipped = build_delete_condition(
            {"ym": "202301", "code": "A"},
            {"ym", "code", "name"},
            "test_table",
        )
        assert len(matched) == 2
        assert ("ym", "202301") in matched
        assert ("code", "A") in matched
        assert skipped == []

    def test_upper_match(self):
        """대문자 변환 후 매칭 (Oracle 등)."""
        matched, skipped = build_delete_condition(
            {"ym": "202301"},
            {"YM", "CODE"},
            "test_table",
        )
        assert matched == [("YM", "202301")]

    def test_normalized_match(self):
        """underscore 제거 + lowercase 매칭."""
        matched, skipped = build_delete_condition(
            {"clsYymm": "202301"},
            {"CLS_YYMM", "OTHER"},
            "test_table",
        )
        assert matched == [("CLS_YYMM", "202301")]

    def test_skipped_params(self):
        """매칭되지 않는 파라미터는 skipped에 포함."""
        matched, skipped = build_delete_condition(
            {"ym": "202301", "unknown": "X"},
            {"ym"},
            "test_table",
        )
        assert len(matched) == 1
        assert skipped == ["unknown"]

    def test_empty_params_raises(self):
        """params 빈 dict → ValueError."""
        with pytest.raises(ValueError, match="params가 필수"):
            build_delete_condition({}, {"col"}, "test_table")

    def test_no_match_raises(self):
        """매칭되는 컬럼이 하나도 없으면 ValueError."""
        with pytest.raises(ValueError, match="매칭 실패"):
            build_delete_condition(
                {"x": "1", "y": "2"},
                {"a", "b"},
                "test_table",
            )
