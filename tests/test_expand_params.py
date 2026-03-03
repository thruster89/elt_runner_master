"""expand_params / expand_range_value 단위 테스트."""
import pytest
from stages.export_stage import expand_params, expand_range_value


# =====================================================================
# expand_range_value
# =====================================================================
class TestExpandRangeValue:
    """월 범위 확장 함수 테스트."""

    def test_single_value_no_colon(self):
        """콜론 없으면 그대로 반환."""
        assert expand_range_value("202301") == ["202301"]

    def test_basic_range(self):
        """기본 월 범위: 201801:201803 → 3개월."""
        result = expand_range_value("201801:201803")
        assert result == ["201801", "201802", "201803"]

    def test_same_start_end(self):
        """시작=끝이면 1개만."""
        assert expand_range_value("202301:202301") == ["202301"]

    def test_full_year_range(self):
        """1년 전체 범위."""
        result = expand_range_value("202301:202312")
        assert len(result) == 12
        assert result[0] == "202301"
        assert result[-1] == "202312"

    def test_cross_year_range(self):
        """연도를 넘기는 범위."""
        result = expand_range_value("202311:202402")
        assert result == ["202311", "202312", "202401", "202402"]

    # --- 옵션 필터 ---
    def test_quarterly_filter(self):
        """~Q: 분기말(03,06,09,12)만."""
        result = expand_range_value("202301:202312~Q")
        assert result == ["202303", "202306", "202309", "202312"]

    def test_half_yearly_filter(self):
        """~H: 반기말(06,12)만."""
        result = expand_range_value("202301:202312~H")
        assert result == ["202306", "202312"]

    def test_yearly_filter(self):
        """~Y: 연말(12)만."""
        result = expand_range_value("202101:202312~Y")
        assert result == ["202112", "202212", "202312"]

    def test_specific_month_filter(self):
        """~MM: 특정 월만 (예: ~3 → 3월만)."""
        result = expand_range_value("202201:202312~3")
        assert result == ["202203", "202303"]

    def test_whitespace_handling(self):
        """앞뒤 공백 처리."""
        assert expand_range_value("  202301  ") == ["202301"]
        result = expand_range_value("  202301:202303  ")
        assert result == ["202301", "202302", "202303"]


# =====================================================================
# expand_params — product 모드
# =====================================================================
class TestExpandParamsProduct:
    """카르테시안 곱 모드 테스트."""

    def test_single_value(self):
        """단일 값 파라미터 → 결과 1개."""
        result = expand_params({"a": "hello"})
        assert result == [{"a": "hello"}]

    def test_pipe_expansion(self):
        """|로 분리된 값 → 각각 확장."""
        result = expand_params({"critYm": "202001|202002"})
        assert result == [{"critYm": "202001"}, {"critYm": "202002"}]

    def test_cartesian_product(self):
        """두 파라미터의 카르테시안 곱."""
        result = expand_params({"a": "1|2", "b": "X|Y"})
        assert len(result) == 4
        expected = [
            {"a": "1", "b": "X"},
            {"a": "1", "b": "Y"},
            {"a": "2", "b": "X"},
            {"a": "2", "b": "Y"},
        ]
        assert result == expected

    def test_range_expansion(self):
        """콜론 범위 확장 (product)."""
        result = expand_params({"ym": "202301:202303"})
        assert len(result) == 3
        assert result[0] == {"ym": "202301"}
        assert result[2] == {"ym": "202303"}

    def test_comma_passthrough(self):
        """콤마는 확장 없이 그대로 통과 (SQL IN절용)."""
        result = expand_params({"codes": "A,B,C"})
        assert result == [{"codes": "A,B,C"}]

    def test_mixed_expand_and_static(self):
        """확장 파라미터 + 정적 파라미터 혼합."""
        result = expand_params({"ym": "202301|202302", "code": "FIXED"})
        assert len(result) == 2
        assert result[0] == {"ym": "202301", "code": "FIXED"}
        assert result[1] == {"ym": "202302", "code": "FIXED"}

    def test_empty_params(self):
        """빈 dict → 빈 리스트가 아니라 빈 dict 1개."""
        result = expand_params({})
        assert result == [{}]

    def test_integer_value_converted(self):
        """정수 값도 문자열로 변환."""
        result = expand_params({"n": 42})
        assert result == [{"n": "42"}]

    def test_pipe_with_spaces(self):
        """| 주변 공백 strip."""
        result = expand_params({"x": " a | b | c "})
        assert result == [{"x": "a"}, {"x": "b"}, {"x": "c"}]

    def test_range_with_filter(self):
        """범위+필터 조합."""
        result = expand_params({"ym": "202301:202312~Q"})
        assert len(result) == 4
        values = [r["ym"] for r in result]
        assert values == ["202303", "202306", "202309", "202312"]

    def test_three_way_product(self):
        """3개 파라미터 곱."""
        result = expand_params({"a": "1|2", "b": "X|Y", "c": "P|Q"})
        assert len(result) == 8  # 2 * 2 * 2


# =====================================================================
# expand_params — zip 모드
# =====================================================================
class TestExpandParamsZip:
    """위치별 1:1 매칭 모드 테스트."""

    def test_basic_zip(self):
        """같은 길이의 두 파라미터 zip."""
        result = expand_params({"a": "1|2|3", "b": "X|Y|Z"}, mode="zip")
        assert result == [
            {"a": "1", "b": "X"},
            {"a": "2", "b": "Y"},
            {"a": "3", "b": "Z"},
        ]

    def test_zip_single_values_repeated(self):
        """단일값 파라미터는 다중값 길이만큼 반복."""
        result = expand_params({"a": "1|2", "b": "FIXED"}, mode="zip")
        assert result == [
            {"a": "1", "b": "FIXED"},
            {"a": "2", "b": "FIXED"},
        ]

    def test_zip_mismatched_lengths_raises(self):
        """다중값 파라미터의 개수가 다르면 ValueError."""
        with pytest.raises(ValueError, match="zip"):
            expand_params({"a": "1|2", "b": "X|Y|Z"}, mode="zip")

    def test_zip_all_single(self):
        """모든 파라미터가 단일값이면 결과 1개."""
        result = expand_params({"a": "1", "b": "X"}, mode="zip")
        assert result == [{"a": "1", "b": "X"}]

    def test_zip_range_expansion(self):
        """범위 확장 + zip."""
        result = expand_params(
            {"ym": "202301:202303", "code": "A|B|C"},
            mode="zip"
        )
        assert len(result) == 3
        assert result[0] == {"ym": "202301", "code": "A"}
        assert result[2] == {"ym": "202303", "code": "C"}
