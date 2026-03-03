# file: engine/delete_utils.py
"""
_delete_by_params 공통 로직: 파라미터와 테이블 컬럼 매칭.

각 adapter(duckdb/sqlite/oracle)에서 이 함수를 호출하여
DELETE WHERE 조건에 사용할 (컬럼명, 값) 쌍을 생성한다.
"""

import logging

logger = logging.getLogger(__name__)


def build_delete_condition(
    params: dict,
    table_cols: set,
    table_display: str,
) -> tuple[list[tuple[str, str]], list[str]]:
    """
    params 키를 table_cols에 매칭하여 DELETE WHERE 조건용 데이터를 반환한다.

    매칭 규칙:
      1) 정확히 일치 (key in table_cols)
      2) 대문자 변환 후 일치 (key.upper() in table_cols)
      3) 정규화 매칭: underscore 제거 + lowercase 비교

    Returns:
        (matched, skipped)
        matched: [(컬럼명, 값), ...] — WHERE 절 구성용
        skipped: [키, ...] — 매칭 실패한 파라미터 키

    Raises:
        ValueError: params가 비어있거나 매칭되는 컬럼이 하나도 없을 때
    """
    if not params:
        raise ValueError(
            f"DELETE 모드는 params가 필수입니다: {table_display} — "
            f"전체 삭제가 필요하면 load.mode=truncate를 사용하세요."
        )

    norm_map = {col.replace("_", "").lower(): col for col in table_cols}

    matched: list[tuple[str, str]] = []
    skipped: list[str] = []

    for key, val in params.items():
        # 1) 정확히 일치
        if key in table_cols:
            matched.append((key, val))
        # 2) 대문자 변환 후 일치 (Oracle 등)
        elif key.upper() in table_cols:
            matched.append((key.upper(), val))
        else:
            # 3) 정규화 매칭
            norm_key = key.replace("_", "").lower()
            matched_col = norm_map.get(norm_key)
            if matched_col:
                logger.debug("DELETE param mapped: %s -> %s", key, matched_col)
                matched.append((matched_col, val))
            else:
                skipped.append(key)

    if not matched:
        raise ValueError(
            f"DELETE 조건 컬럼 매칭 실패: {table_display} — "
            f"params={list(params.keys())} 중 일치하는 컬럼이 없습니다. "
            f"전체 삭제가 필요하면 load.mode=truncate를 사용하세요."
        )

    if skipped:
        logger.warning("DELETE 조건에서 제외된 파라미터 (컬럼 없음): %s %s",
                        table_display, skipped)

    return matched, skipped
