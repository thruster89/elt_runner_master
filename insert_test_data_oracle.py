"""
ELT Runner 통합 테스트 — Oracle에 테스트 데이터 INSERT
generate_test_data.py가 생성한 CSV를 읽어서 Oracle에 bulk insert.

사전 조건:
  1. python generate_test_data.py  (CSV 22개 생성)
  2. Oracle에 sql/ddl/test_oracle_ddl.sql 실행 (테이블 10개 생성)

사용법:
  python insert_test_data_oracle.py

환경변수 또는 아래 CONNECTION 상수를 수정하세요.
"""

import csv
import os
from pathlib import Path
from datetime import datetime

import oracledb

# ── Oracle 접속 정보 (환경변수 또는 직접 수정) ──
USER = os.getenv("ORA_USER", "tester")
PASSWORD = os.getenv("ORA_PASSWORD", "aa12345")
DSN = os.getenv("ORA_DSN", "localhost:1521/orclpdb")

CSV_DIR = Path("data/export/test/test_insurance")
BATCH_SIZE = 5000  # executemany batch size


def get_conn():
    return oracledb.connect(user=USER, password=PASSWORD, dsn=DSN)


def count_table(cur, table):
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]


def load_csv(conn, csv_file: Path, table: str,
             date_cols: list[int] = None,
             num_cols: list[int] = None,
             float_cols: list[int] = None):
    """CSV 파일을 읽어서 Oracle 테이블에 bulk insert."""
    date_cols = date_cols or []
    num_cols = num_cols or []
    float_cols = float_cols or []

    with open(csv_file, encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        ncols = len(headers)
        bind = ", ".join([f":{i+1}" for i in range(ncols)])
        sql = f"INSERT INTO {table} ({', '.join(headers)}) VALUES ({bind})"

        cur = conn.cursor()
        batch = []
        total = 0

        for row in reader:
            converted = []
            for i, val in enumerate(row):
                if i in date_cols:
                    converted.append(datetime.strptime(val, "%Y-%m-%d") if val else None)
                elif i in float_cols:
                    converted.append(float(val))
                elif i in num_cols:
                    converted.append(int(val))
                else:
                    converted.append(val)
            batch.append(converted)

            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                total += len(batch)
                batch = []

        if batch:
            cur.executemany(sql, batch)
            total += len(batch)

        conn.commit()
        cur.close()

    after = count_table(conn.cursor(), table)
    print(f"  {table:<25s} +{total:>8,}행  (총 {after:,})")


# ── 테이블별 CSV 매핑 ──
# (csv glob pattern, oracle table, date_cols, num_cols, float_cols)
#
# 컬럼 인덱스 기준:
#   date_cols  → datetime 변환 (DATE)
#   num_cols   → int 변환 (NUMBER)
#   float_cols → float 변환 (NUMBER with decimals)
#   나머지     → str 그대로 (VARCHAR2)
TABLE_MAP = [
    # 마스터
    # TB_CUSTOMER: CUSTOMER_ID(0) CUSTOMER_NAME(1) BIRTH_DATE(2) GENDER(3) REGION(4)
    ("02_customer__*.csv",       "TB_CUSTOMER",       [2], [], []),
    # TB_PRODUCT: PRODUCT_CD(0) PRODUCT_NAME(1) CATEGORY(2) MIN_AMT(3) MAX_AMT(4)
    ("03_product__*.csv",        "TB_PRODUCT",        [], [3, 4], []),
    # TB_BRANCH: BRANCH_CD(0) BRANCH_NAME(1) REGION(2)
    ("07_branch__*.csv",         "TB_BRANCH",         [], [], []),
    # TB_AGENT: AGENT_ID(0) AGENT_NAME(1) BRANCH_CD(2) JOIN_DATE(3)
    ("06_agent__*.csv",          "TB_AGENT",          [3], [], []),
    # TB_RATE: PRODUCT_CD(0) RATE_CODE(1) RATE_VALUE(2) EFFECTIVE_DATE(3)
    ("09_rate__*.csv",           "TB_RATE",           [3], [], [2]),
    # TB_CODE: CODE_TYPE(0) CODE(1) CODE_NAME(2) — 전부 VARCHAR2
    ("10_code__*.csv",           "TB_CODE",           [], [], []),
    # 분기별
    # TB_CONTRACT: CONTRACT_ID(0) CUSTOMER_ID(1) PRODUCT_CD(2) CONTRACT_DATE(3) CLS_YYMM(4) STATUS(5) CONTRACT_AMT(6)
    ("01_contract__*__clsYymm_*.csv",      "TB_CONTRACT",       [3], [6], []),
    # TB_PAYMENT: PAYMENT_ID(0) CONTRACT_ID(1) PAYMENT_DATE(2) PAYMENT_AMT(3) PAYMENT_TYPE(4)
    ("04_payment__*__clsYymm_*.csv",       "TB_PAYMENT",        [2], [3], []),
    # TB_CLAIM: CLAIM_ID(0) CONTRACT_ID(1) CLAIM_DATE(2) CLAIM_AMT(3) CLAIM_TYPE(4) STATUS(5)
    ("05_claim__*__clsYymm_*.csv",         "TB_CLAIM",          [2], [3], []),
    # TB_CONTRACT_AGENT: CONTRACT_ID(0) AGENT_ID(1) — 전부 VARCHAR2
    ("08_contract_agent__*__clsYymm_*.csv","TB_CONTRACT_AGENT", [], [], []),
]


def main():
    print(f"Oracle 접속: {USER}@{DSN}")
    conn = get_conn()
    print("접속 성공\n")

    for glob_pat, table, date_cols, num_cols, float_cols in TABLE_MAP:
        files = sorted(CSV_DIR.glob(glob_pat))
        if not files:
            print(f"  [SKIP] {table} — CSV 없음 ({glob_pat})")
            continue
        for f in files:
            load_csv(conn, f, table, date_cols, num_cols, float_cols)

    # 최종 요약
    print(f"\n{'='*50}")
    cur = conn.cursor()
    grand_total = 0
    for _, table, *_ in TABLE_MAP:
        cnt = count_table(cur, table)
        grand_total += cnt
        print(f"  {table:<25s} {cnt:>10,}행")
    print(f"  {'─'*36}")
    print(f"  {'합계':<25s} {grand_total:>10,}행")
    cur.close()
    conn.close()
    print("\n완료. 이제 export 포함 파이프라인 실행:")
    print("  python runner.py --job jobs/test_insurance.yml")


if __name__ == "__main__":
    main()
