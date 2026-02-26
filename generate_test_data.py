"""
ELT Runner — 통합 테스트 데이터 생성
보험계약 관리 시나리오 (10개 테이블, ~25만 행)

사용법:  python generate_test_data.py
결과:    data/export/test/ 에 CSV 22개 생성

이후:    python runner.py --job jobs/test_insurance.yml
"""

import csv
import random
from pathlib import Path
from datetime import datetime, timedelta

random.seed(42)

OUT_DIR = Path("data/export/test/test_insurance")
HOST = "local"
QUARTERS = ["202303", "202306", "202309", "202312"]


def write_csv(filename, headers, rows):
    path = OUT_DIR / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"  {filename}: {len(rows):,} rows")


def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def fmt(d):
    return d.strftime("%Y-%m-%d")


# ────────────────────────────────────────
# 마스터 테이블 (파라미터 없음)
# ────────────────────────────────────────

def gen_customers(n=10000):
    regions = ["서울","경기","인천","부산","대구","광주","대전","울산","강원","충북","충남","전북","전남","경북","경남","제주"]
    last = ["김","이","박","최","정","강","조","윤","장","임","한","오","서","신","권","황","안","송","류","홍"]
    first = ["민","서","지","현","수","영","은","예","도","하","준","우","재","성","태"]
    rows = []
    for i in range(1, n + 1):
        rows.append([
            f"C{i:06d}",
            random.choice(last) + random.choice(first) + random.choice(first),
            fmt(rand_date(datetime(1950,1,1), datetime(2000,12,31))),
            random.choice(["M","F"]),
            random.choice(regions),
        ])
    write_csv(f"02_customer__{HOST}.csv",
              ["CUSTOMER_ID","CUSTOMER_NAME","BIRTH_DATE","GENDER","REGION"], rows)
    return [r[0] for r in rows]


def gen_products():
    cats = ["종신","정기","연금","건강","실손","운전자","화재","여행","저축","변액"]
    rows = []
    for i, cat in enumerate(cats):
        for j in range(1, 4):
            idx = i * 3 + j
            rows.append([
                f"P{idx:03d}", f"{cat}보험_{j}형", cat,
                random.choice([100000,200000,500000,1000000]),
                random.choice([5000000,10000000,50000000,100000000]),
            ])
    write_csv(f"03_product__{HOST}.csv",
              ["PRODUCT_CD","PRODUCT_NAME","CATEGORY","MIN_AMT","MAX_AMT"], rows)
    return [r[0] for r in rows]


def gen_branches():
    data = [
        ("BR01","서울본부","서울"),("BR02","강남지점","서울"),("BR03","서초지점","서울"),
        ("BR04","영등포지점","서울"),("BR05","마포지점","서울"),
        ("BR06","경기본부","경기"),("BR07","수원지점","경기"),("BR08","성남지점","경기"),
        ("BR09","인천지점","인천"),("BR10","부산본부","부산"),
        ("BR11","대구지점","대구"),("BR12","광주지점","광주"),
        ("BR13","대전지점","대전"),("BR14","울산지점","울산"),
        ("BR15","강원지점","강원"),("BR16","충북지점","충북"),
        ("BR17","전북지점","전북"),("BR18","전남지점","전남"),
        ("BR19","경북지점","경북"),("BR20","제주지점","제주"),
    ]
    rows = [list(b) for b in data]
    write_csv(f"07_branch__{HOST}.csv",
              ["BRANCH_CD","BRANCH_NAME","REGION"], rows)
    return [r[0] for r in rows]


def gen_agents(branch_cds, n=200):
    last = ["김","이","박","최","정","강","조","윤","장","임"]
    first = ["민","서","지","현","수","영","은","예","도","하","준"]
    rows = []
    for i in range(1, n + 1):
        rows.append([
            f"A{i:04d}",
            random.choice(last) + random.choice(first) + random.choice(first),
            random.choice(branch_cds),
            fmt(rand_date(datetime(2015,1,1), datetime(2023,6,30))),
        ])
    write_csv(f"06_agent__{HOST}.csv",
              ["AGENT_ID","AGENT_NAME","BRANCH_CD","JOIN_DATE"], rows)
    return [r[0] for r in rows]


def gen_rates(product_cds):
    rows = []
    for pcd in product_cds:
        for rc in ["0000", "0001"]:
            rows.append([pcd, rc, round(random.uniform(0.01, 0.15), 4), "2023-01-01"])
    write_csv(f"09_rate__{HOST}.csv",
              ["PRODUCT_CD","RATE_CODE","RATE_VALUE","EFFECTIVE_DATE"], rows)


def gen_codes():
    data = [
        ("STATUS","01","정상"),("STATUS","02","해지"),("STATUS","03","만기"),
        ("STATUS","04","실효"),("STATUS","05","취소"),
        ("PAY_TYPE","01","계좌이체"),("PAY_TYPE","02","카드납"),("PAY_TYPE","03","가상계좌"),
        ("CLAIM_TYPE","01","사망"),("CLAIM_TYPE","02","입원"),("CLAIM_TYPE","03","수술"),
        ("CLAIM_TYPE","04","통원"),("CLAIM_TYPE","05","진단"),
        ("CLAIM_ST","01","접수"),("CLAIM_ST","02","심사중"),("CLAIM_ST","03","지급완료"),
        ("CLAIM_ST","04","반려"),("CLAIM_ST","05","취소"),
        ("GENDER","M","남성"),("GENDER","F","여성"),
        ("REGION","11","서울"),("REGION","41","경기"),("REGION","28","인천"),
        ("REGION","26","부산"),("REGION","27","대구"),("REGION","29","광주"),
        ("REGION","30","대전"),("REGION","31","울산"),("REGION","36","세종"),
    ]
    for i in range(len(data), 50):
        data.append(("ETC", f"E{i:02d}", f"기타코드_{i}"))
    write_csv(f"10_code__{HOST}.csv",
              ["CODE_TYPE","CODE","CODE_NAME"], [list(d) for d in data])


# ────────────────────────────────────────
# 분기별 테이블 (clsYymm 파라미터)
# ────────────────────────────────────────

def gen_contracts(customer_ids, product_cds, quarter, n=12500):
    yy, mm = int(quarter[:4]), int(quarter[4:])
    q_start = datetime(yy, max(1, mm - 2), 1)
    q_end = datetime(yy, mm, 28)
    statuses = ["01"]*70 + ["02"]*15 + ["03"]*10 + ["04"]*3 + ["05"]*2

    rows = []
    for i in range(n):
        seq = int(quarter) * 100000 + i + 1
        rows.append([
            f"CT{seq}", random.choice(customer_ids), random.choice(product_cds),
            fmt(rand_date(q_start, q_end)), quarter,
            random.choice(statuses), random.randint(1, 500) * 10000,
        ])
    write_csv(f"01_contract__{HOST}__clsYymm_{quarter}.csv",
              ["CONTRACT_ID","CUSTOMER_ID","PRODUCT_CD","CONTRACT_DATE","CLS_YYMM","STATUS","CONTRACT_AMT"],
              rows)
    return [r[0] for r in rows]


def gen_payments(contract_ids, quarter):
    yy, mm = int(quarter[:4]), int(quarter[4:])
    q_start = datetime(yy, max(1, mm - 2), 1)
    q_end = datetime(yy, mm, 28)
    pay_types = ["01","02","03"]

    rows = []
    pid = int(quarter) * 1000000
    for cid in contract_ids:
        for _ in range(random.choices([1,2,3], weights=[30,50,20])[0]):
            pid += 1
            rows.append([
                f"PM{pid}", cid, fmt(rand_date(q_start, q_end)),
                random.randint(1, 100) * 10000, random.choice(pay_types),
            ])
    write_csv(f"04_payment__{HOST}__clsYymm_{quarter}.csv",
              ["PAYMENT_ID","CONTRACT_ID","PAYMENT_DATE","PAYMENT_AMT","PAYMENT_TYPE"],
              rows)


def gen_claims(contract_ids, quarter, ratio=0.15):
    yy, mm = int(quarter[:4]), int(quarter[4:])
    q_start = datetime(yy, max(1, mm - 2), 1)
    q_end = datetime(yy, mm, 28)
    claim_types = ["01","02","03","04","05"]
    claim_st = ["01"]*10 + ["02"]*20 + ["03"]*60 + ["04"]*5 + ["05"]*5

    selected = random.sample(contract_ids, int(len(contract_ids) * ratio))
    rows = []
    clid = int(quarter) * 100000
    for cid in selected:
        clid += 1
        rows.append([
            f"CL{clid}", cid, fmt(rand_date(q_start, q_end)),
            random.randint(10, 5000) * 10000,
            random.choice(claim_types), random.choice(claim_st),
        ])
    write_csv(f"05_claim__{HOST}__clsYymm_{quarter}.csv",
              ["CLAIM_ID","CONTRACT_ID","CLAIM_DATE","CLAIM_AMT","CLAIM_TYPE","STATUS"],
              rows)


def gen_contract_agent(contract_ids, agent_ids, quarter):
    rows = [[cid, random.choice(agent_ids)] for cid in contract_ids]
    write_csv(f"08_contract_agent__{HOST}__clsYymm_{quarter}.csv",
              ["CONTRACT_ID","AGENT_ID"], rows)


# ────────────────────────────────────────

def main():
    print(f"출력: {OUT_DIR.resolve()}\n")

    print("=== 마스터 테이블 ===")
    customer_ids = gen_customers(10000)
    product_cds = gen_products()
    branch_cds = gen_branches()
    agent_ids = gen_agents(branch_cds, 200)
    gen_rates(product_cds)
    gen_codes()

    print("\n=== 분기별 테이블 ===")
    for q in QUARTERS:
        print(f"\n--- {q} ---")
        cids = gen_contracts(customer_ids, product_cds, q, 12500)
        gen_payments(cids, q)
        gen_claims(cids, q)
        gen_contract_agent(cids, agent_ids, q)

    total_files = len(list(OUT_DIR.glob("*.csv")))
    total_rows = 0
    for f in OUT_DIR.glob("*.csv"):
        with open(f, encoding="utf-8") as fh:
            total_rows += sum(1 for _ in fh) - 1
    print(f"\n{'='*50}")
    print(f"완료: CSV {total_files}개 | 총 {total_rows:,}행")
    print(f"\n다음 단계:")
    print(f"  python runner.py --job jobs/test_insurance.yml")


if __name__ == "__main__":
    main()
