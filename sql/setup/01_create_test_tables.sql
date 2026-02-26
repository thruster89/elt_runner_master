-- ============================================================
-- 테스트용 테이블 생성 스크립트 (Oracle에서 수동 1회 실행)
-- 실행: sqlplus tester/aa12345@127.0.0.1:1521/ORCLPDB @sql/setup/01_create_test_tables.sql
-- ============================================================

-- 계약 기본 테이블
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TB_CONTRACT';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE TB_CONTRACT (
    CONTRACT_ID   VARCHAR2(20)   NOT NULL,
    CLS_YYMM      VARCHAR2(6)    NOT NULL,   -- 기준연월 (YYYYMM)
    PRODUCT_CD    VARCHAR2(10),
    CUSTOMER_ID   VARCHAR2(20),
    CONTRACT_AMT  NUMBER(15, 2),
    STATUS        VARCHAR2(2),               -- 10:유효, 20:실효, 30:해지
    REG_DTM       DATE DEFAULT SYSDATE,
    CONSTRAINT PK_CONTRACT PRIMARY KEY (CONTRACT_ID)
);

-- 납입 내역 테이블
BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE TB_PAYMENT';
EXCEPTION WHEN OTHERS THEN NULL;
END;
/

CREATE TABLE TB_PAYMENT (
    PAYMENT_ID    VARCHAR2(20)   NOT NULL,
    CONTRACT_ID   VARCHAR2(20)   NOT NULL,
    CLS_YYMM      VARCHAR2(6)    NOT NULL,
    PAY_AMT       NUMBER(15, 2),
    PAY_DT        DATE,
    PAY_TYPE      VARCHAR2(2),               -- 01:카드, 02:계좌이체, 03:가상계좌
    CONSTRAINT PK_PAYMENT PRIMARY KEY (PAYMENT_ID)
);

COMMIT;
