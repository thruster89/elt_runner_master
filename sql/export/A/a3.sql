SELECT
    plyno,
    ins_st,
    ins_clstr,
    an_py_stdt,
    gdcd,
    rate_code,
    pr_bzcs_dscno,
    rl_pym_trm,
    dfr_trm,
    an_py_trm,
    expct_inrt,
    pr_nwcrt_tamt,
    crfw_pr_nwcrt_tamt,
    pym_cyccd,
    an_ins_trm_flgcd,
    an_pytcd,
    an_py_girt,
    reg_dtm,
    upd_dtm
FROM
    contract
    WHERE RATE_CODE = :rateCode
    AND TO_CHAR(INS_ST,'YYYYMM') = :clsYymm1