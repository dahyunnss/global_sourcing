#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
- 03_collect_en, 03_collect_cn의 최종 CSV를 자동 탐색
- master_column_list_before_relev.csv에 정의된 컬럼만 유지
- 두 CSV를 합쳐 하나의 combined_suppliers_{date}.csv로 저장
"""

import os
import glob
import pandas as pd
from datetime import datetime
import logging

COLLECT_DIR      = os.path.dirname(os.path.abspath(__file__))
BASE_DIR         = os.path.dirname(COLLECT_DIR)
MASTER_COL_CSV   = os.path.join(COLLECT_DIR, "master_column_list_before_relev.csv")


# ── 마스터 컬럼 로드 ──────────────────────────────────────────
def load_master_columns() -> list:
    df = pd.read_csv(MASTER_COL_CSV, header=None, encoding="utf-8-sig")
    cols = df.iloc[:, 0].dropna().str.strip().tolist()
    return [c for c in cols if c]


# ── 수집 결과 CSV 자동 탐색 ───────────────────────────────────
def find_output_csv(filename: str, root: str = None) -> str | None:
    """
    sourcing_data/**/filename 패턴으로 가장 최신 CSV 탐색.
    root가 주어지면 그 경로 기준, 없으면 BASE_DIR 기준으로 검색.
    """
    search_root = root if root else BASE_DIR
    pattern = os.path.join(search_root, "sourcing_data", "**", filename)
    matches = glob.glob(pattern, recursive=True)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


# ── 컬럼 필터링 ───────────────────────────────────────────────
def filter_to_master(df: pd.DataFrame, master_cols: list) -> pd.DataFrame:
    """
    master_cols 순서대로 컬럼 정렬.
    원본에 없는 컬럼은 빈 값으로 추가.
    """
    for col in master_cols:
        if col not in df.columns:
            df[col] = None
    return df[master_cols]


# ── 메인 ──────────────────────────────────────────────────────
def main(output_path: str = None, sourcing_root: str = None):
    # 1. 마스터 컬럼 로드
    master_cols = load_master_columns()
    logging.info(f"마스터 컬럼: {len(master_cols)}개")
    logging.info(f"  → {master_cols}")

    # 2. EN / CN CSV 탐색
    en_csv = find_output_csv("all_keywords_suppliers_en.csv", sourcing_root)
    cn_csv = find_output_csv("all_keywords_suppliers_cn.csv", sourcing_root)

    dfs = []

    if en_csv:
        df_en = pd.read_csv(en_csv, encoding="utf-8-sig", low_memory=False)
        logging.info(f"\nEN CSV 로드: {len(df_en):,}행, {len(df_en.columns)}컬럼")
        logging.info(f"  경로: {en_csv}")
        df_en = filter_to_master(df_en, master_cols)
        dfs.append(df_en)
    else:
        logging.info("⚠️  EN CSV(all_keywords_suppliers_en.csv)를 찾을 수 없습니다.")

    if cn_csv:
        df_cn = pd.read_csv(cn_csv, encoding="utf-8-sig", low_memory=False)
        logging.info(f"\nCN CSV 로드: {len(df_cn):,}행, {len(df_cn.columns)}컬럼")
        logging.info(f"  경로: {cn_csv}")
        df_cn = filter_to_master(df_cn, master_cols)
        dfs.append(df_cn)
    else:
        logging.info("⚠️  CN CSV(all_keywords_suppliers_cn.csv)를 찾을 수 없습니다.")

    if not dfs:
        logging.info("❌ 처리할 CSV 파일이 없습니다.")
        return

    # 3. EN + CN 합치기
    df_combined = pd.concat(dfs, ignore_index=True)
    logging.info(f"\n합산: {len(df_combined):,}행 × {len(master_cols)}컬럼")

    # 4. 저장
    if output_path is None:
        date_tag = datetime.now().strftime("%y%m%d")
        output_path = os.path.join(COLLECT_DIR, f"combined_suppliers_{date_tag}.csv")

    df_combined.to_csv(output_path, index=False, encoding="utf-8-sig")

    logging.info(f"\n완료! 저장됨 → {output_path}")
    logging.info(f"최종: {len(df_combined):,}행, {len(master_cols)}컬럼")


if __name__ == "__main__":
    main()
