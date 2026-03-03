#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
valid.py
- is_valid_manufacturer == FALSE 인 행 제거
- TRUE 또는 BLANK(빈값/NaN) 인 행만 유지
"""

import os
import glob
import pandas as pd
import logging

_COLLECT_DIR = os.path.dirname(os.path.abspath(__file__))


def main(input_csv: str = None, output_csv: str = None):
    if input_csv is None:
        candidates = sorted(
            glob.glob(os.path.join(_COLLECT_DIR, "verified_*.csv")),
            key=os.path.getmtime, reverse=True
        )
        if not candidates:
            logging.info("❌ verified_*.csv 파일을 찾을 수 없습니다.")
            return None
        input_csv = candidates[0]
        logging.info(f"📂 자동 탐색된 CSV: {os.path.basename(input_csv)}")
    if output_csv is None:
        base, ext = os.path.splitext(input_csv)
        output_csv = f"{base}_filtered{ext}"

    # 로드
    try:
        df = pd.read_csv(input_csv, encoding="utf-8-sig", low_memory=False)
    except Exception:
        df = pd.read_csv(input_csv, encoding="cp949", low_memory=False)

    logging.info(f"✅ 입력 데이터: {len(df):,}행")

    col = "is_valid_manufacturer"
    if col not in df.columns:
        logging.info(f"❌ '{col}' 컬럼을 찾을 수 없습니다.")
        return

    # FALSE 판별: 문자열 "False"/"FALSE"/"false" 또는 bool False
    is_false = df[col].apply(
        lambda v: str(v).strip().lower() == "false" if pd.notna(v) and str(v).strip() != "" else False
    )

    before = len(df)
    df_filtered = df[~is_false].copy()
    removed = before - len(df_filtered)

    logging.info(f"  제거된 행 (FALSE): {removed:,}개")
    logging.info(f"  남은 행 (TRUE + BLANK): {len(df_filtered):,}개")

    # 불필요 컬럼 제거
    drop_cols = [c for c in [
        "product_name", "match_score", "matched_keywords",
        "matched_keyword_count", "is_valid_manufacturer", "validation_reason"
    ] if c in df_filtered.columns]
    df_filtered = df_filtered.drop(columns=drop_cols)
    logging.info(f"  제거된 컬럼: {drop_cols}")

    df_filtered.to_csv(output_csv, index=False, encoding="utf-8-sig")
    logging.info(f"🚀 저장 완료: {output_csv}")
    return output_csv


if __name__ == "__main__":
    main()
