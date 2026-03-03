#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
10_master_kor.py
- true_*.csv을 입력으로 받음
- master_column.csv의 첫 번째 열(영어 컬럼)만 추출
- 두 번째 열(한국어 컬럼명)으로 rename하여 저장
"""

import os
import glob
import pandas as pd
from datetime import datetime

COLLECT_DIR = os.path.dirname(os.path.abspath(__file__))
MASTER_COL_CSV = os.path.join(COLLECT_DIR, "master_column.csv")


def load_column_mapping() -> tuple[list, dict]:
    """
    master_column.csv 로드
    반환: (영어 컬럼 순서 리스트, {영어 컬럼: 한국어 컬럼} 딕셔너리)
    """
    df = pd.read_csv(MASTER_COL_CSV, encoding="utf-8-sig", header=0)
    # 첫 번째 열: 영어 컬럼명, 두 번째 열: 한국어 컬럼명
    df = df.dropna(subset=[df.columns[0]])
    df = df[df[df.columns[0]].str.strip() != ""]

    en_cols = df.iloc[:, 0].str.strip().tolist()
    ko_cols = df.iloc[:, 1].str.strip().tolist()

    col_order = en_cols
    col_map = dict(zip(en_cols, ko_cols))
    return col_order, col_map


def main(input_csv: str = None, output_csv: str = None):
    # 1. master_column.csv 로드
    col_order, col_map = load_column_mapping()
    print(f"마스터 컬럼: {len(col_order)}개")

    # 2. 입력 파일 탐색
    if input_csv is None:
        candidates = sorted(
            glob.glob(os.path.join(COLLECT_DIR, "true_*.csv")),
            key=os.path.getmtime, reverse=True
        )
        if not candidates:
            print("❌ true_*.csv 파일을 찾을 수 없습니다.")
            return None
        input_csv = candidates[0]
        print(f"📂 자동 탐색된 CSV: {os.path.basename(input_csv)}")

    # 3. 출력 경로 자동 생성
    if output_csv is None:
        date_tag = datetime.now().strftime("%y%m%d")
        output_csv = os.path.join(COLLECT_DIR, f"master_{date_tag}.csv")

    # 4. 데이터 로드
    try:
        df = pd.read_csv(input_csv, encoding="utf-8-sig", low_memory=False)
    except Exception:
        df = pd.read_csv(input_csv, encoding="cp949", low_memory=False)

    print(f"✅ 입력 데이터: {len(df):,}행 × {len(df.columns)}컬럼")

    # 5. 영어 컬럼 중 실제 존재하는 것만 선택 (순서 유지)
    existing_cols = [c for c in col_order if c in df.columns]
    missing_cols  = [c for c in col_order if c not in df.columns]

    if missing_cols:
        print(f"⚠️  데이터에 없는 컬럼 {len(missing_cols)}개 (제외됨): {missing_cols}")

    df = df[existing_cols].copy()

    # 6. 한국어 컬럼명으로 rename
    rename_map = {c: col_map[c] for c in existing_cols}
    df = df.rename(columns=rename_map)

    # 7. 저장
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print("-" * 50)
    print(f"🚀 완료: {output_csv}")
    print(f"📊 최종 행 수: {len(df):,}개 / 최종 컬럼 수: {len(df.columns)}개")
    return output_csv


if __name__ == "__main__":
    main()
