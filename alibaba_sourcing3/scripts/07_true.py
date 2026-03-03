#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
is_relevant = TRUE 데이터만 필터링
"""

import os
import glob
import pandas as pd
import logging
import sys
from datetime import datetime

# ============================================
# 1. 로깅 설정
# ============================================
def setup_logging():
    log_filename = f"filter_true_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)

    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return log_filename

def log_print(message=""):
    logging.info(message)

# ============================================
# 2. 메인 실행부
# ============================================
def main(input_csv: str = None, output_csv: str = None):
    log_print("🚀 is_relevant = TRUE 필터링 시작")

    try:
        if input_csv is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = sorted(glob.glob(os.path.join(script_dir, "relevance_*.csv")),
                                key=os.path.getmtime, reverse=True)
            if not candidates:
                log_print("❌ relevance_*.csv 파일을 찾을 수 없습니다.")
                return None
            input_csv = candidates[0]
            log_print(f"📂 자동 탐색된 CSV: {os.path.basename(input_csv)}")

        df = pd.read_csv(input_csv, encoding='utf-8-sig', low_memory=False)

        # 초기 데이터 통계
        log_print("\n" + "="*80)
        log_print("📊 초기 데이터 통계")
        log_print("="*80)
        log_print(f"전체 상품 수: {len(df):,}개")

        # is_relevant 컬럼 확인
        if 'is_relevant' not in df.columns:
            raise ValueError("is_relevant 컬럼을 찾을 수 없습니다")

        # TRUE/FALSE 분포 확인
        true_count = df['is_relevant'].sum()
        false_count = len(df) - true_count

        log_print(f"  ✅ is_relevant = TRUE:  {true_count:,}개 ({true_count/len(df)*100:.1f}%)")
        log_print(f"  ❌ is_relevant = FALSE: {false_count:,}개 ({false_count/len(df)*100:.1f}%)")

        # 제조사 수 계산 (있다면)
        supplier_col = None
        if 'supplier_companyId' in df.columns:
            supplier_col = 'supplier_companyId'

        if supplier_col:
            total_suppliers = df[supplier_col].nunique()
            log_print(f"\n전체 제조사 수: {total_suppliers:,}개")

        # TRUE 데이터만 필터링
        log_print("\n" + "="*80)
        log_print("🔍 is_relevant = TRUE 데이터 필터링 중...")
        log_print("="*80)

        df_true = df[df['is_relevant'] == True]

        # 필터링 결과 통계
        log_print("\n" + "="*80)
        log_print("📊 필터링 결과 통계")
        log_print("="*80)

        log_print(f"\n[상품 수]")
        log_print(f"  필터링 전: {len(df):,}개")
        log_print(f"  필터링 후: {len(df_true):,}개")
        log_print(f"  제거된 상품: {len(df) - len(df_true):,}개")

        # 제조사 수 통계
        if supplier_col:
            true_suppliers = df_true[supplier_col].nunique()
            log_print(f"\n[제조사 수]")
            log_print(f"  필터링 전: {total_suppliers:,}개")
            log_print(f"  필터링 후: {true_suppliers:,}개")
            log_print(f"  제거된 제조사: {total_suppliers - true_suppliers:,}개")

        # general_keyword별 통계
        keyword_col = None
        for col in ['general_keyword_네이버쇼핑', 'general_keyword', 'keyword']:
            if col in df.columns:
                keyword_col = col
                break

        if keyword_col:
            log_print(f"\n[{keyword_col}별 분포]")
            keyword_dist_before = df[keyword_col].value_counts()
            keyword_dist_after = df_true[keyword_col].value_counts()

            log_print(f"  필터링 전 키워드 종류: {len(keyword_dist_before)}개")
            log_print(f"  필터링 후 키워드 종류: {len(keyword_dist_after)}개")

        # 결과 저장
        if output_csv is None:
            date_tag = datetime.now().strftime("%y%m%d")
            output_dir = os.path.dirname(os.path.abspath(input_csv))
            output_csv = os.path.join(output_dir, f"true_{date_tag}.csv")

        drop_cols = [c for c in ['is_relevant', 'product_traceCommonArgs_enKeyword'] if c in df_true.columns]
        df_true = df_true.drop(columns=drop_cols)
        df_true.to_csv(output_csv, index=False, encoding='utf-8-sig')

        log_print("\n" + "="*80)
        log_print(f"✅ 작업 완료!")
        log_print(f"💾 최종 결과 저장: {output_csv}")
        log_print("="*80)

        return output_csv

    except Exception as e:
        logging.error(f"❌ 오류 발생: {str(e)}", exc_info=True)
        return None

if __name__ == "__main__":
    setup_logging()
    main()
