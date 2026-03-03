#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Alibaba 제품 필터링 시스템(그룹 + 단어 단위)
- general_keyword 그룹 단위로 모든 키워드 수집
- 키워드와 제품명을 단어로 분리
- 단어 단위로 매칭 (단어 하나당 2점)
- 모든 키워드의 점수 합산
"""

import os
import glob
import pandas as pd
import re
import urllib.parse
import logging
import sys
from datetime import datetime
from typing import List, Tuple

# ============================================
# 1. 로깅 설정
# ============================================
def setup_logging():
    log_filename = f"filter_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
# 2. 데이터 추출 및 매칭 로직
# ============================================
EXCLUDED_WORDS = ['jet', 'motorcycle', 'bike', 'airplane', 'marine', 'boat']

def extract_product_name_from_url(url: str) -> str:
    """URL에서 제품명 추출 (기존 로직)"""
    if not url or pd.isna(url): return ""
    match = re.search(r'product-detail/([^_]+)', url)
    if match:
        product_slug = match.group(1)
        return product_slug.replace('-', ' ').strip()
    return ""

def normalize_word(word: str) -> str:
    """
    단어 정규화: 하이픈, 공백 제거 및 소문자 변환
    예: "10W-40" -> "10w40", "10W 40" -> "10w40"
    """
    return re.sub(r'[-\s]+', '', word.lower())

def parse_recall_keyword(recall_keyword: str) -> List[str]:
    """키워드 파싱"""
    if not recall_keyword or pd.isna(recall_keyword): return []
    decoded = urllib.parse.unquote_plus(str(recall_keyword))
    return [kw.strip().lower() for kw in decoded.split(',') if kw.strip()]

def calculate_word_match_score(product_name: str, keyword: str) -> Tuple[float, List[str]]:
    """
    키워드와 제품명을 모두 단어로 분리해서 단어 단위로 비교
    하이픈, 공백, 대소문자 무시하고 비교
    """
    product_lower = product_name.lower()

    # 제품명을 단어로 분리 (공백과 +로만 분리, 하이픈은 유지)
    product_words_raw = re.split(r'[\s+]+', product_lower)
    product_words_raw = [w.strip() for w in product_words_raw if len(w.strip()) > 1]

    # 제품명 단어를 정규화
    product_words_normalized = {normalize_word(w): w for w in product_words_raw}

    # 키워드를 단어로 분리 (공백과 +로만 분리, 하이픈은 유지)
    keyword_words = re.split(r'[\s+]+', keyword.lower())
    keyword_words = [w.strip() for w in keyword_words if len(w.strip()) > 1]

    score = 0.0
    matched = []

    # 각 키워드 단어가 제품명 단어 리스트에 있는지 확인 (정규화해서 비교)
    for word in keyword_words:
        normalized = normalize_word(word)
        if normalized in product_words_normalized:
            score += 2.0
            matched.append(product_words_normalized[normalized])  # 제품명의 원본 단어 추가

    return score, matched

def calculate_total_match_score(product_name: str, all_keywords: List[str]) -> Tuple[float, List[str]]:
    """
    모든 키워드를 단어로 쪼개서 제품명 단어와 매칭
    매칭되는 단어마다 2점 부여
    하이픈, 공백, 대소문자 무시하고 비교
    """
    product_lower = product_name.lower()

    # 제품명을 단어로 분리 (공백과 +로만 분리, 하이픈은 유지)
    product_words_raw = re.split(r'[\s+]+', product_lower)
    product_words_raw = [w.strip() for w in product_words_raw if len(w.strip()) > 1]

    # 제품명 단어를 정규화 (하이픈, 공백 제거)
    product_words_normalized = {normalize_word(w): w for w in product_words_raw}

    # 모든 키워드를 단어로 분리해서 합치기
    keyword_words_normalized = {}  # {정규화된 단어: 원본 단어}
    for keyword in all_keywords:
        # 키워드를 단어로 분리 (공백과 +로만 분리, 하이픈은 유지)
        keyword_words = re.split(r'[\s+]+', keyword.lower())
        keyword_words = [w.strip() for w in keyword_words if len(w.strip()) > 1]

        # 정규화해서 저장
        for word in keyword_words:
            normalized = normalize_word(word)
            if normalized not in keyword_words_normalized:
                keyword_words_normalized[normalized] = word

    # 정규화된 단어로 교집합 구하기
    matched_normalized = set(product_words_normalized.keys()) & set(keyword_words_normalized.keys())

    # 매칭된 원본 단어 추출 (제품명에 있는 원본 단어)
    matched_words = [product_words_normalized[norm] for norm in matched_normalized]

    # 점수 계산 (매칭된 단어 개수 × 2점)
    score = len(matched_words) * 2.0

    return score, matched_words

def collect_all_keywords_for_group(df_group: pd.DataFrame) -> List[str]:
    """
    같은 general_keyword를 가진 모든 행의 키워드를 수집
    """
    all_keywords = []
    
    # 우선순위: recallKeyWord > oriKeyWord > enKeyword
    for col in ['product_traceCommonArgs_recallKeyWord', 
                'product_traceCommonArgs_oriKeyWord',
                'product_traceCommonArgs_enKeyword']:
        
        if col not in df_group.columns:
            continue
        
        # 이 컬럼의 모든 값 수집
        for val in df_group[col].dropna():
            if str(val).strip():
                keywords = parse_recall_keyword(str(val))
                all_keywords.extend(keywords)
    
    # 중복 제거하고 반환
    return list(set(all_keywords))

# ============================================
# 3. 필터링 프로세스
# ============================================
def process_filtering(df: pd.DataFrame, min_score: float = 2.0) -> pd.DataFrame:
    log_print("="*80)
    log_print(f"🔍 제품 필터링 시작 (총 {len(df):,}개 제품)")
    log_print("="*80)
    
    # general_keyword 컬럼 확인
    keyword_col = None
    for col in ['general_keyword_네이버쇼핑', 'general_keyword', 'keyword']:
        if col in df.columns:
            keyword_col = col
            break
    
    if not keyword_col:
        raise ValueError("general_keyword 컬럼을 찾을 수 없습니다")
    
    log_print(f"\n📝 사용 컬럼: {keyword_col}")
    
    # 제품명 추출 (기존 로직)
    df['product_name'] = df['product_action'].apply(extract_product_name_from_url)
    
    # general_keyword별로 그룹화
    log_print("\n📊 general_keyword별 키워드 수집 중...")
    
    unique_keywords = df[keyword_col].unique()
    log_print(f"총 {len(unique_keywords)}개의 general_keyword 발견")
    
    # general_keyword별 모든 키워드 수집
    keyword_dict = {}  # {general_keyword: [모든 키워드들]}
    
    for gk in unique_keywords:
        if pd.isna(gk):
            continue
        
        df_group = df[df[keyword_col] == gk]
        all_kws = collect_all_keywords_for_group(df_group)
        keyword_dict[gk] = all_kws
        
        log_print(f"  {gk}: {len(all_kws)}개 고유 키워드")
    
    # 각 제품을 그룹의 모든 키워드와 비교
    log_print("\n🎯 제품별 최적 키워드 매칭 중...")
    
    results = []
    excluded_lower = [w.lower() for w in EXCLUDED_WORDS]
    
    for idx, row in df.iterrows():
        product_name_raw = row['product_name']
        p_name_lower = product_name_raw.lower()
        general_kw = row[keyword_col]
        
        # 1. 제외 키워드 체크
        found_excluded = None
        for word in excluded_lower:
            if word in p_name_lower:
                found_excluded = word
                break
        
        if found_excluded:
            score, matched = 0.0, []
            is_relevant = False
            reason = f"제외 단어 포함 ({found_excluded})"
        else:
            # 2. 이 general_keyword의 모든 키워드와 비교
            if pd.notna(general_kw) and general_kw in keyword_dict:
                all_kws = keyword_dict[general_kw]
                
                if len(all_kws) > 0:
                    # 모든 키워드의 단어들과 비교해서 점수 합산
                    score, matched = calculate_total_match_score(product_name_raw, all_kws)
                else:
                    score, matched = 0.0, []
            else:
                score, matched = 0.0, []
            
            is_relevant = score >= min_score
            reason = "점수 미달" if not is_relevant else "통과"
        
        # [상세 로깅] False로 판정된 경우만 기록
        if not is_relevant:
            log_print(f"   [False] 행: {idx} | general_kw: {general_kw} | 사유: {reason} | 점수: {score}")
            log_print(f"      ㄴ 제품명: {product_name_raw}")
            if matched:
                log_print(f"      ㄴ 매칭된 단어: {matched}")
        
        results.append({
            'match_score': score,
            'matched_keywords': ', '.join(matched) if matched else '',
            'matched_keyword_count': len(matched),
            'is_relevant': is_relevant
        })
    
    df['match_score'] = [r['match_score'] for r in results]
    df['matched_keywords'] = [r['matched_keywords'] for r in results]
    df['matched_keyword_count'] = [r['matched_keyword_count'] for r in results]
    df['is_relevant'] = [r['is_relevant'] for r in results]
    
    return df

# ============================================
# 4. 메인 실행부
# ============================================
def main(input_csv: str = None, output_csv: str = None):
    log_print("🚀 Alibaba 제품 그룹 필터링 시작")

    try:
        if input_csv is None:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = sorted(glob.glob(os.path.join(script_dir, "combined_suppliers_*.csv")),
                                key=os.path.getmtime, reverse=True)
            if not candidates:
                log_print("❌ combined_suppliers_*.csv 파일을 찾을 수 없습니다.")
                return None
            input_csv = candidates[0]
            log_print(f"📂 자동 탐색된 CSV: {os.path.basename(input_csv)}")

        df = pd.read_csv(input_csv, encoding='utf-8-sig', low_memory=False)
        
        # 초기 데이터 통계
        log_print("\n" + "="*80)
        log_print("📊 초기 데이터 통계")
        log_print("="*80)
        log_print(f"전체 상품 수: {len(df):,}개")
        
        # 제조사 수 계산
        supplier_columns = [
            'supplier_companyId'
        ]
        
        supplier_col_found = None
        for col in supplier_columns:
            if col in df.columns:
                supplier_col_found = col
                total_suppliers = df[col].nunique()
                log_print(f"전체 제조사 수: {total_suppliers:,}개 (컬럼: {col})")
                break
        
        if not supplier_col_found:
            log_print("전체 제조사 수: 제조사 정보 컬럼을 찾을 수 없습니다")
        
        # 필터링 실행
        df_result = process_filtering(df, min_score=2.0)
        
        # 필터링 결과 통계
        true_count = df_result['is_relevant'].sum()
        false_count = len(df_result) - true_count
        
        log_print("\n" + "="*80)
        log_print("📊 필터링 결과 통계")
        log_print("="*80)
        
        log_print(f"\n[상품 수]")
        log_print(f"  전체 상품: {len(df_result):,}개")
        log_print(f"  ✅ 관련 있음 (True):  {true_count:,}개 ({true_count/len(df_result)*100:.1f}%)")
        log_print(f"  ❌ 관련 없음 (False): {false_count:,}개 ({false_count/len(df_result)*100:.1f}%)")
        
        # 제조사 수 통계
        if supplier_col_found:
            log_print(f"\n[제조사 수]")
            total_suppliers_result = df_result[supplier_col_found].nunique()
            log_print(f"  전체 제조사: {total_suppliers_result:,}개")
            
            df_true = df_result[df_result['is_relevant'] == True]
            true_suppliers = df_true[supplier_col_found].nunique() if len(df_true) > 0 else 0
            log_print(f"  ✅ 관련 있는 제조사: {true_suppliers:,}개 ({true_suppliers/total_suppliers_result*100:.1f}%)")
            
            df_false = df_result[df_result['is_relevant'] == False]
            false_suppliers = df_false[supplier_col_found].nunique() if len(df_false) > 0 else 0
            log_print(f"  ❌ 관련 없는 제조사: {false_suppliers:,}개 ({false_suppliers/total_suppliers_result*100:.1f}%)")
            
            if len(df_true) > 0 and len(df_false) > 0:
                true_supplier_set = set(df_true[supplier_col_found].dropna().unique())
                false_supplier_set = set(df_false[supplier_col_found].dropna().unique())
                overlap_suppliers = true_supplier_set & false_supplier_set
                if len(overlap_suppliers) > 0:
                    log_print(f"  ⚠️ 양쪽에 모두 있는 제조사: {len(overlap_suppliers):,}개")
        
        # 결과 저장
        if output_csv is None:
            date_tag = datetime.now().strftime("%y%m%d")
            output_dir = os.path.dirname(os.path.abspath(input_csv))
            output_csv = os.path.join(output_dir, f"relevance_{date_tag}.csv")

        df_result.to_csv(output_csv, index=False, encoding='utf-8-sig')

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