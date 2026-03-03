#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
main.py
- 0_input/ 폴더에 0_input_gk_*.csv 파일을 넣고 실행하면 전체 파이프라인이 순차 실행됩니다.
- output 파일이 이미 존재하면 해당 STEP을 SKIP합니다.

파이프라인:
  STEP 1 : GK 변환            (scripts/01_convert_gk.py)
  STEP 2 : 영/중 번역         (scripts/02_translate_en_cn.py)
  STEP 3 : EN 키워드 수집     (scripts/03_collect_en.py)
  STEP 4 : CN 키워드 수집     (scripts/04_collect_cn.py)
  STEP 5 : 컬럼 통합          (scripts/05_combine_column.py)
  STEP 6 : 관련성 필터링      (scripts/06_relevance.py)
  STEP 7 : TRUE 필터링        (scripts/07_true.py)
  STEP 8 : 제조사 유효성 검증 (scripts/08_verify.py)
  STEP 9 : 유효 제조사 필터링 (scripts/09_valid.py)
  STEP 10: 마스터 컬럼 정리   (scripts/10_master_kor.py)

디렉토리 구조:
  0_input/          원본 입력 파일 (0_input_gk_날짜.csv)
  1_output/날짜/    단계별 결과 CSV
  2_logs/           파이프라인 실행 로그
  scripts/          파이프라인 스크립트 모음
"""

import sys
import os
import glob
import importlib.util
import logging
import pandas as pd
from datetime import datetime

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR     = os.path.join(BASE_DIR, "scripts")
INPUT_DIR       = os.path.join(BASE_DIR, "0_input")
OUTPUT_BASE_DIR = os.path.join(BASE_DIR, "1_output")
LOG_DIR         = os.path.join(BASE_DIR, "2_logs")

sys.path.insert(0, SCRIPTS_DIR)


# ── 로깅 설정 ──────────────────────────────────────────────────
def setup_logging() -> str:
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file  = os.path.join(LOG_DIR, f"pipeline_{timestamp}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    if logger.hasHandlers():
        logger.handlers.clear()

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    return log_file

def log(msg: str = ""):
    logging.info(msg)


# ── 입력 파일 자동 감지 ────────────────────────────────────────
def find_input_csv() -> str:
    pattern = os.path.join(INPUT_DIR, "0_input_gk_*.csv")
    matches = sorted(glob.glob(pattern))
    if not matches:
        raise FileNotFoundError(
            f"입력 파일을 찾을 수 없습니다.\n"
            f"아래 경로에 '0_input_gk_날짜.csv' 형식으로 파일을 넣어주세요:\n"
            f"  {INPUT_DIR}"
        )
    if len(matches) > 1:
        matches.sort(key=os.path.getmtime, reverse=True)
        log(f"[주의] 입력 파일 {len(matches)}개 감지 → 가장 최신 파일 사용: {os.path.basename(matches[0])}")
    return matches[0]


# ── sourcing_data/** 에서 파일 탐색 ───────────────────────────
def find_in_sourcing_data(filename: str, output_dir: str) -> str | None:
    pattern = os.path.join(output_dir, "sourcing_data", "**", filename)
    matches = glob.glob(pattern, recursive=True)
    return matches[0] if matches else None


# ── 스크립트 모듈 로드 ────────────────────────────────────────
def load_script(script_filename: str):
    spec = importlib.util.spec_from_file_location(
        script_filename.replace(".", "_"),
        os.path.join(SCRIPTS_DIR, script_filename)
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── 일반 스텝 실행 (input → output) ──────────────────────────
def run_step(script_filename: str, step_label: str, input_csv: str, output_csv: str):
    if os.path.exists(output_csv):
        log(f"[SKIP] {step_label} → 이미 존재: {os.path.basename(output_csv)}")
        return

    log(f"{'='*55}")
    log(f"  {step_label}")
    log(f"  입력: {os.path.basename(input_csv)}")
    log(f"  출력: {os.path.basename(output_csv)}")
    log(f"{'='*55}")

    mod = load_script(script_filename)
    mod.main(input_csv, output_csv)

    log(f"[완료] {step_label}")


# ── 번역 CSV → 언어별 분리 저장 ───────────────────────────────
def split_translate_csv(translate_csv: str, output_dir: str, date_tag: str) -> tuple[str, str]:
    df = pd.read_csv(translate_csv, encoding="utf-8-sig")

    keep_cols = ["keyword", "general_keyword_네이버쇼핑"]

    df_en = df[df["language"] == "en"][keep_cols].drop_duplicates().reset_index(drop=True)
    df_zh = df[df["language"] == "zh"][keep_cols].drop_duplicates().reset_index(drop=True)

    en_csv = os.path.join(output_dir, f"step3_en_keywords_{date_tag}.csv")
    zh_csv = os.path.join(output_dir, f"step4_zh_keywords_{date_tag}.csv")

    df_en.to_csv(en_csv, index=False, encoding="utf-8-sig")
    df_zh.to_csv(zh_csv, index=False, encoding="utf-8-sig")

    log(f"  EN 키워드: {len(df_en)}개 → {os.path.basename(en_csv)}")
    log(f"  CN 키워드: {len(df_zh)}개 → {os.path.basename(zh_csv)}")

    return en_csv, zh_csv


# ── 수집 스텝 실행 (auto_confirm=True) ────────────────────────
def run_collect_step(script_filename: str, step_label: str, csv_path: str, output_root: str):
    log(f"{'='*55}")
    log(f"  {step_label}")
    log(f"  입력: {os.path.basename(csv_path)}")
    log(f"  출력: {output_root}")
    log(f"{'='*55}")

    mod = load_script(script_filename)
    mod.main(csv_path, auto_confirm=True, output_root=output_root)

    log(f"[완료] {step_label}")


# ── 메인 ──────────────────────────────────────────────────────
if __name__ == "__main__":

    # 0. 로깅 초기화
    log_file = setup_logging()

    # 1. 입력 파일 감지
    input_csv = find_input_csv()
    date_tag  = os.path.basename(input_csv).replace("0_input_gk_", "").replace(".csv", "")

    # 2. 출력 디렉토리 생성
    output_dir = os.path.join(OUTPUT_BASE_DIR, date_tag)
    os.makedirs(output_dir, exist_ok=True)

    log(f"로그 파일    : {log_file}")
    log(f"입력 파일    : {os.path.basename(input_csv)}  (날짜 태그: {date_tag})")
    log(f"출력 디렉토리: {output_dir}")

    # 3. sourcing_data 루트 (STEP 3/4 수집 결과가 저장되는 위치)
    en_sourcing_root = os.path.join(output_dir, "sourcing_data", f"supplier_search_{date_tag}_en")
    cn_sourcing_root = os.path.join(output_dir, "sourcing_data", f"supplier_search_{date_tag}_cn")

    # 4. 중간/출력 경로 자동 생성
    step1_out     = os.path.join(output_dir, f"step1_gk_{date_tag}.csv")
    step2_out     = os.path.join(output_dir, f"step2_translate_{date_tag}.csv")
    combined_out  = os.path.join(output_dir, f"step5_combined_{date_tag}.csv")
    relevance_out = os.path.join(output_dir, f"step6_relevance_{date_tag}.csv")
    true_out      = os.path.join(output_dir, f"step7_true_{date_tag}.csv")
    verified_out  = os.path.join(output_dir, f"step8_verified_{date_tag}.csv")
    valid_out     = os.path.join(output_dir, f"step9_valid_{date_tag}.csv")
    master_out    = os.path.join(output_dir, f"step10_master_{date_tag}.csv")

    # ── STEP 1: GK 변환 ──
    run_step("01_convert_gk.py", "STEP 1 / 10 : GK 변환", input_csv, step1_out)

    # ── STEP 2: 영/중 번역 ──
    run_step("02_translate_en_cn.py", "STEP 2 / 10 : 영/중 번역", step1_out, step2_out)

    # ── STEP 3/4: 번역 CSV → EN/CN 분리 ──
    en_csv = os.path.join(output_dir, f"step3_en_keywords_{date_tag}.csv")
    zh_csv = os.path.join(output_dir, f"step4_zh_keywords_{date_tag}.csv")

    if os.path.exists(en_csv) and os.path.exists(zh_csv):
        log(f"[SKIP] STEP 3·4 / 10 준비 → EN/CN 키워드 CSV 이미 존재")
    else:
        log(f"{'='*55}")
        log(f"  STEP 3·4 / 10 준비: 언어별 키워드 CSV 분리")
        log(f"{'='*55}")
        en_csv, zh_csv = split_translate_csv(step2_out, output_dir, date_tag)

    # ── STEP 3: EN 키워드 수집 ──
    if find_in_sourcing_data("all_keywords_suppliers_en.csv", output_dir):
        log(f"[SKIP] STEP 3 / 10 : EN 키워드 수집 → all_keywords_suppliers_en.csv 이미 존재")
    else:
        run_collect_step("03_collect_en.py", "STEP 3 / 10 : EN 키워드 수집", en_csv, en_sourcing_root)

    # ── STEP 4: CN 키워드 수집 ──
    if find_in_sourcing_data("all_keywords_suppliers_cn.csv", output_dir):
        log(f"[SKIP] STEP 4 / 10 : CN 키워드 수집 → all_keywords_suppliers_cn.csv 이미 존재")
    else:
        run_collect_step("04_collect_cn.py", "STEP 4 / 10 : CN 키워드 수집", zh_csv, cn_sourcing_root)

    # ── STEP 5: 컬럼 통합 ──
    if os.path.exists(combined_out):
        log(f"[SKIP] STEP 5 / 10 : 컬럼 통합 → 이미 존재: {os.path.basename(combined_out)}")
    else:
        log(f"{'='*55}")
        log(f"  STEP 5 / 10 : 컬럼 통합 (05_combine_column.py)")
        log(f"  출력: {os.path.basename(combined_out)}")
        log(f"{'='*55}")
        combine_mod = load_script("05_combine_column.py")
        combine_mod.main(output_path=combined_out, sourcing_root=output_dir)
        log(f"[완료] STEP 5 / 10 : 컬럼 통합")

    # ── STEP 6: 관련성 필터링 ──
    run_step("06_relevance.py", "STEP 6 / 10 : 관련성 필터링", combined_out, relevance_out)

    # ── STEP 7: TRUE 필터링 ──
    run_step("07_true.py", "STEP 7 / 10 : TRUE 필터링", relevance_out, true_out)

    # ── STEP 8: 제조사 유효성 검증 ──
    run_step("08_verify.py", "STEP 8 / 10 : 제조사 유효성 검증", true_out, verified_out)

    # ── STEP 9: 유효 제조사 필터링 ──
    run_step("09_valid.py", "STEP 9 / 10 : 유효 제조사 필터링", verified_out, valid_out)

    # ── STEP 10: 마스터 컬럼 정리 ──
    run_step("10_master_kor.py", "STEP 10 / 10 : 마스터 컬럼 정리", valid_out, master_out)

    log(f"{'='*55}")
    log("  전체 파이프라인 완료!")
    log(f"  번역 결과      → {step2_out}")
    log(f"  컬럼 통합      → {combined_out}")
    log(f"  관련성 필터링  → {relevance_out}")
    log(f"  TRUE 필터링    → {true_out}")
    log(f"  유효성 검증    → {verified_out}")
    log(f"  유효 필터링    → {valid_out}")
    log(f"  최종 결과      → {master_out}")
    log(f"  로그 파일      → {log_file}")
    log(f"{'='*55}")
