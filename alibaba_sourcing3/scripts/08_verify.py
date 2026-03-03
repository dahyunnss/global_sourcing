#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Gemini API ONLY 검증 버전
- 전체 데이터를 LLM으로 바로 검증
- Batch 처리
- 중간 저장 지원
"""

import os
import json
import time
import pandas as pd
import google.generativeai as genai
from dotenv import load_dotenv
from datetime import timedelta
from google.api_core.exceptions import ResourceExhausted

# ======================
# 설정
# ======================
load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MODEL_NAME = "gemini-2.5-flash"

BATCH_SIZE = 120
SAVE_INTERVAL = 1000

_COLLECT_DIR = os.path.dirname(os.path.abspath(__file__))

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(MODEL_NAME)


# ======================
# LLM Batch 처리
# ======================

def process_batch(batch_df):

    records = []

    for idx, row in batch_df.iterrows():
        records.append({
            "index": int(idx),
            "keyword": str(row.get("general_keyword_네이버쇼핑", "")),
            "main_products": str(row.get("supplier_mainProducts", "")),
            "product_name": str(row.get("product_name", "")),
        })

    prompt = f"""
각 업체가 keyword 제품을 실제로 판매/제조/취급하면 TRUE,
관련 없으면 FALSE.

단순 키워드 포함 여부가 아니라
실제 제품 카테고리 적합성 기준으로 판단하라.

입력(JSON 배열):
{json.dumps(records, ensure_ascii=False)}

반드시 JSON 배열로만 답하라:
[
  {{"index":0,"is_valid":true}},
  ...
]
"""

    while True:
        try:
            response = model.generate_content(prompt)
            text = response.text.strip()

            json_start = text.find("[")
            json_end = text.rfind("]") + 1

            return json.loads(text[json_start:json_end])

        except ResourceExhausted:
            wait_time = 40
            print(f"⚠️ 쿼터 초과. {wait_time}초 대기 후 재시도...")
            time.sleep(wait_time)

        except Exception as e:
            print("❌ LLM 오류:", e)
            return []


# ======================
# 메인 실행
# ======================

def main(input_csv: str = None, output_csv: str = None):
    import glob
    if input_csv is None:
        candidates = sorted(
            glob.glob(os.path.join(_COLLECT_DIR, "true_*.csv")),
            key=os.path.getmtime, reverse=True
        )
        if not candidates:
            print("❌ true_*.csv 파일을 찾을 수 없습니다.")
            return None
        input_csv = candidates[0]
        print(f"📂 자동 탐색된 CSV: {os.path.basename(input_csv)}")

    if output_csv is None:
        from datetime import datetime
        date_tag = datetime.now().strftime("%y%m%d")
        output_csv = os.path.join(_COLLECT_DIR, f"verified_{date_tag}.csv")

    df = pd.read_csv(input_csv, encoding="utf-8-sig")

    total = len(df)
    print(f"\n🚀 전체 {total:,}건 Gemini ONLY 검증 시작\n")

    df["is_valid_manufacturer"] = None
    df["validation_reason"] = "llm_only"

    start_time = time.time()
    processed = 0

    for i in range(0, total, BATCH_SIZE):

        batch_df = df.iloc[i:i+BATCH_SIZE]

        results = process_batch(batch_df)

        for r in results:
            idx = r["index"]
            df.at[idx, "is_valid_manufacturer"] = r["is_valid"]

        processed = i + len(batch_df)

        elapsed = time.time() - start_time
        progress = processed / total
        eta = (elapsed / progress) - elapsed if progress > 0 else 0

        print(f"✅ 진행: {processed:,}/{total:,} ({progress*100:.2f}%)")
        print(f"⏱ 경과: {timedelta(seconds=int(elapsed))}")
        print(f"⌛ ETA: {timedelta(seconds=int(eta))}")

        if processed % SAVE_INTERVAL < BATCH_SIZE:
            df.to_csv(output_csv, index=False, encoding="utf-8-sig")
            print("💾 중간 저장 완료")

    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    total_time = time.time() - start_time
    print("\n🔥 전체 완료")
    print(f"⏱ 총 소요 시간: {timedelta(seconds=int(total_time))}")
    print(f"📁 저장: {output_csv}")
    return output_csv


if __name__ == "__main__":
    main()