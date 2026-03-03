#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import time
import json
import pandas as pd
from dotenv import load_dotenv

# OpenAI
from openai import OpenAI

# Gemini
import google.generativeai as genai

OPENAI_MODEL = "gpt-4o"
GEMINI_MODEL = "gemini-2.5-flash-lite"

SLEEP_SEC = 0.4


# API 초기화
def init_openai() -> OpenAI:
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("환경변수 OPENAI_API_KEY가 없습니다.")
    return OpenAI(api_key=api_key, timeout=30)


def init_gemini():
    load_dotenv()
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("환경변수 GEMINI_API_KEY가 없습니다.")
    genai.configure(api_key=api_key)


# 공통 프롬프트
def build_instructions() -> str:
    return (
        "당신은 자동차 부품 및 액세서리 분야의 전문 번역가이자 Alibaba 공급사 소싱 전문가입니다.\n"
        "당신은 짧은 한국어 자동차 용품 키워드를 입력받게 됩니다.\n"
        "각 한국어 키워드에 대해 다음을 생성하세요:\n"
        "- GPT가 판단한 영어(en) 키워드 리스트\n"
        "- GPT가 판단한 중국어(간체)(zh) 키워드 리스트\n\n"

        "번역 및 생성 규칙:\n"
        "1) 영어/중국어 키워드는 1개 이상 자유롭게 생성할 수 있습니다.\n"
        "2) 자동차 애프터마켓 검색에 적합한 '명사구 형태'로 작성해야 합니다.\n"
        "3) Alibaba 제조사가 실제로 사용하는 업계 표준 용어를 기준으로 생성하십시오.\n"
        "4) 한국어 키워드가 모호할 경우, 가장 일반적이고 널리 사용되는 자동차 관련 의미를 선택하십시오.\n"
        "5) 비현실적이거나 Alibaba에서 사용하지 않는 단어는 절대 사용하지 마십시오.\n"
        "6) 번역 시 차량 관련 단어를 반드시 포함하여 자동차 용품으로 보이도록 하십시오.\n\n"

        "⚠️ 반드시 다음 JSON 형식으로만 답변하십시오:\n"
        "{\n"
        "  \"ko\": \"<원본>\",\n"
        "  \"en\": [\"<영문1>\", \"<영문2>\", ...],\n"
        "  \"zh\": [\"<중문1>\", \"<중문2>\", ...]\n"
        "}\n"
    )


def translate_openai(client: OpenAI, ko: str):
    messages = [
        {"role": "system", "content": build_instructions()},
        {"role": "user", "content": f"한국어 키워드: \"{ko}\"\nJSON으로 번역 반환."}
    ]
    completion = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=messages,
        temperature=0.2,
    )
    raw = completion.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.strip().strip("`").replace("json", "", 1).strip()
    return json.loads(raw)


def translate_gemini(ko: str):
    model = genai.GenerativeModel(GEMINI_MODEL)
    response = model.generate_content([
        build_instructions(),
        f"한국어 키워드: \"{ko}\"\nJSON으로 번역 반환.\n"
    ])
    raw = response.text.strip()
    if raw.startswith("```"):
        raw = raw.strip().strip("`").replace("json", "", 1).strip()
    return json.loads(raw)


def translate_keyword(engine: str, openai_client: OpenAI, ko: str):
    for attempt in range(1, 3):
        try:
            if engine == "openai":
                return translate_openai(openai_client, ko)
            else:
                return translate_gemini(ko)
        except Exception as e:
            print(f"  [경고] {engine} 번역 실패 시도 {attempt}: {e}")
            time.sleep(attempt)
    return None


def run_engine(engine: str, api_name: str, unique_keywords, openai_client) -> list:
    """단일 엔진으로 전체 키워드 번역 후 row 리스트 반환"""
    rows = []
    total = len(unique_keywords)
    for idx, ko in enumerate(unique_keywords):
        print(f"  [{idx+1}/{total}] '{ko}' 번역 중... ({api_name})")

        result = translate_keyword(engine, openai_client, ko)

        if result is None:
            for lang in ["en", "zh"]:
                rows.append({"API_Type": api_name, "general_keyword_car": ko,
                              "language": lang, "keyword": ""})
            continue

        en_list = result.get("en", [])
        zh_list = result.get("zh", [])
        if not isinstance(en_list, list):
            en_list = [en_list] if en_list else []
        if not isinstance(zh_list, list):
            zh_list = [zh_list] if zh_list else []

        if en_list:
            for kw in en_list:
                rows.append({"API_Type": api_name, "general_keyword_car": ko,
                              "language": "en", "keyword": kw})
        else:
            rows.append({"API_Type": api_name, "general_keyword_car": ko,
                         "language": "en", "keyword": ""})

        if zh_list:
            for kw in zh_list:
                rows.append({"API_Type": api_name, "general_keyword_car": ko,
                              "language": "zh", "keyword": kw})
        else:
            rows.append({"API_Type": api_name, "general_keyword_car": ko,
                         "language": "zh", "keyword": ""})

        time.sleep(SLEEP_SEC)

    return rows


def main(input_csv: str, output_csv: str):
    openai_client = init_openai()
    init_gemini()

    df = pd.read_csv(input_csv, encoding="utf-8-sig")
    if "general_keyword_car" not in df.columns:
        raise RuntimeError("입력 CSV에 'general_keyword_car' 컬럼이 없습니다.")

    unique_keywords = df["general_keyword_car"].unique()

    # ── STEP A: Gemini 번역 ──────────────────────────────────
    print(f"\n[Gemini 번역 시작] 총 {len(unique_keywords)}개 키워드")
    rows_gemini = run_engine("gemini", "Gemini", unique_keywords, openai_client)
    print(f"[Gemini 번역 완료] {len(rows_gemini)}행 생성")

    # ── STEP B: OpenAI 번역 ──────────────────────────────────
    print(f"\n[OpenAI 번역 시작] 총 {len(unique_keywords)}개 키워드")
    rows_openai = run_engine("openai", "ChatGPT", unique_keywords, openai_client)
    print(f"[OpenAI 번역 완료] {len(rows_openai)}행 생성")

    # ── 합쳐서 저장 (Gemini 먼저, OpenAI 다음) ───────────────
    df_out = pd.DataFrame(rows_gemini + rows_openai)
    df_out = df_out[["API_Type", "general_keyword_car", "language", "keyword"]]
    df_out = df_out.rename(columns={"general_keyword_car": "general_keyword_네이버쇼핑"})
    df_out.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"\n완료! 저장됨 → {output_csv}")
    print(f"총 {len(df_out)}행 생성됨  (Gemini {len(rows_gemini)}행 + OpenAI {len(rows_openai)}행)")


if __name__ == "__main__":
    _dir = os.path.dirname(os.path.abspath(__file__))
    _matches = sorted(glob.glob(os.path.join(_dir, "0_output_gk_*.csv")))
    if not _matches:
        raise FileNotFoundError(f"0_output_gk_*.csv 파일을 찾을 수 없습니다: {_dir}")
    _input = max(_matches, key=os.path.getmtime)
    _tag = os.path.basename(_input).replace("0_output_gk_", "").replace(".csv", "")
    _output = os.path.join(_dir, f"1_output_translate_{_tag}.csv")
    main(_input, _output)
