#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv
import google.generativeai as genai
import json, time, hashlib, re, glob
from pathlib import Path
from typing import Any, Dict, List, Tuple
import requests
import pandas as pd
from urllib.parse import urlencode
from datetime import datetime
import html as _html
import re as _re
import time, shutil
import random
import unicodedata
from collections import OrderedDict
import sys
import urllib.parse

# [설정] 수집 경로 및 필터
OUTPUT_ROOT = ""  # main()에서 동적으로 설정됨
MAX_RETRY_PER_KEYWORD = 3
API_BASE = "https://www.alibaba.com/search/api/supplierTextSearch"

# [핵심] 중국어 수집을 위한 언어/국가 설정
LANGUAGE = "zh"
COUNTRY = "CN"

SEARCH_FILTERS = {
    "country": "CN",              
}

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

# ⏰ 휴식 시간 설정
BATCH_SIZE = 10           # 10개 키워드마다 긴 휴식
LONG_BREAK_MIN = 3        # 최소 휴식 시간 (분)
LONG_BREAK_MAX = 5        # 최대 휴식 시간 (분)
KEYWORD_DELAY_MIN = 8     # 키워드 간 최소 대기 시간 (초)
KEYWORD_DELAY_MAX = 15    # 키워드 간 최대 대기 시간 (초)
PAGE_DELAY_MIN = 6        # 페이지 간 최소 대기 시간 (초)
PAGE_DELAY_MAX = 12       # 페이지 간 최대 대기 시간 (초)

_INVALID_FS_CHARS = r'<>:"/\\|?*'
_WINDOWS_RESERVED = {
    "CON", "PRN", "AUX", "NUL",
    *{f"COM{i}" for i in range(1, 10)},
    *{f"LPT{i}" for i in range(1, 10)},
}

load_dotenv()
genai.configure(api_key=os.getenv("GENAI_API_KEY"))

# ================================
# Only File Logging System
# ================================
class FileLogger:
    """터미널에는 출력하지 않고 오직 파일에만 기록"""
    def __init__(self, filename):
        self.log = open(filename, "a", encoding="utf-8")

    def write(self, message):
        self.log.write(message)
        # 즉시 파일에 쓰기 위해 flush 호출
        self.log.flush()

    def flush(self):
        self.log.flush()

def ensure_dir(p: Path): 
    p.mkdir(parents=True, exist_ok=True)
    
def setup_logging():
    """로그 파일 전용 설정"""
    log_dir = Path(OUTPUT_ROOT) / "0_logs"
    ensure_dir(log_dir)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"
    
    # 1. 파일 로거 인스턴스 생성
    logger = FileLogger(log_file)
    
    # 2. 표준 출력과 표준 에러를 모두 파일로 리다이렉트
    sys.stdout = logger
    sys.stderr = logger

    sys.__stdout__.write(f"🚀 작업이 시작되었습니다. 모든 출력은 로그 파일에 저장됩니다.\n📂 로그 경로: {log_file}\n")
    return log_file

def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    최종 컬럼 순서 재배치 (한 번만 실행)
    - 존재하는 컬럼만 정렬
    - 없는 컬럼은 자동 제외
    - 예상 외 컬럼은 맨 뒤로
    """
    desired_order = [
        
        # General Keyword
        "general_keyword_네이버쇼핑",
        
        # Meta 정보
        "meta_keyword",
        "meta_page",
        "meta_collected_at",
        
        # Supplier 정보
        "supplier_action",
        "supplier_companyId",
        "supplier_companyName",
        "supplier_companyTitle",
        "supplier_companyIcon",
        "supplier_companyImage",
        "supplier_newAd",
        "supplier_adInfo_ext",
        "supplier_adInfo_multiTemplate",
        "supplier_adInfo_campaignType",
        "supplier_adInfo_elementScene",
        "supplier_adInfo_immersion",
        "supplier_adInfo_campaignId",
        "supplier_adInfo_adgroupId",
        "supplier_adInfo_creativeInfo_viewProfileText",
        "supplier_adInfo_creativeInfo_mainProduct_subject",
        "supplier_adInfo_creativeInfo_mainProduct_imageUrl",
        "supplier_adInfo_creativeInfo_mainProduct_action",
        "supplier_adInfo_creativeInfo_mainProduct_id",
        "supplier_adInfo_creativeInfo_mainProduct_title",
        "supplier_adInfo_creativeInfo_tpText",
        "supplier_adInfo_creativeInfo_products",
        "supplier_adInfo_templateId",
        "supplier_adInfo_creativeId",
        "supplier_adInfo_creativeInfo_directoryVideoUrl",
        "supplier_adInfo_creativeInfo_companyVideo",
        "supplier_adInfo_creativeInfo_sampleExhibitionVideoUrl",
        "supplier_adInfo_creativeInfo_productionFlowVideoUrl",
        "supplier_areaNumber",
        "supplier_staffNumber",
        "supplier_goldYearsNumber",
        "supplier_goldYears",
        "supplier_verifiedSupplier",
        "supplier_verifiedSupplierPro",
        "supplier_countryCode",
        "supplier_city",
        "supplier_isFactory",
        "supplier_factoryCertificationType",
        "supplier_factoryCertificationLabel",
        "supplier_certIconList",
        "supplier_tagList",
        "supplier_vrUrl",
        "supplier_videoUrl",
        "supplier_videoPoster",
        "supplier_reviewScore",
        "supplier_reviewCount",
        "supplier_reviewLink",
        "supplier_onTimeDelivery",
        "supplier_replyAvgTime",
        "supplier_reorderRate",
        "supplier_onlineRevenue",
        "supplier_customizationOptions",
        "supplier_contactSupplier",
        "supplier_tmlid",
        "supplier_chatToken",
        "supplier_mainProducts",
        "supplier_clickEurl",
        "supplier_impsEurl",
        "supplier_liveLink",
        "supplier_oemLabel",
        "supplier_trackInfo",
        "supplier_traceCommonArgs_companyId",
        "supplier_traceCommonArgs_isManufacture",
        "supplier_traceCommonArgs_productId",
        "supplier_traceCommonArgs_item_type",
        "supplier_traceCommonArgs_rank",
        "supplier_traceCommonArgs_pid",
        "supplier_traceCommonArgs_trackInfo_detail",
        "supplier_traceCommonArgs_requestGlobalId",
        
        # Product 정보
        "product_action",
        "product_productId",
        "product_productImg",
        "product_price",
        "product_moq",
        "product_cateId",
        "product_itemType",
        "product_traceCommonArgs_@@recallKeyWord:",
        "product_traceCommonArgs_is_customizable",
        "product_traceCommonArgs_productId",
        "product_traceCommonArgs_item_type",
        "product_traceCommonArgs_semiManaged",
        "product_traceCommonArgs_pid",
        "product_traceCommonArgs_is_half_trust_instant_order",
        "product_traceCommonArgs_showAd",
        "product_traceCommonArgs_enKeyword",
        "product_traceCommonArgs_oriKeyWord",
        "product_traceCommonArgs_companyId",
        "product_traceCommonArgs_product_type",
        "product_traceCommonArgs_rlt_rank",
        "product_traceCommonArgs_isCommon",
        "product_traceCommonArgs_langident",
        "product_traceCommonArgs_is_half_trust_customizable",
        "product_traceCommonArgs_requestGlobalId",
        "product_traceCommonArgs_traceInfoStr",
        "product_traceCommonArgs_recallKeyWord",
    ]
    
    # 실제 존재하는 컬럼만 필터링
    existing_ordered_cols = [col for col in desired_order if col in df.columns]
    
    # 지정되지 않은 나머지 컬럼 (맨 뒤로)
    remaining_cols = [col for col in df.columns if col not in desired_order]
    
    # 최종 컬럼 순서
    final_order = existing_ordered_cols + remaining_cols
    
    return df[final_order]


def extract_main_products_names(main_products_raw: str) -> str:
    """
    mainProducts JSON 문자열에서 name 값만 추출
    
    입력: '[{"name": "Car Mat", "count": null}, {"name": "Floor Liner", "count": null}]'
    출력: 'Car Mat, Floor Liner'
    """
    if not main_products_raw or pd.isna(main_products_raw):
        return ""
    
    if not isinstance(main_products_raw, str):
        return str(main_products_raw)
    
    try:
        products_list = json.loads(main_products_raw)
        
        if not isinstance(products_list, list):
            return main_products_raw
        
        names = []
        for item in products_list:
            if isinstance(item, dict) and item.get("name"):
                names.append(str(item["name"]))
        
        return ", ".join(names)
    
    except (json.JSONDecodeError, TypeError, AttributeError):
        return main_products_raw


def is_chinese(text: str) -> bool:
    return bool(re.search(r'[\u4e00-\u9fff]', text))

# is_chinese() 함수 바로 아래에 추가
def translate_cn_to_en(text_cn: str) -> str:
    """
    중국어 키워드를 영어로 번역 (Gemini API 사용)
    """
    if not text_cn or not text_cn.strip():
        return text_cn
    
    # 이미 영어면 그대로 반환
    if not is_chinese(text_cn):
        return text_cn
    
    for attempt in range(5):
        try:
            model = genai.GenerativeModel("gemini-2.0-flash")
            
            prompt = (
                "Translate this Chinese automotive keyword to English.\n"
                "Rules:\n"
                "- Keep it concise and natural\n"
                "- Use common product terminology\n"
                "- Output ONLY the English translation\n"
                "- Do NOT add explanations\n\n"
                f"Chinese: {text_cn}"
            )
            
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=50
                )
            )
            
            translated = response.text.strip()
            print(f"🌏 번역: {text_cn} → {translated}")
            return translated if translated else text_cn
            
        except Exception as e:
            if "429" in str(e):
                wait_time = (2 ** attempt) + random.random()
                print(f"⚠️ Rate Limit. {wait_time:.1f}초 후 재시도...")
                time.sleep(wait_time)
            else:
                print(f"⚠️ 번역 실패: {e}")
                return text_cn
    
    return text_cn

def clean_keyword(s: str) -> str:
    """폴더/요청에서 키워드를 안정적으로 쓰기 위한 정규화"""
    if not isinstance(s, str):
        return ""
    s = s.strip()
    # 양끝 큰따옴표 제거
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1].strip()
    # 유니코드 정규화(전각/반각/결합문자 등)
    s = unicodedata.normalize("NFKC", s)
    # 연속 공백 정리
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_keyword_for_matching(keyword: str) -> str:
    """
    키워드를 매칭용으로 정규화
    - clean_keyword() 적용
    - 파일시스템 금지문자를 언더스코어로 치환 (slug와 동일한 로직)
    """
    if not keyword:
        return ""
    
    # 1단계: 기본 정규화 (따옴표 제거, 공백 정리 등)
    normalized = clean_keyword(keyword)
    
    # 2단계: 파일시스템 금지문자 치환 (slug 함수와 동일)
    for ch in _INVALID_FS_CHARS:  # '<>:"/\\|?*'
        normalized = normalized.replace(ch, '_')
    
    # 3단계: 연속된 언더스코어 정리
    normalized = re.sub(r'_+', '_', normalized).strip('_')
    
    return normalized

def load_keywords_from_csv(csv_path: str) -> List[Dict[str, str]]:
    encodings = ["utf-8-sig", "gb18030", "cp949"]
    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"지원하는 인코딩으로 파일을 읽을 수 없습니다: {csv_path}")

    # 헤더 정리
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    if 'keyword' not in df.columns:
        raise ValueError("CSV 파일에 'keyword' 컬럼이 없습니다.")

    # 일반 키워드: 결측 제거
    df['keyword'] = df['keyword'].astype(str).str.strip()
    df = df[df['keyword'] != ""]

    # general 컬럼 정규화
    gcol = 'general_keyword_네이버쇼핑'
    if gcol not in df.columns:
        df[gcol] = None
    df[gcol] = df[gcol].astype(str).str.strip().replace({"": None, "nan": None})

    rows = [{'keyword': r['keyword'], 'general': (r[gcol] if pd.notna(r[gcol]) else None)}
            for _, r in df.iterrows()]

    print(f"{len(rows)}개 키워드 로드 완료.")
    return rows
    
def slug(s: str, max_len: int = 80) -> str:
    """
    중국어/한글/영어/혼합 모두 원문 기반 폴더명 생성
    - 파일시스템 금지문자만 '_' 치환
    - 너무 길면 해시 fallback
    - 필요 시에만 keyword_mapping.json에 매핑 저장
    """
    original = clean_keyword(s)
    if not original:
        return "alibaba"

    # 금지문자/제어문자 치환
    buf = []
    for ch in original:
        if ch in _INVALID_FS_CHARS or ord(ch) < 32:
            buf.append("_")
        else:
            buf.append(ch)
    safe = "".join(buf)

    # Windows에서 문제되는 trailing dot/space 제거
    safe = safe.strip(" .")
    safe = re.sub(r"_+", "_", safe).strip("_")

    if not safe:
        return "alibaba"

    # Windows 예약 파일명 회피
    if safe.upper() in _WINDOWS_RESERVED:
        safe = f"_{safe}"

    # 너무 길면 해시로 fallback + 매핑 저장
    if len(safe) > max_len:
        hash_val = hashlib.md5(original.encode("utf-8")).hexdigest()[:12]

        mapping_file = Path(OUTPUT_ROOT) / "keyword_mapping.json"
        mapping = {}
        if mapping_file.exists():
            with open(mapping_file, "r", encoding="utf-8") as f:
                mapping = json.load(f)
        mapping[hash_val] = original
        mapping_file.parent.mkdir(parents=True, exist_ok=True)
        with open(mapping_file, "w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)

        return hash_val

    return safe


def now_ms() -> int: 
    return int(time.time() * 1000)


def extract_supplier_id(supplier_card: dict) -> str:
    """제조사의 고유 ID를 추출"""
    # companyId 우선
    company_id = supplier_card.get("companyId")
    if company_id:
        return str(company_id)
    
    # action URL에서 ID 추출
    action = supplier_card.get("action", "")
    if action:
        match = re.search(r"https?://([^.]+)\.en\.alibaba\.com", action)
        if match:
            return match.group(1)
    
    # companyName
    company_name = supplier_card.get("companyName", "")
    if company_name:
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', str(company_name))
        return safe_name[:50]
    
    return ""


def is_keyword_already_collected(keyword: str, out_root: str) -> bool:
    """
    키워드가 이미 수집되었는지 확인
    - suppliers_all.json 파일 존재 여부로 판단
    - summary.json의 success 플래그로 성공 여부 확인
    """
    keyword_norm = clean_keyword(keyword)
    keyword_folder = Path(out_root) / slug(keyword_norm)
    
    # suppliers_all.json 파일 존재 확인
    suppliers_file = keyword_folder / "suppliers_all.json"
    summary_file = keyword_folder / "summary.json"
    
    if not suppliers_file.exists():
        return False
    
    # summary.json이 있으면 success 플래그 확인
    if summary_file.exists():
        try:
            with open(summary_file, "r", encoding="utf-8") as f:
                summary = json.load(f)
                # success가 True이고 최소 1개 이상의 데이터가 있으면 수집 완료로 판단
                if summary.get("success", False) and summary.get("deduplication_stats", {}).get("unique_suppliers", 0) > 0:
                    return True
        except:
            pass
    
    # suppliers_all.json이 있고 비어있지 않으면 수집 완료로 판단
    try:
        with open(suppliers_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            if data and len(data) > 0:
                return True
    except:
        pass
    
    return False


def get_collection_progress(keywords: List[Dict[str, str]], out_root: str) -> Dict[str, Any]:
    """
    현재 수집 진행 상황 확인
    """
    total = len(keywords)
    collected = 0
    skipped_keywords = []
    
    for item in keywords:
        keyword = item["keyword"]
        if is_keyword_already_collected(keyword, out_root):
            collected += 1
            skipped_keywords.append(keyword)
    
    return {
        "total": total,
        "collected": collected,
        "remaining": total - collected,
        "progress": f"{collected}/{total} ({collected/total*100:.1f}%)" if total > 0 else "0/0 (0.0%)",
        "skipped_keywords": skipped_keywords
    }


# ================================
# API 호출 및 데이터 처리 함수들
# ================================
def build_url(query_cn: str, query_en: str, page: int, page_size: int, extras: Dict[str, str] = None) -> str:
    """
    브라우저 요청과 동일한 형식으로 URL 생성
    
    Args:
        query_cn: 중국어 원본 (예: "车载档把")
        query_en: 영어 번역 (예: "car gear shift lever")
    """
    # 영어를 제품명/속성으로 분리
    words = query_en.split()
    
    # 제품명과 속성 분리 로직
    if len(words) >= 3:
        # 예: "car gear shift lever" → product="car parts", attr="gear shift lever"
        if words[0] in ["car", "vehicle", "auto", "automotive"]:
            product_name = f"{words[0]} parts"
            product_attr = " ".join(words[1:])
        else:
            product_name = " ".join(words[:2])
            product_attr = " ".join(words[2:])
    elif len(words) == 2:
        product_name = words[0]
        product_attr = words[1]
    else:
        product_name = query_en
        product_attr = ""
    
    common = {
        # ✅ 브라우저와 동일한 파라미터
        "productQpKeywords": query_en.replace(" ", "+"),
        "supplierQpProductName": product_name,
        "query": f"{product_name}, {product_attr}" if product_attr else product_name,
        "productAttributes": product_attr,
        "pageSize": page_size,
        "productName": product_name,
        "intention": "",
        "queryProduct": query_en.replace(" ", "+"),
        "supplierAttributes": "",
        "requestId": f"AI_Web_3500008931995_{now_ms()}",
        "queryRaw": query_cn,  # ✅ 중국어 원본
        "supplierQpKeywords": f"{product_name},{product_attr}" if product_attr else product_name,
        "startTime": now_ms(),
        "isCompanyName": "0",
        "langident": "zh",  # ✅ 중국어 언어 설정
        "page": page,
        "pro": "true",
        "from": "pcHomeContent",
    }
    
    if extras:
        common.update(extras)
    
    return f"{API_BASE}?{urlencode(common, doseq=True)}"


def find_card_arrays(node: Any, path: str = "") -> List[Tuple[str, List[Dict[str, Any]]]]:
    found = []
    if isinstance(node, dict):
        for k, v in node.items():
            p = f"{path}.{k}" if path else k
            found.extend(find_card_arrays(v, p))
    elif isinstance(node, list):
        dicts = [x for x in node if isinstance(x, dict)]
        if dicts and any("companyId" in d for d in dicts):
            found.append((path, dicts))
        else:
            for i, v in enumerate(node):
                found.extend(find_card_arrays(v, f"{path}[{i}]"))
    return found


def safe_get(session, url, headers, retries=3, delay=3):
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=(10, 60))

            if r.status_code == 200:
                return r

            print(f"⚠️ HTTP {r.status_code} 오류 → 재시도 {attempt}/{retries}")
        except Exception as e:
            print(f"⚠️ 요청 실패 → 재시도 {attempt}/{retries} ({e})")

        # 재시도 딜레이 (점진적 증가)
        wait_time = delay * (attempt + 1)  # 6초, 9초, 12초
        print(f"   {wait_time}초 대기 중...")
        time.sleep(wait_time)

    return None  # 최종 실패


def collect_single_keyword(query: str, out_root: str, page_size: int = 20, max_pages: int = 20, cookie: str = None) -> bool:
    #단일 키워드에 대해 Alibaba 공급자 검색 API를 호출하여 데이터 수집
    #페이지 간 중복제거
    query_norm = clean_keyword(query)
    query_en = translate_cn_to_en(query_norm)
    print(f"🔍 키워드: {query_norm} → {query_en}")

    s = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7,ko;q=0.6",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": f"https://www.alibaba.com/search/page?SearchScene=suppliers&pro=true&SearchText={urllib.parse.quote(query_norm)}&from=pcHomeContent",
        "Sec-Ch-Ua": '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Priority": "u=1, i",
    }

    if cookie:
            headers["Cookie"] = cookie
    elif os.getenv("ALIBABA_COOKIE"):
            headers["Cookie"] = os.getenv("ALIBABA_COOKIE")

    print(f"수집 설정: page_size={page_size}, max_pages={max_pages}")
    
    base = Path(out_root) / slug(query_norm)
    raw_dir = base / "raw"
    ensure_dir(raw_dir)

    unique_suppliers = {}
    per_page_counts = []
    per_page_new_counts = []  # 페이지별 신규 제조사 수
    per_page_dup_counts = []  # 페이지별 중복 제조사 수
    
    stop_reason = None
    filters = SEARCH_FILTERS
    has_data = False  # 데이터 수집 성공 여부

    # ✅ 연속 빈 페이지 카운터 추가
    consecutive_empty_pages = 0
    MAX_CONSECUTIVE_EMPTY = 3  # 연속 3페이지 빈 페이지면 중단
    
    for page in range(1, max_pages + 1):
        #url = build_url(query_norm, page, page_size, extras=filters)
        url = build_url(query_norm, query_en, page, page_size, extras=filters)

        print(f"\n{'='*100}")
        print(f"[페이지 {page}] 요청 URL:")
        print(f"{url}")
        print(f"{'='*100}\n")

        
        # ⏰ 페이지 간 대기 시간 (랜덤)
        wait_time = random.uniform(PAGE_DELAY_MIN, PAGE_DELAY_MAX)

        print(f"[{page}] ⏰ {wait_time:.1f}초 대기 중...")
        time.sleep(wait_time)
        r = safe_get(s, url, headers)  
        if r is None:
            stop_reason = "http_502_retry_fail"
            print(f"[{page}] ❌ 3회 재시도 실패 → 중단")
            break
    
        if r.status_code != 200:
            stop_reason = f"http_{r.status_code}"
            print(f"[{page}] ❌ HTTP {r.status_code}")
            break

        # RAW 저장
        try:
            data = r.json()
        except Exception as e:
            print(f"[{page}] ❌ JSON 파싱 실패: {e}")
            stop_reason = "json_parse_error"
            break
        
        # 페이지별 RAW JSON 저장
        with open(raw_dir / f"page_{page:03d}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        # 카드 수 확인
        buckets = find_card_arrays(data)
        cards = []
        if buckets:
            buckets.sort(key=lambda x: -len(x[1]))
            cards = [c for c in buckets[0][1] if isinstance(c, dict)]
        
        # 빈 페이지 처리 개선
        if len(cards) == 0:
            consecutive_empty_pages += 1
            print(f"[{page}] ⚠️ 빈 페이지 (연속 {consecutive_empty_pages}회)")
            
            # 연속 3페이지 빈 페이지면 중단
            if consecutive_empty_pages >= MAX_CONSECUTIVE_EMPTY:
                stop_reason = f"consecutive_empty_pages_{consecutive_empty_pages}"
                print(f"[{page}] 🛑 연속 {consecutive_empty_pages}개 빈 페이지 → 중단")
                break
            else:
                # 아직 연속 빈 페이지가 3개 미만이면 계속 진행
                per_page_counts.append(0)
                per_page_new_counts.append(0)
                per_page_dup_counts.append(0)
                continue
        else:
            # 데이터가 있으면 카운터 리셋
            consecutive_empty_pages = 0
            has_data = True
            
        page_new = 0
        page_dup = 0
        
        for card in cards:
            supplier_id = extract_supplier_id(card)
            
            if not supplier_id:
                print(f"  ⚠️ supplier_id 추출 실패 (건너뜀)")
                continue
            
            if supplier_id in unique_suppliers:
                # 중복 발견
                unique_suppliers[supplier_id]['appearance_count'] += 1
                unique_suppliers[supplier_id]['pages_found'].append(page)
                page_dup += 1
            else:
                # 신규 제조사 (원본 데이터 그대로 저장)
                unique_suppliers[supplier_id] = {
                    'data': card,  # ← 원본 그대로, 메타 정보 추가 안 함
                    'appearance_count': 1,
                    'pages_found': [page],
                    'supplier_id': supplier_id
                }
                page_new += 1
        
        per_page_counts.append(len(cards))
        per_page_new_counts.append(page_new)
        per_page_dup_counts.append(page_dup)
        
        # 페이지별 상세 로그
        print(f"[{page}] ✅ 총: {len(cards)}개 | "
              f"신규: {page_new}개 | "
              f"중복: {page_dup}개 | "
              f"누적 유니크: {len(unique_suppliers)}개")
        
        # 종료 조건
        if len(cards) == 0:
            stop_reason = "empty_page"
            break
        if len(cards) < page_size:
            stop_reason = "last_page"
            break

    # 중복 제거 통계
    total_collected = sum(per_page_counts)
    total_unique = len(unique_suppliers)
    total_duplicates = total_collected - total_unique
    dup_rate = (total_duplicates / total_collected * 100) if total_collected > 0 else 0

    summary = {
        "query": query_norm,
        "pages_fetched": len(per_page_counts),
        "counts_per_page": per_page_counts,
        "new_per_page": per_page_new_counts,
        "dup_per_page": per_page_dup_counts,
        "deduplication_stats": {
            "total_collected": total_collected,
            "unique_suppliers": total_unique,
            "duplicates_removed": total_duplicates,
            "duplication_rate": f"{dup_rate:.1f}%"
        },
        "stop_reason": stop_reason or "max_pages_reached",
        "out_dir": str(base.resolve()),
        "success": has_data,  # 성공 여부 추가
    }
    (base / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), "utf-8")

    print("📊 수집 요약")
    print("="*50)
    print(f"🔍 키워드: {query_norm}")
    print(f"📄 페이지: {len(per_page_counts)}개")
    print(f"📦 총 수집: {total_collected}개")
    print(f"✅ 유니크: {total_unique}개")
    print(f"🔄 중복 제거: {total_duplicates}개 ({dup_rate:.1f}%)")
    
    # 최다 등장 제조사 TOP3
    if unique_suppliers:
        top_suppliers = sorted(
            unique_suppliers.values(), 
            key=lambda x: x['appearance_count'], 
            reverse=True
        )[:3]
        
        if any(s['appearance_count'] > 1 for s in top_suppliers):
            print(f"\n🏆 최다 등장 제조사 TOP3:")
            for i, s in enumerate(top_suppliers, 1):
                if s['appearance_count'] > 1:
                    company_name = s['data'].get('companyName', 'Unknown')[:40]
                    pages = ', '.join(map(str, s['pages_found']))
                    print(f"  {i}. {company_name}")
                    print(f"     - 등장: {s['appearance_count']}번 (페이지: {pages})")
    
    print("="*50)
    
    # 중복 제거된 데이터를 페이지 형식으로 재구성 (merge_raw_pages 역할 대체)
    # 이제 merge_raw_pages를 호출할 필요가 없음
    merged_pages = []
    page_suppliers = {}  # {page_num: [suppliers]}
    
    for supplier_id, supplier_info in unique_suppliers.items():
        first_page = supplier_info['pages_found'][0]
        
        if first_page not in page_suppliers:
            page_suppliers[first_page] = []
        
        # 원본 데이터 그대로 사용 (메타 정보 추가 안 함)
        page_suppliers[first_page].append(supplier_info['data'])
    
    # 페이지 순서대로 재구성
    for page in sorted(page_suppliers.keys()):
        raw_file = raw_dir / f"page_{page:03d}.json"
        if raw_file.exists():
            with open(raw_file, "r", encoding="utf-8") as f:
                original_data = json.load(f)
            
            # offers 부분만 중복 제거된 데이터로 교체
            if 'model' in original_data and 'offers' in original_data['model']:
                original_data['model']['offers'] = page_suppliers[page]
            
            merged_pages.append({
                "page": page,
                "data": original_data
            })
    
    # suppliers_all.json 저장 (merge_raw_pages와 동일한 형식)
    out_path = base / "suppliers_all.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged_pages, f, ensure_ascii=False, indent=2)
    
    print(f"✅ {query_norm} → suppliers_all.json 생성 완료 (중복 제거됨)")
    
    return has_data
    
    
def merge_raw_pages(keyword: str, base_dir: Path) -> bool:
    #키워드별 raw 폴더의 page JSON들을 하나의 suppliers_all.json으로 통합
    keyword_norm = clean_keyword(keyword)
    keyword_slug = slug(keyword_norm)
    raw_dir = base_dir / keyword_slug / "raw"

    if not raw_dir.exists():
        print(f"❌ RAW 폴더 없음: {raw_dir}")
        return False

    raw_files = sorted(raw_dir.glob("page_*.json"))
    if not raw_files:
        print(f"❌ page JSON 없음: {raw_dir}")
        return False

    merged = []

    for rf in raw_files:
        page = int(rf.stem.split("_")[1])
        with open(rf, "r", encoding="utf-8") as f:
            data = json.load(f)

        merged.append({
            "page": page,
            "data": data
        })

    out_path = base_dir / keyword_slug / "suppliers_all.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    
    print(f"✅ {keyword_norm} → suppliers_all.json 생성 완료 ({len(merged)} pages)")
    return True


def merge_all_keywords_json(base_dir: Path, output_filename: str = "all_keywords_suppliers_cn.json"):
    #모든 키워드의 suppliers_all.json을 하나로 통합
    print("\n" + "="*60)
    print("🔄 모든 키워드의 suppliers_all.json 통합 시작")
    print("="*60)
    
    # 모든 키워드 폴더 찾기
    keyword_folders = [d for d in base_dir.iterdir() if d.is_dir() and (d / "suppliers_all.json").exists()]
    
    if not keyword_folders:
        print("❌ suppliers_all.json 파일을 찾을 수 없습니다.")
        return None
    
    print(f"📁 발견된 키워드 폴더: {len(keyword_folders)}개")
    
    all_data = []
    
    for folder in sorted(keyword_folders):
        suppliers_json = folder / "suppliers_all.json"
        
        try:
            with open(suppliers_json, "r", encoding="utf-8") as f:
                keyword_data = json.load(f)
            
            # suppliers_all.json 구조 확인
            if keyword_data and isinstance(keyword_data, list):
                keyword_name = keyword_data[0].get("keyword", folder.name)
                
                all_data.append({
                    "keyword": keyword_name,
                    "folder": folder.name,
                    "pages": keyword_data
                })
                
                print(f"  ✅ {keyword_name} ({len(keyword_data)} pages)")
            else:
                print(f"  ⚠️ {folder.name}: 데이터 구조 이상")
                
        except Exception as e:
            print(f"  ❌ {folder.name}: 읽기 실패 ({e})")
    
    # 통합 JSON 저장
    output_path = base_dir / output_filename
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 통합 완료: {output_path}")
    print(f"📊 총 {len(all_data)}개 키워드 데이터 통합")
    
    return output_path


def save_failed_keywords(base_dir: Path, failed_keywords: List[str]):
    """실패한 키워드 목록을 JSON 파일로 저장"""
    failed_path = base_dir / "failed_keywords.json"
    with open(failed_path, "w", encoding="utf-8") as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "failed_keywords": failed_keywords,
            "count": len(failed_keywords)
        }, f, ensure_ascii=False, indent=2)
    print(f"\n⚠️ 실패한 키워드 목록 저장: {failed_path}")


def load_keyword_general_mapping(csv_path: str = "") -> Dict[str, str]:
    # 원본 CSV에서 keyword → general_keyword_네이버쇼핑 매핑 로드
    
    encodings = ["utf-8-sig", "gb18030", "cp949"]
    for enc in encodings:
        try:
            df = pd.read_csv(csv_path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        print(f"⚠️ CSV 파일을 읽을 수 없습니다: {csv_path}")
        return {}
    
    # 헤더 정리
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]
    
    # 필수 컬럼 확인
    if 'keyword' not in df.columns:
        print(f"⚠️ 'keyword' 컬럼이 없습니다.")
        return {}
    
    gcol = 'general_keyword_네이버쇼핑'
    if gcol not in df.columns:
        print(f"⚠️ '{gcol}' 컬럼이 없습니다.")
        return {}
    
    # 매핑 딕셔너리 생성 (정규화 적용)
    mapping = {}
    mismatches = []  # 디버깅용: 변환된 키워드 추적
    
    for _, row in df.iterrows():
        keyword_raw = str(row['keyword']).strip()
        keyword_normalized = normalize_keyword_for_matching(keyword_raw)  # 정규화
        general = str(row[gcol]).strip() if pd.notna(row[gcol]) else ""
        
        # 변환 전후 차이가 있으면 기록 (디버깅용)
        if keyword_raw != keyword_normalized:
            mismatches.append({
                'original': keyword_raw,
                'normalized': keyword_normalized,
                'general': general
            })
        
        if keyword_normalized and keyword_normalized != "nan":
            mapping[keyword_normalized] = general
    
    print(f"✅ {len(mapping)}개 키워드-general 매핑 로드 완료")
    
    # 정규화된 키워드 샘플 출력 (처음 10개)
    if mismatches:
        print(f"\n🔍 정규화된 키워드 샘플 ({len(mismatches)}개 중 최대 10개):")
        for m in mismatches[:10]:
            print(f"  '{m['original']}' → '{m['normalized']}'")
    
    return mapping

# ================================
# CSV 변환 관련 함수들
# ================================
def flatten_dict_ordered(
    data: Dict[str, Any],
    parent_key: str = "",
    sep: str = "_"
) -> OrderedDict:
    """
    dict를 재귀적으로 flatten (순서 보존)
    list는 JSON string으로 유지
    OrderedDict를 사용하여 원본 JSON의 키 순서를 보존
    """
    items = OrderedDict()
    
    # ✅ 타입 체크 추가 - dict가 아니면 빈 OrderedDict 반환
    if not isinstance(data, dict):
        return items
    
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k

        if isinstance(v, dict):
            # 재귀적으로 flatten하되 OrderedDict로 병합
            flattened = flatten_dict_ordered(v, new_key, sep)
            items.update(flattened)
        elif isinstance(v, list):
            items[new_key] = json.dumps(v, ensure_ascii=False)
        else:
            items[new_key] = v
    return items

def convert_json_to_csv(
    json_path: Path,
    output_csv_path: Path = None,
    csv_source: str = ""
) -> Path:
    
    print("\n" + "="*60)
    print("🔄 JSON → CSV 변환 시작")
    print("="*60)
    
    if output_csv_path is None:
        output_csv_path = json_path.parent / "all_keywords_suppliers_cn.csv"
    
    with open(json_path, "r", encoding="utf-8") as f:
        all_data = json.load(f)
    
    print(f"📂 소스: {json_path}")
    print(f"📊 키워드 수: {len(all_data)}")
    
    keyword_to_general = load_keyword_general_mapping(csv_source)
    
    # 모든 데이터를 rows로 변환
    all_rows = []
    total_suppliers = 0
    total_products = 0
    
    for keyword_data in all_data:
        keyword = keyword_data.get("keyword", "")
        pages = keyword_data.get("pages", [])
        
        # general_keyword 조회
        general_keyword = keyword_to_general.get(keyword, "")
        
        for page_data in pages:
            page_num = page_data.get("page", 0)
            data = page_data.get("data", {})
            
            # 실제 구조: data.model.offers
            model = data.get("model", {})
            offers = model.get("offers", [])
            
            if not offers:
                print(f"  ⚠️ {keyword} page {page_num}: offers 없음")
                continue
            
            now_iso = datetime.now().isoformat(timespec="seconds")
            
            for supplier_card in offers:
                total_suppliers += 1
                
                # ---------- meta ----------
                meta = OrderedDict([
                    ("general_keyword_네이버쇼핑", general_keyword),
                    ("meta_keyword", keyword),
                    ("meta_page", page_num),
                    ("meta_collected_at", now_iso),
                ])
                
                # ---------- supplier ----------
                supplier_flat = flatten_dict_ordered(supplier_card)
                supplier_ordered = OrderedDict()
                
                for k, v in supplier_flat.items():
                    if k == "productList":
                        continue
                    if k.startswith("productList_"):
                        continue
                    supplier_ordered[f"supplier_{k}"] = v
                            
                product_list = supplier_card.get("productList") or []
                
                # ---------- 1:N (supplier:products) ----------
                if product_list:
                    for p in product_list:
                        total_products += 1
                        
                        product_flat = flatten_dict_ordered(p)
                        product_ordered = OrderedDict()
                        
                        for k, v in product_flat.items():
                            product_ordered[f"product_{k}"] = v
                            
                        row = OrderedDict()
                        row.update(meta)
                        row.update(supplier_ordered)
                        row.update(product_ordered)
                        all_rows.append(row)
                else:
                    # 제품이 없는 제조사
                    row = OrderedDict()
                    row.update(meta)
                    row.update(supplier_ordered)
                    all_rows.append(row)
    
    print(f"📦 총 제조사 수: {total_suppliers}")
    print(f"📦 총 제품 수: {total_products}")
    
    if not all_rows:
        print("❌ 변환할 데이터가 없습니다.")
        return None
    
    # DataFrame 생성 (OrderedDict 순서 보존)
    df = pd.DataFrame(all_rows)
    
    # supplier_mainProducts 후처리
    if "supplier_mainProducts" in df.columns:
        print(f"\n🔧 supplier_mainProducts 후처리 중...")
        original_count = df["supplier_mainProducts"].notna().sum()

        # JSON → 텍스트 추출        
        df["supplier_mainProducts"] = df["supplier_mainProducts"].apply(
            extract_main_products_names
        )

        processed_count = df["supplier_mainProducts"].apply(lambda x: bool(x and x.strip())).sum()
        print(f"후처리 완료: {original_count}개 → {processed_count}개 (유효 데이터)")

    # 최종 컬럼 순서 재배치 및 저장
    print(f"\n📋 최종 컬럼 순서 재배치 중...")
    df = reorder_columns(df)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

    print(f"\nCSV 변환 완료: {output_csv_path}")
    print(f"📊 최종 행 수: {len(df):,}개")
    
    return output_csv_path


class AlibabaCollector:
    """
    역할:
    - 키워드 리스트 순회
    - 이미 수집된 키워드 건너뛰기
    - 실패 시 최대 3회 재시도 (Retry)
    - 실패 목록 즉시 저장
    """

    def __init__(self, output_root: str = OUTPUT_ROOT):
        self.output_root = Path(output_root)

    def run(self, keywords: List[Dict[str, str]]):
        total = len(keywords)
        print(f"[총 {total}개 키워드 수집 시작]")
        
        # 현재 진행 상황 확인
        progress = get_collection_progress(keywords, str(self.output_root))
        print(f"\n📊 진행 상황: {progress['progress']}")
        
        failed_keywords = []
        processed_count = 0

        # [1차 수집]
        for idx, item in enumerate(keywords, 1):
            keyword = item["keyword"]

            # 이미 수집 완료된 폴더는 건너뜀
            if is_keyword_already_collected(keyword, str(self.output_root)):
                continue

            processed_count += 1
            print(f"\n[{idx}/{total}] 🔍 키워드 수집 시도: {keyword}")

            success = collect_single_keyword(
                query=keyword,
                out_root=str(self.output_root),
                page_size=20,
                max_pages=20
            )
        
            if not success:
                print(f"⚠️ {keyword}: 1차 수집 실패")
                failed_keywords.append(keyword)
                # 실패 즉시 저장 (비정상 종료 대비)
                save_failed_keywords(self.output_root, failed_keywords)
            
            # 대기 시간
            time.sleep(random.uniform(KEYWORD_DELAY_MIN, KEYWORD_DELAY_MAX))
            if processed_count % BATCH_SIZE == 0:
                break_min = random.uniform(LONG_BREAK_MIN, LONG_BREAK_MAX)
                print(f"☕ 배치 휴식 중... ({break_min:.1f}분)")
                time.sleep(break_min * 60)

        # [2차 및 최종 재시도 로직]
        if failed_keywords:
            print("\n" + "="*60)
            print(f"🔄 실패한 키워드 {len(failed_keywords)}개에 대해 재시도를 시작합니다.")
            print("="*60)

            for retry_round in range(1, MAX_RETRY_PER_KEYWORD + 1):
                if not failed_keywords:
                    break
                
                print(f"\n🔄 [재시도 {retry_round}회차] 시작 (대상: {len(failed_keywords)}개)")
                still_failed = []

                for f_idx, f_kw in enumerate(failed_keywords, 1):
                    print(f"  ({f_idx}/{len(failed_keywords)}) 다시 시도 중: {f_kw}")
                    
                    # 재시도 시에는 약간 더 긴 대기 시간 부여
                    time.sleep(random.uniform(KEYWORD_DELAY_MIN * 1.5, KEYWORD_DELAY_MAX * 1.5))
                    
                    success = collect_single_keyword(f_kw, str(self.output_root))
                    
                    if success:
                        print(f"  ✅ {f_kw}: 재시도 성공!")
                    else:
                        print(f"  ❌ {f_kw}: 여전히 실패")
                        still_failed.append(f_kw)
                
                failed_keywords = still_failed
                save_failed_keywords(self.output_root, failed_keywords)

        print("\n" + "="*60)
        print(f"📊 최종 수집 완료 (남은 실패 키워드: {len(failed_keywords)}개)")
        print("="*60 + "\n")


def main(csv_path: str = "", auto_confirm: bool = False, output_root: str = None):

    global OUTPUT_ROOT
    if output_root:
        OUTPUT_ROOT = output_root
    else:
        _collect_dir = os.path.dirname(os.path.abspath(__file__))
        _base_dir = os.path.dirname(_collect_dir)  # alibaba_sourcing3/
        date_tag = datetime.now().strftime("%y%m%d")
        OUTPUT_ROOT = os.path.join(_base_dir, "sourcing_data", f"supplier_search_{date_tag}_cn")

    log_path = setup_logging()

    # 1) CSV 로드
    all_keywords = load_keywords_from_csv(csv_path)
    total_len = len(all_keywords)

    if total_len == 0:
        print("❌ 수집할 키워드가 없습니다.")
        return

    print(f"📊 전체 키워드 수: {total_len}개")
    print("-" * 40)

    # 2) 실행 확인
    if auto_confirm:
        sys.__stdout__.write(f"🚀 {total_len}개 키워드 수집을 자동으로 시작합니다. (auto_confirm)\n")
    else:
        sys.__stdout__.write(f"🚀 {total_len}개 키워드 수집을 시작하시겠습니까? (y/N): ")
        sys.__stdout__.flush()
        confirm = sys.__stdin__.readline().strip()
        if confirm.lower() not in ("y", "yes"):
            sys.__stdout__.write("❌ 실행이 취소되었습니다.\n")
            return

    # 3) RAW 수집 (전체 키워드)
    collector = AlibabaCollector(output_root=OUTPUT_ROOT)
    collector.run(all_keywords)

    # 4) 모든 키워드의 JSON을 하나로 통합
    output_json = merge_all_keywords_json(Path(OUTPUT_ROOT))
    if output_json:
        output_csv = convert_json_to_csv(output_json, csv_source=csv_path)
        
        sys.__stdout__.write(f"\n✅ 모든 작업 완료!\n")
        sys.__stdout__.write(f"📂 로그: {log_path}\n")
        if output_csv:
            sys.__stdout__.write(f"📊 CSV: {output_csv}\n")
            sys.__stdout__.write(f"📄 JSON: {output_json}\n")
    else:
        sys.__stdout__.write(f"\n✅ 모든 작업 완료! 로그를 확인하세요.\n📂 {log_path}\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = sorted(glob.glob(os.path.join(script_dir, "zh_keywords_*.csv")),
                            key=os.path.getmtime, reverse=True)
        if not candidates:
            sys.__stdout__.write("❌ zh_keywords_*.csv 파일을 찾을 수 없습니다.\n")
            sys.exit(1)
        csv_file = candidates[0]
        sys.__stdout__.write(f"📂 자동 탐색된 CSV: {os.path.basename(csv_file)}\n")
    main(csv_file,output_root=OUTPUT_ROOT)