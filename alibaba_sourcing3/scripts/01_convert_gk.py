#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import glob
import pandas as pd


def add_car_prefix(keyword):
    """
    키워드에 '차량용'이 없으면 앞에 추가
    이미 있으면 그대로 반환
    '_'와 공백 제거
    """
    if pd.isna(keyword):
        return keyword

    keyword = str(keyword).strip()

    # 1. _ 제거
    keyword = keyword.replace("_", "")

    # 2. 공백 제거
    keyword = keyword.replace(" ", "")

    # 3. '차량용' 포함 여부 확인
    if "차량용" in keyword:
        return keyword
    else:
        return f"차량용{keyword}"


def read_csv_safe(path):
    """
    CSV 인코딩 자동 대응
    """
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise RuntimeError("CSV 인코딩을 판별할 수 없습니다.")


def main(input_csv: str, output_csv: str):
    # CSV 읽기 (안전하게)
    df = read_csv_safe(input_csv)

    # 필수 컬럼 확인
    col = "general_keyword_네이버쇼핑"
    if col not in df.columns:
        raise RuntimeError(f"입력 CSV에 '{col}' 컬럼이 없습니다.")

    # 컬럼 생성
    df["general_keyword_car"] = df[col].apply(add_car_prefix)

    # CSV 저장 (엑셀 더블클릭 대응)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")

    print(f"완료! 저장됨 → {output_csv}")
    print(f"총 {len(df)} 행 처리됨")

    print("\n결과 샘플:")
    print(df[[col, "general_keyword_car"]].head(10))


if __name__ == "__main__":
    _dir = os.path.dirname(os.path.abspath(__file__))
    _matches = sorted(glob.glob(os.path.join(_dir, "0_input_gk_*.csv")))
    if not _matches:
        raise FileNotFoundError(f"0_input_gk_*.csv 파일을 찾을 수 없습니다: {_dir}")
    _input = max(_matches, key=os.path.getmtime)
    _tag = os.path.basename(_input).replace("0_input_gk_", "").replace(".csv", "")
    _output = os.path.join(_dir, f"0_output_gk_{_tag}.csv")
    main(_input, _output)
