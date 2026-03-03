# global_sourcing
## 📁 프로젝트 구조
```
alibaba_sourcing3/
├── main.py
├── 0_input/              ← 원본 입력
├── 1_output/날짜/        ← 단계별 결과 CSV
├── 2_logs/               ← 파이프라인 로그
└── scripts/              ← 모든 스크립트 + CSV 설정 파일
    ├── 01_convert_gk.py
    ├── 02_translate_en_cn.py
    ├── 03_collect_en.py
    ├── 04_collect_cn.py
    ├── 05_combine_column.py
    ├── 06_relevance.py
    ├── 07_true.py
    ├── 08_verify.py
    ├── 09_valid.py
    ├── 10_master_kor.py
    ├── master_column.csv
    └── master_column_list_before_relev.csv
```
