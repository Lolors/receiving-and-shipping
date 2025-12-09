import streamlit as st
import pandas as pd
from datetime import date, timedelta
import tempfile
import io
import os
from html import escape
from pathlib import Path

# ============ S3 연동 ============

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = "rec-and-ship"
S3_KEY_EXCEL = "bulk-ledger.xlsx"   # 기존 엑셀
S3_KEY_LABEL = "label_db.csv"       # 🔸 라벨 전용 DB (CSV)

def get_s3_client():
    try:
        return boto3.client(
            "s3",
            aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"],
            region_name="ap-northeast-2",
        )
    except Exception as e:
        st.error(f"S3 클라이언트를 생성하는 중 오류 발생: {e}")
        return None

s3_client = get_s3_client()


@st.cache_data(show_spinner=True)
def load_file_from_s3():
    """S3에 엑셀 파일이 있으면 bytes로 읽어온다."""
    if s3_client is None:
        return None
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_EXCEL)  # 🔴 여기 S3_KEY → S3_KEY_EXCEL 로 수정
        return obj["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return None
        st.error(f"S3에서 파일을 가져오는 중 오류가 발생했습니다: {e}")
        return None

@st.cache_data(show_spinner=True)
def load_label_db_from_s3() -> pd.DataFrame:
    """
    S3에서 라벨 DB CSV를 읽어 DataFrame으로 반환.
    없으면 빈 DF 반환.
    """
    if s3_client is None:
        return pd.DataFrame()

    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_LABEL)
        data = obj["Body"].read().decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(data))
        return df
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            # 아직 라벨 DB를 만든 적이 없음
            return pd.DataFrame()
        st.error(f"S3에서 라벨 DB를 가져오는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()


def save_label_db_to_s3(df: pd.DataFrame):
    """
    현재 라벨 DB DataFrame을 S3에 CSV로 저장.
    """
    if s3_client is None:
        st.error("S3 클라이언트가 없습니다. 라벨 DB를 저장할 수 없습니다.")
        return

    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=S3_KEY_LABEL,
        Body=csv_buf.getvalue().encode("utf-8-sig"),
    )
    # 캐시된 라벨 DB 무효화
    load_label_db_from_s3.clear()


# PDF 생성용 (reportlab 없는 환경에서도 앱이 죽지 않도록 처리)
try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate,
        Table,
        TableStyle,
        Paragraph,
        Spacer,
    )
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    KOREAN_FONT_NAME = "MalgunGothic"

    # 🔹 app.py 기준으로 font/malgun.ttf 절대 경로 만들기
    FONT_PATH = os.path.join(os.path.dirname(__file__), "font", "malgun.ttf")

    if not os.path.exists(FONT_PATH):
        st.write("⚠️ 폰트 파일을 찾지 못했습니다:", FONT_PATH)
        KOREAN_FONT_NAME = "Helvetica"
    else:
        try:
            pdfmetrics.registerFont(TTFont(KOREAN_FONT_NAME, FONT_PATH))
        except Exception as e:
            st.write("⚠️ 폰트 로딩 실패:", repr(e))
            KOREAN_FONT_NAME = "Helvetica"

    REPORTLAB_AVAILABLE = True
except ModuleNotFoundError:
    REPORTLAB_AVAILABLE = False
    KOREAN_FONT_NAME = "Helvetica"


st.set_page_config(page_title="부자재 관리 시스템", layout="wide")

# -----------------------------
# 유틸 함수
# -----------------------------
@st.cache_data
def load_excel(file_bytes: bytes):
    """bytes 또는 파일 객체를 받아 전체 시트를 dict로 반환"""
    xls = pd.ExcelFile(file_bytes)
    sheets = {}
    for sheet_name in xls.sheet_names:
        try:
            sheets[sheet_name] = pd.read_excel(xls, sheet_name)
        except Exception:
            pass
    return sheets


def get_week_of_month(d: date) -> str:
    """간단히: 1~7일=1주차, 8~14=2주차, ..."""
    week_no = (d.day - 1) // 7 + 1
    return f"{d.month}월{week_no}주차"


def ensure_session_df(key: str, columns: list):
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame(columns=columns)
    return st.session_state[key]


def excel_col_to_index(col_letter: str) -> int:
    """엑셀 열 문자(A, B, ... AA, AB...)를 0-base index로 변환"""
    col_letter = col_letter.upper()
    result = 0
    for ch in col_letter:
        if not ("A" <= ch <= "Z"):
            continue
        result = result * 26 + (ord(ch) - ord("A") + 1)
    return result - 1  # 0-base


def pick_col(df: pd.DataFrame, letter: str, preferred_names: list):
    """
    우선 컬럼명으로 찾고, 없으면 엑셀 열 위치(letter)로 찾기
    (preferred_names 중 하나라도 있으면 그걸 우선 사용)
    """
    cols = list(df.columns)
    for name in preferred_names:
        if name in df.columns:
            return name
    idx = excel_col_to_index(letter)
    if 0 <= idx < len(cols):
        return cols[idx]
    return None


def safe_num(x):
    """숫자가 아니면 최대한 float으로 변환, 안 되면 0"""
    try:
        if pd.isna(x):
            return 0
    except Exception:
        pass
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(str(x).replace(",", ""))
    except Exception:
        return 0.0

import re

LABEL_TYPES = [
    "봉합라벨",
    "리실러블라벨",
    "용기라벨",
    "상단라벨",
    "용기전면라벨",
    "용기후면라벨",
    "용기상단라벨",
    "용기우측라벨",
    "용기좌측라벨",
    "엠블럼",
    "실링지",
    "덧방라벨",
]

def parse_label_db(file_obj) -> pd.DataFrame:
    """
    기존 '라벨 및 스티커 지관무게+수량 계산기_*.xlsx' 파일에서
    라벨 DB를 뽑아서 통일된 컬럼명으로 정리한다.

    - 사용 시트: '라벨 및 스티커'
    - 헤더 행: 5번째 줄(0-base index=4)
    - 주요 컬럼 매핑:
        No.        → 샘플번호
        품번       → 품번
        품명       → 품명
        구분       → 구분
        실무게     → 지관무게
        추정값     → 추정값
        오차       → 오차
        외경       → 외경
        내경       → 내경
        높이       → 높이
        1R무게     → 1R무게
        기준 샘플  → 기준샘플
        샘플무게   → 샘플무게
    """
    try:
        xls = pd.ExcelFile(file_obj)
    except Exception as e:
        st.error(f"라벨 엑셀 파일을 여는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

    # 시트 이름 찾기 (정확히 '라벨 및 스티커'가 있으면 그걸 최우선)
    sheet_name = None
    for s in xls.sheet_names:
        if "라벨" in s and "스티커" in s:
            sheet_name = s
            break
    if sheet_name is None:
        # 없으면 첫 번째 시트
        sheet_name = xls.sheet_names[0]

    # header=4 → 5번째 줄을 헤더로 사용 (실제 파일 구조 기준)
    try:
        df_raw = pd.read_excel(xls, sheet_name=sheet_name, header=4)
    except Exception as e:
        st.error(f"라벨 시트를 읽는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

    # 컬럼 이름 정리
    rename_map = {}
    cols = list(df_raw.columns)

    for c in cols:
        cname = str(c).strip()
        if cname == "No.":
            rename_map[c] = "샘플번호"
        elif cname == "품번":
            rename_map[c] = "품번"
        elif cname == "품명":
            rename_map[c] = "품명"
        elif cname == "구분":
            rename_map[c] = "구분"
        elif cname == "실무게":
            rename_map[c] = "지관무게"
        elif cname == "추정값":
            rename_map[c] = "추정값"
        elif cname == "오차":
            rename_map[c] = "오차"
        elif cname == "외경":
            rename_map[c] = "외경"
        elif cname == "내경":
            rename_map[c] = "내경"
        elif cname == "높이":
            rename_map[c] = "높이"
        elif cname == "1R무게":
            rename_map[c] = "1R무게"
        elif cname.replace(" ", "") in ("기준샘플", "기준샘플"):
            rename_map[c] = "기준샘플"
        elif cname.replace(" ", "") in ("샘플무게", "샘플무게"):
            rename_map[c] = "샘플무게"

    df = df_raw.rename(columns=rename_map)

    # 우리가 쓸 컬럼만 골라서 새 DF 구성
    base_cols = [
        "샘플번호",
        "품번",
        "품명",
        "구분",
        "지관무게",
        "추정값",
        "오차",
        "외경",
        "내경",
        "높이",
        "1R무게",
        "기준샘플",
        "샘플무게",
    ]
    existing = [c for c in base_cols if c in df.columns]
    df_out = df[existing].copy()

    # 구분이 정해진 12개 중 하나인 행만 사용 (쓰레기 행 제거 용도)
    if "구분" in df_out.columns:
        df_out = df_out[df_out["구분"].isin(LABEL_TYPES)]

    # 품번/품명 둘 다 없는 행은 버리기
    if "품번" in df_out.columns and "품명" in df_out.columns:
        df_out = df_out.dropna(subset=["품번", "품명"], how="all")

    # 숫자 컬럼 float 변환
    num_cols = ["지관무게", "추정값", "오차", "외경", "내경", "높이", "1R무게", "샘플무게"]
    for c in num_cols:
        if c in df_out.columns:
            df_out[c] = df_out[c].apply(safe_num)

    # 인덱스 리셋
    df_out = df_out.reset_index(drop=True)

    return df_out


def parse_label_sample_count(text: str) -> float:
    """
    기준샘플 문자열에서 '몇 매'인지 숫자만 뽑아서 float으로 반환.
    예) '4매' → 4, '2매(아이마크)' → 2, '1매' → 1
    숫자가 없으면 1로 처리.
    """
    if pd.isna(text):
        return 1.0
    s = str(text)
    m = re.search(r"(\d+)", s)
    if not m:
        return 1.0
    try:
        return float(m.group(1))
    except Exception:
        return 1.0

# 라벨 DF를 한 번 정리해 주는 공통 함수
def normalize_label_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    라벨 DB DataFrame을 표준 형태로 정리한다.

    - 필수 컬럼이 없으면 추가
    - 숫자 컬럼은 safe_num으로 float 변환
    - 외경/내경/높이가 있는데 추정값이 없거나 0이면 공식으로 재계산
    - 지관무게가 있으면 오차(추정값-지관무게) 재계산
    """
    df = df.copy()

    # 필수 컬럼 세트
    required_cols = [
        "샘플번호",
        "품번",
        "품명",
        "구분",
        "지관무게",
        "추정값",
        "오차",
        "외경",
        "내경",
        "높이",
        "1R무게",
        "기준샘플",
        "샘플무게",
    ]

    for c in required_cols:
        if c not in df.columns:
            df[c] = None

    # 숫자 컬럼은 safe_num으로 통일
    num_cols = ["지관무게", "추정값", "오차", "외경", "내경", "높이", "1R무게", "샘플무게"]
    for c in num_cols:
        df[c] = df[c].apply(safe_num)

    # 구분이 있으면 LABEL_TYPES 안에 있는 값만 남기기 (있을 때만)
    if "구분" in df.columns and "LABEL_TYPES" in globals():
        mask = df["구분"].isin(LABEL_TYPES) | df["구분"].isna()
        df = df[mask]

    # 추정값 재계산 (외경/내경/높이가 있을 때, 추정값이 0 또는 NaN인 경우)
    def _recalc_est(row):
        od = safe_num(row["외경"])
        inner = safe_num(row["내경"])
        h = safe_num(row["높이"])
        est = safe_num(row["추정값"])

        if od > 0 and inner > 0 and h > 0 and est <= 0:
            est = 3.14 * h * ((od ** 2 - inner ** 2) / 4.0) * 0.78
        return round(est, 2) if est != 0 else est

    df["추정값"] = df.apply(_recalc_est, axis=1)

    # 오차 재계산 (지관무게가 있을 때만)
    def _recalc_err(row):
        core = safe_num(row["지관무게"])
        est = safe_num(row["추정값"])
        if core > 0 and est > 0:
            return est - core
        return safe_num(row["오차"])

    df["오차"] = df.apply(_recalc_err, axis=1)

    df = df.reset_index(drop=True)
    return df


# 화면에 보이는 환입 예상재고 테이블 컬럼 순서
VISIBLE_COLS = [
    "수주번호",
    "완성품번",
    "품번",
    "품명",
    "ERP불출수량",
    "현장실물입고",
    "지시수량",
    "생산수량",
    "QC샘플",
    "기타샘플",
    "단위수량",
    "원불",
    "작불",
    "예상재고",
    "ERP재고",
]

# CSV에 들어갈 전체 컬럼 (요청한 순서 그대로)
CSV_COLS = [
    "수주번호",
    "지시번호",
    "생산공정",
    "생산시작일",
    "생산종료일",
    "종료조건",
    "환입일",
    "환입주차",
    "완성품번",
    "완성품명",
    "품번",
    "품명",
    "ERP불출수량",
    "현장실물입고",
    "지시수량",
    "생산수량",
    "QC샘플",
    "기타샘플",
    "단위수량",
    "원불",
    "작불",
    "예상재고",
    "ERP재고",
]

# --------
# 기간 기준 입고 수량 합계
# --------

def get_real_in_by_period(part_code, start_date, end_date):
    """
    품번(part_code)과 입고 기간(start_date ~ end_date)을 기준으로
    입고 시트(df_in_raw)에서 '현장실물입고' 합계를 구한다.
    """
    df = df_in_raw.copy()

    # 날짜 / 품번 / 실물입고 컬럼 찾기
    date_col = pick_col(df, "K", ["요청날짜", "요청일"])
    part_col = pick_col(df, "M", ["품번"])
    real_col = pick_col(df, "R", ["현장실물입고"])

    if not all([date_col, part_col, real_col]):
        return 0.0  # 필수 컬럼 없으면 0 리턴

    # 날짜형 변환
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # 기간 + 품번으로 필터
    mask = (
        (df[part_col].astype(str) == str(part_code))
        & (df[date_col] >= start_date)
        & (df[date_col] <= end_date)
    )

    sub = df.loc[mask, real_col]

    if sub.empty:
        return 0.0

    return sub.apply(safe_num).sum()

# -----
# 추가수주번호 찾기
# ------

def get_extra_orders_by_period(part_code, base_suju, start_date, end_date):
    """
    입고 시트(df_in_raw)에서
    - 품번(part_code)
    - 요청날짜: start_date ~ end_date
    조건에 해당하는 수주번호들을 찾아서,
    기본 수주번호(base_suju)는 제외하고
    중복 없이 쉼표로 이어붙인 문자열을 반환한다.
    """
    df = df_in_raw.copy()

    date_col = pick_col(df, "K", ["요청날짜", "요청일"])
    part_col = pick_col(df, "M", ["품번"])
    suju_col = pick_col(df, "B", ["수주번호"])

    if not all([date_col, part_col, suju_col]):
        return ""

    # 날짜형 변환
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date

    # 필터: 품번 + 기간
    mask = (
        (df[part_col].astype(str) == str(part_code))
        & (df[date_col] >= start_date)
        & (df[date_col] <= end_date)
    )

    sub = df.loc[mask, suju_col]

    if sub.empty:
        return ""

    # 수주번호들 정리
    suju_list = (
        sub.dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    # 기본 수주번호 제외
    suju_list = [s for s in suju_list if s != str(base_suju)]

    if not suju_list:
        return ""

    # 쉼표로 이어붙여서 반환
    return ", ".join(suju_list)


# -----------------------------
# 집계 테이블 빌드
# -----------------------------
def build_aggregates(df_in_raw, df_job_raw, df_result_raw, df_defect_raw, df_stock_raw):
    """
    큰 원본 시트들을 미리 groupby 해서, 나중엔 merge만 하도록 만드는 집계 테이블들
    """
    aggregates = {}

    # === 1) 입고 집계: [수주번호, 지시번호, 품번] 별 ERP불출수량/현장실물입고 합계 ===
    # 수주번호: B열, 지시번호: C열, 품번: M열, ERP불출수량: Q열, 현장실물입고: R열
    in_suju_col = pick_col(df_in_raw, "B", ["수주번호"])
    in_jisi_col = pick_col(df_in_raw, "C", ["지시번호"])
    in_part_col = pick_col(df_in_raw, "M", ["품번"])
    in_erp_col = pick_col(df_in_raw, "Q", ["ERP불출수량"])
    in_real_col = pick_col(df_in_raw, "R", ["현장실물입고"])

    if all([in_suju_col, in_jisi_col, in_part_col, in_erp_col, in_real_col]):
        df_in = df_in_raw[
            [in_suju_col, in_jisi_col, in_part_col, in_erp_col, in_real_col]
        ].copy()
        df_in.columns = ["수주번호", "지시번호", "품번", "ERP불출수량", "현장실물입고"]
        agg_in = (
            df_in.groupby(["수주번호", "지시번호", "품번"], as_index=False)
            .agg({"ERP불출수량": "sum", "현장실물입고": "sum"})
        )
        aggregates["in"] = agg_in
    else:
        aggregates["in"] = pd.DataFrame(
            columns=["수주번호", "지시번호", "품번", "ERP불출수량", "현장실물입고"]
        )

    # === 2) 작업지시 집계: 지시번호별 지시수량 ===
    job_jisi_col = (
        "지시번호"
        if "지시번호" in df_job_raw.columns
        else pick_col(df_job_raw, "F", ["지시번호"])
    )
    job_qty_col = (
        "수량"
        if "수량" in df_job_raw.columns
        else pick_col(df_job_raw, "R", ["수량", "지시수량"])
    )

    if job_jisi_col and job_qty_col:
        df_job = df_job_raw[[job_jisi_col, job_qty_col]].copy()
        df_job.columns = ["지시번호", "지시수량"]
        agg_job = df_job.groupby("지시번호", as_index=False).agg({"지시수량": "sum"})
        aggregates["job"] = agg_job
    else:
        aggregates["job"] = pd.DataFrame(columns=["지시번호", "지시수량"])

    # === 3) 생산실적 집계: 지시번호(작지번호)별 양품 / QC샘플 / 기타샘플 합계 ===
    # 작지번호: 보통 "작지번호" 컬럼 사용 (A열)
    res_jisi_col = (
        "작지번호"
        if "작지번호" in df_result_raw.columns
        else pick_col(df_result_raw, "A", ["작지번호", "지시번호"])
    )

    # 수주번호: 있으면 같이 들고만 다니다가 필요할 때 사용
    res_suju_col = (
        "수주번호"
        if "수주번호" in df_result_raw.columns
        else pick_col(df_result_raw, "E", ["수주번호"])
    )

    # 양품(실제 생산수량) 컬럼 찾기
    res_good_col = None
    for cand in ["양품", "양품수량", "양품수", "합격", "생산수량"]:
        if cand in df_result_raw.columns:
            res_good_col = cand
            break

    # QC샘플: AG열, 기타샘플: AH열 기준으로 컬럼 찾기
    res_qc_col = pick_col(df_result_raw, "AG", ["QC샘플"])
    res_etc_col = pick_col(df_result_raw, "AH", ["기타샘플"])

    # 최소한 지시번호(작지번호)나 수주번호 둘 중 하나는 있어야 집계 가능
    if res_jisi_col or res_suju_col:
        use_cols = []
        if res_jisi_col:
            use_cols.append(res_jisi_col)
        if res_suju_col:
            use_cols.append(res_suju_col)
        if res_good_col:
            use_cols.append(res_good_col)
        if res_qc_col:
            use_cols.append(res_qc_col)
        if res_etc_col:
            use_cols.append(res_etc_col)

        df_res = df_result_raw[use_cols].copy()

        # 컬럼명 통일
        rename_map = {}
        if res_jisi_col:
            rename_map[res_jisi_col] = "지시번호"
        if res_suju_col:
            rename_map[res_suju_col] = "수주번호"
        if res_good_col:
            rename_map[res_good_col] = "생산수량"
        if res_qc_col:
            rename_map[res_qc_col] = "QC샘플"
        if res_etc_col:
            rename_map[res_etc_col] = "기타샘플"

        df_res = df_res.rename(columns=rename_map)

        # NaN → 0 처리
        for col in ["생산수량", "QC샘플", "기타샘플"]:
            if col in df_res.columns:
                df_res[col] = df_res[col].apply(safe_num)

        # ✅ 기준 키: 지시번호가 있으면 지시번호로, 없으면 기존처럼 수주번호로
        group_keys = []
        if "지시번호" in df_res.columns:
            group_keys.append("지시번호")
        elif "수주번호" in df_res.columns:
            group_keys.append("수주번호")

        # 집계 방식 정의
        agg_dict = {}
        for col in df_res.columns:
            if col in group_keys:
                continue
            if col in ["생산수량", "QC샘플", "기타샘플"]:
                agg_dict[col] = "sum"
            elif col == "수주번호" and "지시번호" in group_keys:
                # 지시번호 기준으로 묶을 때 수주번호는 대표값 하나만
                agg_dict[col] = "first"
            else:
                agg_dict[col] = "first"

        agg_res = df_res.groupby(group_keys, as_index=False).agg(agg_dict)
        aggregates["result"] = agg_res
    else:
        # 둘 다 없으면 빈 DF
        aggregates["result"] = pd.DataFrame(
            columns=["지시번호", "수주번호", "생산수량", "QC샘플", "기타샘플"]
        )


    # === 4) 불량 집계: [지시번호, 품번]별 원불/작불 수량 ===
    def_jisi_col = (
        "작지번호"
        if "작지번호" in df_defect_raw.columns
        else pick_col(df_defect_raw, "C", ["작지번호"])
    )
    def_part_col = (
        "투입품번"
        if "투입품번" in df_defect_raw.columns
        else pick_col(df_defect_raw, "Q", ["투입품번"])
    )
    def_qty_col = (
        "불량수량"
        if "불량수량" in df_defect_raw.columns
        else pick_col(df_defect_raw, "W", ["불량수량"])
    )
    def_type_col = (
        "불량유형.1"
        if "불량유형.1" in df_defect_raw.columns
        else pick_col(df_defect_raw, "Z", ["불량유형.1", "불량유형"])
    )

    if def_jisi_col and def_part_col and def_qty_col and def_type_col:
        df_def = df_defect_raw[
            [def_jisi_col, def_part_col, def_qty_col, def_type_col]
        ].copy()
        df_def.columns = ["지시번호", "품번", "불량수량", "불량유형"]
        df_def["불량유형"] = df_def["불량유형"].astype(str)

        # 원불
        df_orig = df_def[df_def["불량유형"].str.startswith("(원)")].copy()
        agg_orig = (
            df_orig.groupby(["지시번호", "품번"], as_index=False)["불량수량"]
            .sum()
            .rename(columns={"불량수량": "원불"})
        )

        # 작불
        df_proc = df_def[df_def["불량유형"].str.startswith("(작)")].copy()
        agg_proc = (
            df_proc.groupby(["지시번호", "품번"], as_index=False)["불량수량"]
            .sum()
            .rename(columns={"불량수량": "작불"})
        )

        # 둘 합치기
        agg_def = pd.merge(agg_orig, agg_proc, on=["지시번호", "품번"], how="outer")
        aggregates["defect"] = agg_def
    else:
        aggregates["defect"] = pd.DataFrame(
            columns=["지시번호", "품번", "원불", "작불"]
        )

    # === 5) 재고 집계: 품번별 ERP재고 (작업장 WC501~WC504) ===
    stock_wc_col = pick_col(df_stock_raw, "A", ["작업장"])
    stock_part_col = pick_col(df_stock_raw, "D", ["품번"])

    # ERP재고는 반드시 "실재고수량" 컬럼을 사용 (없으면 N열 fallback)
    if "실재고수량" in df_stock_raw.columns:
        stock_qty_col = "실재고수량"
    else:
        stock_qty_col = pick_col(df_stock_raw, "N", ["실재고수량"])

    if stock_wc_col and stock_part_col and stock_qty_col:
        df_stock = df_stock_raw[
            [stock_wc_col, stock_part_col, stock_qty_col]
        ].copy()
        df_stock.columns = ["작업장", "품번", "실재고수량"]
        df_stock = df_stock[df_stock["작업장"].isin(["WC501", "WC502", "WC503", "WC504"])]
        if not df_stock.empty:
            agg_stock = (
                df_stock.groupby("품번", as_index=False)["실재고수량"]
                .sum()
                .rename(columns={"실재고수량": "ERP재고"})
            )
            aggregates["stock"] = agg_stock
        else:
            aggregates["stock"] = pd.DataFrame(columns=["품번", "ERP재고"])
    else:
        aggregates["stock"] = pd.DataFrame(columns=["품번", "ERP재고"])

    return aggregates


# -----------------------------
# 환입 예상재고 계산 (merge 기반)
# -----------------------------
def recalc_return_expectation(df_return, aggs):
    """
    df_return(환입관리 테이블)에 집계 데이터(aggs)를 merge로 붙여서
    ERP불출수량, 현장실물입고, 지시수량, 생산수량, QC샘플, 기타샘플, 원불, 작불, ERP재고, 예상재고를 계산

    예상재고 = 현장실물입고 - (생산수량 + QC샘플 + 기타샘플) * 단위수량 - 작불
    """
    if df_return.empty:
        return pd.DataFrame(columns=CSV_COLS)

    # [수주번호, 지시번호, 품번] 기준 중복 제거
    df = df_return.drop_duplicates(
        subset=["수주번호", "지시번호", "품번"], keep="last"
    ).copy()

    # 1) 입고 집계 붙이기
    df = df.merge(
        aggs["in"],
        how="left",
        on=["수주번호", "지시번호", "품번"],
        suffixes=("", "_in"),
    )

    # 2) 작업지시 집계 붙이기
    df = df.merge(
        aggs["job"],
        how="left",
        on="지시번호",
    )

    # 3) 생산실적 집계 붙이기
    res_tbl = aggs["result"]

    # 새 방식: 지시번호(작지번호) 기준 집계가 되어 있는 경우
    if isinstance(res_tbl, pd.DataFrame) and not res_tbl.empty and "지시번호" in res_tbl.columns:
        merge_cols = ["지시번호"]
        for c in ["생산수량", "QC샘플", "기타샘플"]:
            if c in res_tbl.columns:
                merge_cols.append(c)

        df = df.merge(
            res_tbl[merge_cols],
            how="left",
            on="지시번호",
        )
    else:
        # 혹시라도 지시번호 집계가 안 되어 있는 구버전 구조일 때는
        # 기존대로 수주번호 기준으로 붙이도록 fallback
        df = df.merge(
            res_tbl,
            how="left",
            on="수주번호",
        )

    # 4) 불량 집계 붙이기
    df = df.merge(
        aggs["defect"],
        how="left",
        on=["지시번호", "품번"],
    )

    # 5) 재고 집계 붙이기
    if "ERP재고" in df.columns:
        df = df.drop(columns=["ERP재고"])
    df = df.merge(
        aggs["stock"],
        how="left",
        on="품번",
    )

    # 숫자 컬럼들 NaN -> 0
    num_cols = [
        "ERP불출수량",
        "현장실물입고",
        "지시수량",
        "생산수량",
        "QC샘플",
        "기타샘플",
        "단위수량",
        "원불",
        "작불",
        "ERP재고",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = df[col].apply(safe_num)
        else:
            df[col] = 0.0

    # ✅ 네가 말한 공식 그대로
    df["예상재고"] = (
        df["현장실물입고"]
        - (df["생산수량"] + df["QC샘플"] + df["기타샘플"]) * df["단위수량"]
        - df["원불"]
        - df["작불"]
    )

    # 완성품명은 제품명 컬럼 그대로 사용
    df["완성품명"] = df.get("제품명", None)

    # CSV용 전체 컬럼만 추출
    for col in CSV_COLS:
        if col not in df.columns:
            df[col] = None

    out = df[CSV_COLS].copy()
    return out

# -----------------------------
# PDF 생성 함수
# -----------------------------
if REPORTLAB_AVAILABLE:
    from xml.sax.saxutils import escape
    from reportlab.graphics.barcode import code128
    from reportlab.graphics.shapes import Drawing
    from reportlab.lib.units import mm
    from reportlab.platypus import PageBreak

    def generate_pdf(
        df_export: pd.DataFrame,
        uploaded_image=None,
        pasted_text: str | None = None,
    ) -> bytes:
        """
        - 제목 / 표 모두 왼쪽 정렬
        - pasted_text가 있으면 제목 아래에 그대로 출력
        - uploaded_image는 지금은 안 써도 됨(차후 확장용)
        """
        import io
        from reportlab.platypus import (
            SimpleDocTemplate,
            Table,
            TableStyle,
            Paragraph,
            Spacer,
            Image,
        )
        from reportlab.lib.pagesizes import A4, landscape
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib import colors

        buffer = io.BytesIO()

        doc = SimpleDocTemplate(
            buffer,
            pagesize=landscape(A4),
            leftMargin=20,
            rightMargin=20,
            topMargin=20,
            bottomMargin=20,
        )

        styles = getSampleStyleSheet()

        title_style = ParagraphStyle(
            "TitleStyle",
            parent=styles["Heading1"],
            fontName=KOREAN_FONT_NAME,
            fontSize=15,
            alignment=0,   # LEFT
        )

        text_style = ParagraphStyle(
            "TextStyle",
            parent=styles["Normal"],
            fontName=KOREAN_FONT_NAME,
            fontSize=10,
            leading=14,
            alignment=0,   # LEFT
        )

        table_style = TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),  # 표 전체 왼쪽 정렬
                ("FONTNAME", (0, 0), (-1, -1), KOREAN_FONT_NAME),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),

                ("BOTTOMPADDING", (0, 0), (-1, -1), 20),
                ("TOPPADDING",    (0, 0), (-1, -1), 20),
                ("MINROWHEIGHT",  (0, 0), (-1, -1), 35),
            ]
        )

        story = []

        # 1) 제목
        suju_list = df_export["수주번호"].dropna().astype(str).unique()
        name_list = df_export["완성품명"].dropna().astype(str).unique()
        title_text = f"{suju_list[0] if len(suju_list) else ''} {name_list[0] if len(name_list) else ''}".strip()

        story.append(Paragraph(title_text, title_style))
        story.append(Spacer(1, 12))

        # 2) 상단 메모 (텍스트)
        if pasted_text is not None and pasted_text.strip() != "":
            # <, >, & 등 이스케이프 + 줄바꿈을 <br/>로 변환
            safe_text = escape(pasted_text).replace("\n", "<br/>")
            story.append(Paragraph(safe_text, text_style))
            story.append(Spacer(1, 12))

        # 3) (원하면 이미지도 여기에)
        if uploaded_image:
            try:
                img = Image(uploaded_image, width=400, height=300)
                story.append(img)
                story.append(Spacer(1, 12))
            except Exception:
                pass

        # 표 구성: 기존 + 1P, 2P, 3P, 4P 4칸 추가
        base_cols = ["품번", "품명", "작불", "예상재고", "ERP재고"]
        table_cols = base_cols + ["1P", "2P", "3P", "4P"]
        table_data = [table_cols]

        for _, row in df_export.iterrows():
            # df_export 에는 1P~4P 컬럼이 없으니까, 기존 데이터만 넣고 4칸은 공백으로 채움
            base_values = [str(row.get(c, "")) for c in base_cols]
            extra_values = ["", "", "", ""]  # 1P, 2P, 3P, 4P
            table_data.append(base_values + extra_values)

        # 행 높이 (헤더는 기본, 데이터 행만 높게)
        default_height = None        # 헤더
        data_height = 40             # 데이터 행
        row_heights = [default_height] + [data_height] * (len(table_data) - 1)

        # 컬럼 폭 설정
        #  - 앞의 5개 컬럼은 None(자동)
        #  - 1P~4P 4칸만 넓게
        col_widths = [None, None, None, None, None, 130, 130, 80, 80]

        table = Table(
            table_data,
            repeatRows=1,
            rowHeights=row_heights,
            colWidths=col_widths,
            hAlign="LEFT",   # 표 전체 왼쪽 정렬
        )

        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                    ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                    ("FONTNAME", (0, 0), (-1, -1), KOREAN_FONT_NAME),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),

                    ("LEFTPADDING", (0, 0), (-1, -1), 0),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 4),

                    # 데이터 행만 위/아래 여백 크게
                    ("TOPPADDING",    (0, 1), (-1, -1), 12),
                    ("BOTTOMPADDING", (0, 1), (-1, -1), 12),
                ]
            )
        )

        story.append(table)

        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes

    # 🔹 소형 라벨프린터(100×120mm)용 부자재반입 라벨 PDF
    def generate_label_pdf(df_labels: pd.DataFrame, barcode_value: str, unit_value: str) -> bytes:
        """
        df_labels: '품명', '품번', '환입일' 컬럼을 가진 DataFrame
        barcode_value: 사용자가 입력한 바코드 값 (예: B202511-00120001)
        unit_value: 사용자가 입력한 단위수량
        """
        import io
        from reportlab.platypus import (
            SimpleDocTemplate,
            Paragraph,
            Spacer,
            PageBreak,
            Flowable,
            Table,
            TableStyle,
        )
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.enums import TA_CENTER, TA_LEFT
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.graphics.barcode import code128
        from xml.sax.saxutils import escape

        buffer = io.BytesIO()

        # 라벨 크기: 100mm * 120mm
        LABEL_WIDTH = 100 * mm
        LABEL_HEIGHT = 120 * mm

        doc = SimpleDocTemplate(
            buffer,
            pagesize=(LABEL_WIDTH, LABEL_HEIGHT),
            leftMargin=5 * mm,
            rightMargin=5 * mm,
            topMargin=5 * mm,
            bottomMargin=5 * mm,
        )

        styles = getSampleStyleSheet()

        # 제목 스타일 (25pt, 중앙정렬)
        title_style = ParagraphStyle(
            "LabelTitle",
            parent=styles["Heading1"],
            fontName=KOREAN_FONT_NAME,
            fontSize=25,
            alignment=TA_CENTER,
        )

        # 왼쪽 필드명 스타일 (굵게)
        field_label_style = ParagraphStyle(
            "FieldLabel",
            parent=styles["Normal"],
            fontName=KOREAN_FONT_NAME,
            fontSize=13,
            leading=16,
            alignment=TA_LEFT,
        )

        # 오른쪽 값 스타일 (굵게 — 원하면 얇게도 바꿀 수 있음)
        field_value_style = ParagraphStyle(
            "FieldValue",
            parent=styles["Normal"],
            fontName=KOREAN_FONT_NAME,
            fontSize=13,
            leading=16,
            alignment=TA_LEFT,
        )

        # 바코드 하단 텍스트 스타일 (중앙정렬)
        barcode_text_style = ParagraphStyle(
            "BarcodeText",
            parent=styles["Normal"],
            fontName=KOREAN_FONT_NAME,
            fontSize=12,
            alignment=TA_CENTER,
        )

        # ✅ 바코드를 가로 중앙 정렬하기 위한 Flowable
        class CenteredBarcode(Flowable):
            def __init__(self, barcode):
                super().__init__()
                self.barcode = barcode
                self._avail_width = None
                self.width = barcode.width
                self.height = barcode.height

            def wrap(self, availWidth, availHeight):
                self._avail_width = availWidth
                return availWidth, self.height

            def draw(self):
                if self._avail_width is None:
                    x = 0
                else:
                    x = (self._avail_width - self.barcode.width) / 2.0
                self.barcode.drawOn(self.canv, x, 0)

        story = []

        # 🔲 페이지마다 보더라인 그리기용 콜백
        def draw_border(canvas, doc_obj):
            canvas.saveState()
            # 3px ≈ 0.8mm 정도 안쪽으로
            inset = 0.8 * mm
            x = inset
            y = inset
            w = LABEL_WIDTH - 2 * inset
            h = LABEL_HEIGHT - 2 * inset
            canvas.setLineWidth(0.75)  # ≈ 1px
            canvas.rect(x, y, w, h)
            canvas.restoreState()

        for idx, row in df_labels.iterrows():
            품명 = str(row.get("품명", ""))
            품번 = str(row.get("품번", ""))
            환입일 = row.get("환입일", "")

            # 환입일 정리
            try:
                if pd.notna(환입일):
                    환입일_str = pd.to_datetime(환입일).strftime("%Y-%m-%d")
                else:
                    환입일_str = ""
            except Exception:
                환입일_str = str(환입일)

            # ----- 제목 -----
            story.append(Paragraph("부자재반입", title_style))
            # 공백 3줄 정도
            story.append(Spacer(1, field_label_style.leading * 3))

            # ----- 필드 4줄을 2열 테이블로 구성 (왼쪽 열 너비 고정) -----
            # 왼쪽 열 너비를 고정하면 오른쪽 값 시작 위치가 모두 동일해짐
            first_col_width = 28 * mm  # 필요하면 mm 값 조절해서 맞추면 됨
            second_col_width = doc.width - first_col_width

            data = [
                [
                    Paragraph("<b>품명</b>", field_label_style),
                    Paragraph(f"<b>{escape(품명)}</b>", field_value_style),
                ],
                [
                    Paragraph("<b>품목코드</b>", field_label_style),
                    Paragraph(f"<b>{escape(품번)}</b>", field_value_style),
                ],
                [
                    Paragraph("<b>단위수량</b>", field_label_style),
                    Paragraph(f"<b>{escape(unit_value)}</b>", field_value_style),
                ],
                [
                    Paragraph("<b>반입일자</b>", field_label_style),
                    Paragraph(f"<b>{escape(환입일_str)}</b>", field_value_style),
                ],
            ]

            row_height = field_label_style.leading * 2  # 한 줄 + 공백 1줄 느낌
            row_heights = [row_height] * len(data)

            tbl = Table(
                data,
                colWidths=[first_col_width, second_col_width],
                rowHeights=row_heights,
            )
            tbl.setStyle(
                TableStyle(
                    [
                        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("LEFTPADDING", (0, 0), (-1, -1), 0),
                        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
                        ("TOPPADDING", (0, 0), (-1, -1), 0),
                        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                    ]
                )
            )

            story.append(tbl)
            story.append(Spacer(1, 8))

            # 🔥 바코드 생성 (전체 너비 약 90px 기준)
            bar_width_px = 30
            bar_width_pt = bar_width_px * 0.75  # px → pt
            char_count = max(len(barcode_value), 1)
            bar_width = bar_width_pt / char_count

            bc = code128.Code128(
                barcode_value,
                barHeight=15 * mm,
                barWidth=bar_width,
            )

            # 중앙정렬 Flowable로 감싸기
            center_bc = CenteredBarcode(bc)

            story.append(Spacer(1, 5))
            story.append(center_bc)
            story.append(Spacer(1, 5))

            # 바코드 값 텍스트 (중앙정렬)
            story.append(Paragraph(barcode_value, barcode_text_style))

            # 여러 장일 경우 다음 페이지
            if idx != len(df_labels) - 1:
                story.append(PageBreak())

        # 보더라인 콜백 적용
        doc.build(story, onFirstPage=draw_border, onLaterPages=draw_border)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes


# -----------------------------
# 메인 화면
# -----------------------------
st.title("부자재 관리 시스템")

menu = st.radio(
    "메뉴 선택",
    [
        "📤 파일 업로드",
        "📦 입고 조회",
        "↩️ 환입 관리",
        "🔍 수주 찾기",
        "🧩 공통자재",
        "🏷 라벨 수량 계산",  
    ],
    horizontal=True,
)

# ==========================================
# 📤 1. 파일 업로드 탭 (S3에 엑셀만 저장)
# ==========================================
if menu == "📤 파일 업로드":
    st.subheader("📤 2025년 부자재 관리대장 업로드")

    uploaded_file = st.file_uploader("파일 업로드", type=["xlsm", "xlsx"])

    if uploaded_file and s3_client is not None:
        try:
            file_bytes = uploaded_file.read()

            # 1) 엑셀 원본을 S3에 저장 (이제 이걸만 쓴다)
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=S3_KEY_EXCEL,
                Body=file_bytes,
            )

            # 2) 캐시 초기화
            load_file_from_s3.clear()
            load_excel.clear()

            st.success("엑셀 파일을 S3에 업로드했습니다. 다른 탭에서 바로 사용할 수 있어요.")
        except Exception as e:
            st.error(f"S3 업로드 중 오류 발생: {e}")

    elif uploaded_file and s3_client is None:
        st.error("S3 클라이언트가 초기화되지 않았습니다. secrets 설정을 확인해주세요.")

    st.stop()  # 업로드 탭에서는 여기서 종료

# ==========================================
# 나머지 탭: S3에서 엑셀 로딩
# ==========================================
excel_bytes = load_file_from_s3()
if excel_bytes is None:
    st.warning("S3에 업로드된 엑셀 파일이 없습니다. 먼저 [📤 파일 업로드] 탭에서 파일을 올려주세요.")
    st.stop()

# 캐시된 엑셀 파싱 함수로 전체 시트 로딩
sheets = load_excel(excel_bytes)

required_sheets = ["입고", "작업지시", "수주", "BOM", "재고", "생산실적", "불량"]
missing_sheets = [s for s in required_sheets if s not in sheets]
if missing_sheets:
    st.error(f"엑셀 파일에 다음 시트를 찾을 수 없습니다: {', '.join(missing_sheets)}")
    st.stop()

# 각 시트 DataFrame 할당 (이름은 그대로 유지)
df_in_raw     = sheets["입고"]
df_job_raw    = sheets["작업지시"]
df_suju_raw   = sheets["수주"]
df_bom_raw    = sheets["BOM"]
df_stock_raw  = sheets["재고"]
df_result_raw = sheets["생산실적"]
df_defect_raw = sheets["불량"]

# 집계는 환입 데이터 불러오기 시 최초 1회
if "aggregates" not in st.session_state:
    st.session_state["aggregates"] = None

# ============================
# 2. 입고 조회 탭
# ============================
if menu == "📦 입고 조회":
    st.header("📦 입고 조회")
    st.caption("요청날짜 기준으로 입고 내역을 조회합니다.")

    # 입고 시트 원본
    df_in = df_in_raw.copy()

    # 요청날짜(K열) 컬럼 찾기
    req_date_col = pick_col(df_in, "K", ["요청날짜", "요청일"])
    if req_date_col is None:
        st.error("입고 시트에서 요청날짜(K열) 컬럼을 찾지 못했습니다.")
    else:
        # 날짜 컬럼 날짜형으로 변환
        df_in[req_date_col] = pd.to_datetime(df_in[req_date_col], errors="coerce").dt.date

        # 🔹 기본 범위: 어제 ~ 오늘
        today = date.today()
        default_start = today - timedelta(days=1)

        # 날짜 선택 + 품명 검색을 같은 줄(col) 에 배치
        col_date, col_name = st.columns([1, 2])

        with col_date:
            date_range = st.date_input(
                "요청날짜 범위 선택",
                (default_start, today),
                key="in_date_range",
            )

        with col_name:
            name_filter = st.text_input(
                "품명으로 검색",
                key="in_name_filter",
                placeholder="부분 검색 (예: 크림, 앰플 등)",
            )

        # Streamlit 버전에 따라 tuple 로 들어올 수 있어서 방어 코드
        if isinstance(date_range, (tuple, list)):
            start_date, end_date = date_range
        else:
            start_date = date_range
            end_date = date_range

        # 날짜 필터 마스크
        mask = (df_in[req_date_col] >= start_date) & (df_in[req_date_col] <= end_date)

        # 각 열 컬럼 찾기
        col_process  = pick_col(df_in, "J", ["생산공정"])
        col_req_no   = pick_col(df_in, "L", ["요청번호"])
        col_part     = pick_col(df_in, "M", ["품번"])
        col_name     = pick_col(df_in, "O", ["품명"])
        col_req_qty  = pick_col(df_in, "P", ["요청수량"])
        col_erp_out  = pick_col(df_in, "Q", ["ERP불출수량", "불출수량"])
        col_real_in  = pick_col(df_in, "R", ["현장실물입고"])

        # 👉 화면에 보여줄 컬럼 순서: 생산공정 → 요청날짜 → 나머지
        raw_cols = [c for c in [
            col_process,
            req_date_col,
            col_req_no,
            col_part,
            col_name,
            col_req_qty,
            col_erp_out,
            col_real_in,
        ] if c is not None]

        if not raw_cols:
            st.error("입고 시트에서 필요한 컬럼들을 찾지 못했습니다.")
        else:
            df_filtered = df_in.loc[mask, raw_cols].copy()

            # 보기 좋게 컬럼명 한글로 맞추기
            rename_map = {}
            rename_map[req_date_col] = "요청날짜"
            if col_process: rename_map[col_process] = "생산공정"
            if col_req_no:  rename_map[col_req_no]  = "요청번호"
            if col_part:    rename_map[col_part]    = "품번"
            if col_name:    rename_map[col_name]    = "품명"
            if col_req_qty: rename_map[col_req_qty] = "요청수량"
            if col_erp_out: rename_map[col_erp_out] = "ERP불출수량"
            if col_real_in: rename_map[col_real_in] = "현장실물입고"

            df_filtered.rename(columns=rename_map, inplace=True)

            # 🔍 품명 필터 추가 (사용자가 입력한 경우만)
            if name_filter:
                if "품명" in df_filtered.columns:
                    df_filtered = df_filtered[
                        df_filtered["품명"].astype(str).str.contains(
                            name_filter, case=False, na=False
                        )
                    ]

            # 🔥 엑셀에서 "마지막(맨 아래) 행"이 위로 오도록: 인덱스 역순 정렬
            df_filtered = df_filtered.iloc[::-1].reset_index(drop=True)

            if df_filtered.empty:
                st.info("선택한 기간에 해당하는 입고 데이터가 없습니다.")
            else:
                st.dataframe(df_filtered, use_container_width=True)

# ============================================================
# 🔍 3. 수주 찾기 화면
# ============================================================
if menu == "🔍 수주 찾기":
    st.subheader("🔍 수주 찾기")

    st.markdown(
        """
        **동작 방식**

        1. 기준 품번을 입력한다.  
        2. BOM 시트의 **C열 품번**에서 기준 품번과 일치하는 행을 찾고, 그 행의 **품목코드(A열)** 값을 구한다.  
        3. 이 품목코드를 **수주 시트의 품번(J열)**에서 검색한다.  
        4. 없으면 2단계 상위 품목코드로 다시 검색한다.  
        5. 오늘(today) 기준으로 **1개월 이내 → 1년 이내 → 과거 3개월 → 6개월 → 12개월** 순으로 유효한 수주를 찾는다.  
        """
    )

    base_part = st.text_input("기준 품번 입력", key="suju_find_part")

    if base_part:
        today = date.today()

        df_bom = df_bom_raw.copy()
        bom_cols = list(df_bom.columns)

        # A열 = 품목코드, B열 = 품명, C열 = 품번
        bom_item_col = pick_col(df_bom, "A", ["품목코드"])
        bom_name_col = pick_col(df_bom, "B", ["품명"])
        bom_component_col = pick_col(df_bom, "C", ["품번"])

        if not all([bom_item_col, bom_name_col, bom_component_col]):
            st.error("BOM 시트에서 품목코드(A), 품명(B), 품번(C)을 찾지 못했습니다.")
        else:
            # 기준 품번을 사용하는 BOM 행 검색
            df_bom_hit = df_bom[df_bom[bom_component_col] == base_part]

            if df_bom_hit.empty:
                st.info("BOM에서 해당 품번을 사용하는 완성품을 찾지 못했습니다.")
            else:
                # 1차 품목코드 목록
                item_codes = df_bom_hit[bom_item_col].dropna().unique().tolist()
                st.write("1차 완성품(품목코드):", item_codes)

                df_suju = df_suju_raw.copy()

                suju_part_col = pick_col(df_suju, "J", ["품번"])
                suju_due_col = pick_col(df_suju, "G", ["조정납기일자"])

                if suju_part_col is None or suju_due_col is None:
                    st.error("수주 시트에서 품번(J열) 또는 조정납기일자(G열)를 찾지 못했습니다.")
                else:
                    df_suju[suju_due_col] = pd.to_datetime(
                        df_suju[suju_due_col], errors="coerce"
                    ).dt.date

                    # 1차 품목코드로 검색
                    df_suju_hit = df_suju[
                        df_suju[suju_part_col].isin(item_codes)
                    ].copy()

                    # 🔁 2차 BOM 경로를 썼는지 여부 플래그
                    used_bom2_flow = False

                    # 없으면 상위(2차) 품목코드로 재검색
                    if df_suju_hit.empty:
                        fallback_item_codes = set()
                        for code in item_codes:
                            df_bom_lvl2 = df_bom[df_bom[bom_component_col] == code]
                            if not df_bom_lvl2.empty:
                                lvl2 = (
                                    df_bom_lvl2[bom_item_col]
                                    .dropna()
                                    .unique()
                                    .tolist()
                                )
                                fallback_item_codes.update(lvl2)

                        fallback_item_codes = list(fallback_item_codes)

                        if fallback_item_codes:
                            st.info("1차 품목코드로는 없어, 2차 상위 품목코드로 재검색합니다.")
                            st.write("2차 품목코드:", fallback_item_codes)

                            df_suju_hit = df_suju[
                                df_suju[suju_part_col].isin(fallback_item_codes)
                            ].copy()

                        # ✅ 2차 상위 품목코드로도 수주가 없으면
                        #    → 그 2차 상위 품목코드로 다시 BOM C열(품번)을 뒤져서
                        #       거기서 나온 완성품 품목코드(A열)로 수주를 재검색
                        if df_suju_hit.empty and fallback_item_codes:
                            df_bom_from_lvl2 = df_bom[
                                df_bom[bom_component_col].isin(fallback_item_codes)
                            ].copy()

                            if df_bom_from_lvl2.empty:
                                st.warning(
                                    "1차·2차 품목코드로 수주를 찾지 못했고, "
                                    "2차 상위 품목코드로 BOM 품번(C열)을 재검색해도 "
                                    "관련 품목을 찾지 못했습니다."
                                )
                                df_show = pd.DataFrame()
                            else:
                                # 3차(더 상위) 완성품 품목코드 목록
                                third_item_codes = (
                                    df_bom_from_lvl2[bom_item_col]
                                    .dropna()
                                    .unique()
                                    .tolist()
                                )

                                st.info(
                                    "1차·2차 품목코드로는 수주가 없어서, "
                                    "2차 상위 품목코드로 연결된 완성품(품목코드) 기준으로 다시 수주를 찾습니다."
                                )
                                st.write("3차(상위) 품목코드:", third_item_codes)

                                # 3차 품목코드로 수주 시트 재검색
                                df_suju_bom2 = df_suju[
                                    df_suju[suju_part_col].isin(third_item_codes)
                                ].copy()

                                if df_suju_bom2.empty:
                                    st.warning(
                                        "2차 상위 품목코드로 연결된 완성품 기준으로도 "
                                        "수주 시트에서 수주를 찾지 못했습니다."
                                    )
                                    df_show = pd.DataFrame()
                                else:
                                    used_bom2_flow = True

                                    # -------------------------------
                                    # 1️⃣ 위쪽 표: 수주 시트 요약
                                    #    (품번, 품명, 수주번호, 조정납기일자, 수량, 매출처)
                                    # -------------------------------
                                    # 원하는 표시 순서 정의
                                    desired_cols = [
                                        suju_part_col,   # 품번 (J열)
                                        "품명",
                                        "수주번호",
                                        suju_due_col,    # 조정납기일자 (G열)
                                        "수량",
                                        "매출처",
                                    ]

                                    # 실제 존재하는 컬럼만 필터링해서 순서 유지
                                    suju_disp_cols = [
                                        c for c in desired_cols if c in df_suju_bom2.columns
                                    ]

                                    # 납기일자 내림차순 정렬
                                    if suju_due_col in df_suju_bom2.columns:
                                        df_suju_bom2 = df_suju_bom2.sort_values(
                                            by=suju_due_col, ascending=False
                                        )

                                    st.markdown("#### 2차 상위 품목코드 기준 수주 정보")
                                    if suju_disp_cols:
                                        st.dataframe(
                                            df_suju_bom2[suju_disp_cols],
                                            use_container_width=True,
                                        )
                                    else:
                                        # 혹시라도 컬럼명을 못 찾았을 때는 전체 보여주기
                                        st.dataframe(
                                            df_suju_bom2,
                                            use_container_width=True,
                                        )


                                    # -------------------------------
                                    # 2️⃣ 아래 표: 작업지시 시트 연계
                                    #    (수주번호 A → 지시번호 B, 지시일자 I, 품명 L)
                                    # -------------------------------
                                    if "수주번호" in df_suju_bom2.columns:
                                        suju_values_bom2 = (
                                            df_suju_bom2["수주번호"]
                                            .dropna()
                                            .astype(str)
                                            .unique()
                                            .tolist()
                                        )

                                        job_suju_col = pick_col(
                                            df_job_raw, "A", ["수주번호"]
                                        )
                                        job_jisi_col = pick_col(
                                            df_job_raw, "B", ["지시번호"]
                                        )
                                        job_date_col = pick_col(
                                            df_job_raw, "I", ["지시일자", "작지일자"]
                                        )
                                        job_name_col = pick_col(
                                            df_job_raw, "L", ["품명", "완성품명"]
                                        )

                                        if not all(
                                            [job_suju_col, job_jisi_col, job_name_col]
                                        ):
                                            st.info(
                                                "작업지시 시트에서 수주번호(A), 지시번호(B), 품명(L)을 모두 찾지 못했습니다."
                                            )
                                        else:
                                            # 필요한 컬럼만 뽑기
                                            use_cols = [job_suju_col, job_jisi_col]
                                            if job_date_col:
                                                use_cols.append(job_date_col)
                                            use_cols.append(job_name_col)

                                            df_job_map2 = df_job_raw[use_cols].copy()

                                            # 컬럼명 통일
                                            new_cols = ["수주번호", "지시번호"]
                                            if job_date_col:
                                                new_cols.append("지시일자")
                                            new_cols.append("품명")
                                            df_job_map2.columns = new_cols

                                            # 문자열 비교용
                                            df_job_map2["수주번호_str"] = df_job_map2[
                                                "수주번호"
                                            ].astype(str)

                                            df_job_filtered2 = df_job_map2[
                                                df_job_map2["수주번호_str"].isin(
                                                    suju_values_bom2
                                                )
                                            ].drop(columns=["수주번호_str"])

                                            if not df_job_filtered2.empty:
                                                subset_cols = ["수주번호", "지시번호", "품명"]
                                                if "지시일자" in df_job_filtered2.columns:
                                                    subset_cols = [
                                                        "수주번호",
                                                        "지시번호",
                                                        "지시일자",
                                                        "품명",
                                                    ]

                                                df_job_filtered2 = df_job_filtered2.drop_duplicates(
                                                    subset=subset_cols
                                                )

                                                # 지시일자 최신순 + 지시번호 정렬
                                                if "지시일자" in df_job_filtered2.columns:
                                                    df_job_filtered2["_지시일자_sort"] = pd.to_datetime(
                                                        df_job_filtered2["지시일자"],
                                                        errors="coerce",
                                                    )
                                                    df_job_filtered2 = df_job_filtered2.sort_values(
                                                        by=["_지시일자_sort", "지시번호"],
                                                        ascending=[False, True],
                                                    ).drop(columns=["_지시일자_sort"])
                                                else:
                                                    df_job_filtered2 = df_job_filtered2.sort_values(
                                                        by=["지시번호"]
                                                    )

                                                st.markdown(
                                                    "#### 2차 상위 품목코드 기준 수주 → 작업지시 매핑"
                                                )

                                                disp_cols2 = ["수주번호", "지시번호"]
                                                if "지시일자" in df_job_filtered2.columns:
                                                    disp_cols2.append("지시일자")
                                                disp_cols2.append("품명")

                                                st.dataframe(
                                                    df_job_filtered2[disp_cols2],
                                                    use_container_width=True,
                                                )
                                            else:
                                                st.info(
                                                    "해당 수주번호로 작업지시 시트에서 지시번호를 찾지 못했습니다."
                                                )

                                    # 이 경로에서는 아래 일반 df_show 로직을 타지 않도록 비워둠
                                    df_show = pd.DataFrame()

                    # 🔁 BOM 2차 상위 품목코드 경로를 쓰지 않은 경우에만
                    #    기존 날짜 범위(1개월/1년/과거) 로직 수행
                    if not used_bom2_flow:
                        if df_suju_hit.empty:
                            st.warning("해당 품목코드로 수주 시트에서 검색된 수주가 없습니다.")
                            df_show = pd.DataFrame()
                        else:
                            # === 검색 범위 설정 ===
                            one_month_after = today + timedelta(days=30)
                            one_year_after = today + timedelta(days=365)

                            # 1) 오늘 → 1개월 이내
                            df_1m = df_suju_hit[



