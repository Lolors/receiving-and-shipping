
import streamlit as st
import pandas as pd
from datetime import date, timedelta
import tempfile
import io
import os
from html import escape
import sqlite3
from pathlib import Path

# ============ S3 연동 ============

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = "rec-and-ship"
S3_KEY_EXCEL = "bulk-ledger.xlsx"   # 기존 엑셀
S3_KEY_DB    = "inout.db"           # 부자재 메인 DB
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


# 🔹🔹🔹 여기 아래에 새 함수 2개 추가 🔹🔹🔹

@st.cache_data(show_spinner=True)
def load_db_from_s3() -> bytes | None:
    """S3에서 inout.db 파일을 바이트로 읽어서 반환"""
    if s3_client is None:
        return None
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY_DB)
        return obj["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return None
        st.error(f"S3 DB 로딩 오류: {e}")
        return None


# 엑셀 DB 변환 함수 추가
@st.cache_resource(show_spinner=True)
def get_db_connection(db_bytes: bytes):
    """
    S3에서 받은 DB bytes를 임시파일로 저장 후 SQLite 연결하기.
    Streamlit 세션 동안 재사용된다.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    tmp.write(db_bytes)
    tmp.flush()
    conn = sqlite3.connect(tmp.name, check_same_thread=False)
    return conn

REQUIRED_SHEETS = ["입고", "작업지시", "수주", "BOM", "재고", "생산실적", "불량"]

def excel_bytes_to_sqlite_bytes(excel_bytes: bytes) -> bytes:
    """
    업로드된 엑셀 바이트 → SQLite DB(inout.db) 파일 bytes로 변환.
    - 꼭 필요한 시트만 읽음
    - dtype=str 로 읽어서 타입 추론 비용 최소화
    """

    # 1) 엑셀 파일을 메모리에서 바로 읽기
    bio = io.BytesIO(excel_bytes)

    # 2) 한 번에 여러 시트를 읽어서 파싱 오버헤드 줄이기
    #    sheet_name=list 를 주면 dict[sheet_name] 형태로 반환됨
    try:
        all_sheets = pd.read_excel(
            bio,
            sheet_name=REQUIRED_SHEETS,
            dtype=str,           # 숫자/날짜 추론 안 하고 문자열로만 읽기 (빠름)
            engine="openpyxl",   # 일반적으로 안정적인 엔진
        )
    except Exception as e:
        # 혹시 engine 지정으로 문제가 생기면 기본 엔진으로 한번 더 시도
        bio.seek(0)
        all_sheets = pd.read_excel(
            bio,
            sheet_name=REQUIRED_SHEETS,
            dtype=str,
        )

    # 3) 임시 DB 파일 생성
    tmp_db = tempfile.NamedTemporaryFile(delete=False, suffix=".db")
    conn = sqlite3.connect(tmp_db.name)

    try:
        # 4) 필요한 시트만 테이블로 저장
        for sheet in REQUIRED_SHEETS:
            if sheet not in all_sheets:
                continue
            df = all_sheets[sheet]

            # 컬럼 이름에 공백 있으면 그대로 두어도 되지만,
            # 나중에 쿼리할 때 불편하면 여기서 strip 정도는 해도 됨
            df.columns = [str(c).strip() for c in df.columns]

            df.to_sql(sheet, conn, if_exists="replace", index=False)

        conn.commit()
    finally:
        conn.close()

    # 5) 완성된 DB 파일을 bytes로 읽어 반환
    with open(tmp_db.name, "rb") as f:
        db_bytes = f.read()

    return db_bytes



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
# 📤 1. 파일 업로드 탭 (S3에 엑셀 + DB 저장)
# ==========================================
if menu == "📤 파일 업로드":
    st.subheader("📤 2025년 부자재 관리대장 업로드")

    uploaded_file = st.file_uploader("파일 업로드", type=["xlsm", "xlsx"])

    if uploaded_file and s3_client is not None:
        try:
            # 1) 업로드된 파일 전체를 bytes로 읽기
            file_bytes = uploaded_file.read()

            # 2) 엑셀 원본을 S3에 저장 (백업/원본 용도)
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=S3_KEY_EXCEL,
                Body=file_bytes,
            )

            # 3) 엑셀 → SQLite DB 변환
            db_bytes = excel_bytes_to_sqlite_bytes(file_bytes)

            # 4) 변환된 DB를 S3에 저장
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=S3_KEY_DB,
                Body=db_bytes,
            )

            # 5) 캐시 초기화
            load_db_from_s3.clear()
            load_file_from_s3.clear()
            # (load_excel은 이제 안 써도 되지만 혹시 몰라 같이 비워 둠)
            load_excel.clear()

            st.success("엑셀과 DB를 S3에 모두 업로드했습니다. 다른 탭에서 빠르게 조회할 수 있어요.")
        except Exception as e:
            st.error(f"S3 업로드/DB 변환 중 오류 발생: {e}")

    elif uploaded_file and s3_client is None:
        st.error("S3 클라이언트가 초기화되지 않았습니다. secrets 설정을 확인해주세요.")

    st.stop()  # 업로드 탭에서는 여기서 종료



# ==========================================
# 나머지 탭: S3에서 DB 로딩
# ==========================================
db_bytes = load_db_from_s3()
if db_bytes is None:
    st.warning("S3에 업로드된 DB 파일이 없습니다. 먼저 [📤 파일 업로드] 탭에서 파일을 올려주세요.")
    st.stop()

# S3에서 받은 DB bytes로 SQLite 연결
conn = get_db_connection(db_bytes)

# 필수 테이블 존재 여부 체크
required_tables = ["입고", "작업지시", "수주", "BOM", "재고", "생산실적", "불량"]
tables_df = pd.read_sql(
    "SELECT name FROM sqlite_master WHERE type='table';",
    conn,
)
existing_tables = set(tables_df["name"].tolist())
missing = [t for t in required_tables if t not in existing_tables]
if missing:
    st.error(f"SQLite DB에 다음 테이블(시트)이 없습니다: {', '.join(missing)}")
    st.stop()

# 각 시트에 해당하는 테이블 읽기
df_in_raw     = pd.read_sql("SELECT * FROM 입고", conn)
df_job_raw    = pd.read_sql("SELECT * FROM 작업지시", conn)
df_suju_raw   = pd.read_sql("SELECT * FROM 수주", conn)
df_bom_raw    = pd.read_sql("SELECT * FROM BOM", conn)
df_stock_raw  = pd.read_sql("SELECT * FROM 재고", conn)
df_result_raw = pd.read_sql("SELECT * FROM 생산실적", conn)
df_defect_raw = pd.read_sql("SELECT * FROM 불량", conn)

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
                                df_suju_hit[suju_due_col].between(today, one_month_after)
                            ].copy()

                            if not df_1m.empty:
                                st.success("오늘 기준 1개월 이내 수주 발견!")
                                df_show = df_1m
                            else:
                                # 2) 오늘 → 1년 이내
                                df_1y = df_suju_hit[
                                    df_suju_hit[suju_due_col].between(
                                        today, one_year_after
                                    )
                                ].copy()

                                if not df_1y.empty:
                                    st.info("1개월 이내는 없고, 1년 이내 수주가 있습니다.")
                                    df_1y.sort_values(
                                        by=suju_due_col, ascending=False, inplace=True
                                    )
                                    df_show = df_1y
                                else:
                                    # 3) 과거 탐색: 3개월·6개월·12개월
                                    back_3m = today - timedelta(days=90)
                                    back_6m = today - timedelta(days=180)
                                    back_12m = today - timedelta(days=365)

                                    df_back3 = df_suju_hit[
                                        df_suju_hit[suju_due_col].between(
                                            back_3m, today
                                        )
                                    ].copy()

                                    if not df_back3.empty:
                                        st.info(
                                            "1년 이내 수주는 없어서, 과거 3개월 수주를 보여줍니다."
                                        )
                                        df_back3.sort_values(
                                            by=suju_due_col,
                                            ascending=False,
                                            inplace=True,
                                        )
                                        df_show = df_back3
                                    else:
                                        df_back6 = df_suju_hit[
                                            df_suju_hit[suju_due_col].between(
                                                back_6m, today
                                            )
                                        ].copy()

                                        if not df_back6.empty:
                                            st.info(
                                                "3개월 이내 없음 → 과거 6개월 수주 표시."
                                            )
                                            df_back6.sort_values(
                                                by=suju_due_col,
                                                ascending=False,
                                                inplace=True,
                                            )
                                            df_show = df_back6
                                        else:
                                            df_back12 = df_suju_hit[
                                                df_suju_hit[suju_due_col].between(
                                                    back_12m, today
                                                )
                                            ].copy()

                                            if not df_back12.empty:
                                                st.info(
                                                    "6개월 이내 없음 → 과거 12개월 수주 표시."
                                                )
                                                df_back12.sort_values(
                                                    by=suju_due_col,
                                                    ascending=False,
                                                    inplace=True,
                                                )
                                                df_show = df_back12
                                            else:
                                                st.warning(
                                                    "과거 12개월까지도 해당 품목코드의 수주가 없습니다."
                                                )
                                                df_show = pd.DataFrame()



                        # ===== 결과 표시 =====
                        if not df_show.empty:
                            display_cols = []
                            for c in [
                                suju_part_col,
                                "품명",
                                "수주번호",
                                suju_due_col,
                                "수량",
                                "매출처",
                            ]:
                                if c in df_show.columns:
                                    display_cols.append(c)

                            st.dataframe(
                                df_show[display_cols],
                                use_container_width=True,
                            )

                        # =======================================================
                        # 🔁 수주번호 → 작업지시 시트에서 지시번호 / 품명 가져오기
                        # =======================================================
                        if "수주번호" in df_show.columns:
                            # 1) 수주 찾기 결과에서 수주번호 목록 추출
                            suju_values = (
                                df_show["수주번호"]
                                .dropna()
                                .astype(str)
                                .unique()
                                .tolist()
                            )

                            # 2) 작업지시 시트에서 컬럼 찾기
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
                                # 3) 필요한 컬럼만 가져오기
                                use_cols = [job_suju_col, job_jisi_col]
                                if job_date_col:
                                    use_cols.append(job_date_col)
                                use_cols.append(job_name_col)

                                df_job_map = df_job_raw[use_cols].copy()
                                
                                # 컬럼명 통일
                                new_cols = ["수주번호", "지시번호"]
                                if job_date_col:
                                    new_cols.append("지시일자")
                                new_cols.append("품명")
                                df_job_map.columns = new_cols

                                # 🔥 필수: 문자열 비교를 위한 컬럼 생성
                                df_job_map["수주번호_str"] = df_job_map["수주번호"].astype(str)

                                # 4) 수주찾기에서 나온 수주번호 목록과 일치하는 행 필터링
                                df_job_filtered = df_job_map[
                                    df_job_map["수주번호_str"].isin(
                                        suju_values
                                    )
                                ].drop(columns=["수주번호_str"])

                                if df_job_filtered.empty:
                                    ...
                                else:
                                    # 중복 제거
                                    subset_cols = ["수주번호", "지시번호", "품명"]
                                    if "지시일자" in df_job_filtered.columns:
                                        subset_cols = ["수주번호", "지시번호", "지시일자", "품명"]

                                    df_job_filtered = df_job_filtered.drop_duplicates(
                                        subset=subset_cols
                                    )

                                    # 🔽 지시일자가 최근일수록 위쪽에 오도록 정렬
                                    if "지시일자" in df_job_filtered.columns:
                                        df_job_filtered["_지시일자_sort"] = pd.to_datetime(
                                            df_job_filtered["지시일자"], errors="coerce"
                                        )
                                        df_job_filtered = df_job_filtered.sort_values(
                                            by=["_지시일자_sort", "지시번호"],
                                            ascending=[False, True],
                                        ).drop(columns=["_지시일자_sort"])
                                    else:
                                        # 지시일자가 없으면 지시번호 기준 오름차순
                                        df_job_filtered = df_job_filtered.sort_values(
                                            by=["지시번호"]
                                        )

                                    st.markdown(
                                        "#### 수주번호별 지시번호 / 품명 (작업지시 기준)"
                                    )

                                    display_cols = ["수주번호", "지시번호"]
                                    if "지시일자" in df_job_filtered.columns:
                                        display_cols.append("지시일자")
                                    display_cols.append("품명")

                                    st.dataframe(
                                        df_job_filtered[display_cols],
                                        use_container_width=True,
                                    )


# ============================================================
# ↩️ 4. 환입 관리 화면 (+ 환입 예상재고)
# ============================================================
if menu == "↩️ 환입 관리":
    st.subheader("↩️ 환입 관리")

    # 환입 관리 테이블 구조 (내부 계산용)
    return_cols = [
        "수주번호",
        "지시번호",
        "생산공정",
        "생산시작일",
        "생산종료일",
        "종료조건",
        "환입일",
        "환입주차",
        "완성품번",
        "제품명",  # 완성품명
        "품번",
        "품명",
        "단위수량",
        "ERP재고",
        "실재고예상",
        "환입결정수",
        "차이",
        "비고",
    ]
    df_return = ensure_session_df("환입관리", return_cols)
    df_full = ensure_session_df("환입재고예상", CSV_COLS)

    # 🔍 수주 검색 (입고 시트 기준)
    st.markdown("### 🔍 수주 검색 (입고 시트 기준)")

    search_keyword = st.text_input(
        "제품명으로 수주 검색 (입고 시트 E열, 부분 일치)",
        key="return_search_product",
        placeholder="예: 앰플, 크림, 마스크팩 등",
    )

    if search_keyword:
        df_in_search = df_in_raw.copy()

        # 요청날짜(K열), 제품명(E열) 컬럼 찾기
        in_req_date_col = pick_col(df_in_search, "K", ["요청날짜", "요청일"])
        in_prod_name_col = pick_col(df_in_search, "E", ["제품명", "품명"])

        if in_req_date_col is None or in_prod_name_col is None:
            st.error("입고 시트에서 요청날짜(K열) 또는 제품명(E열) 컬럼을 찾지 못했습니다.")
        else:
            # 날짜형 변환
            df_in_search[in_req_date_col] = pd.to_datetime(
                df_in_search[in_req_date_col], errors="coerce"
            ).dt.date

            today = date.today()
            start_date = today - timedelta(days=30)  # 최근 1개월

            # 날짜 필터: 현재로부터 1달 이내
            mask_date = df_in_search[in_req_date_col].between(start_date, today)

            # 제품명 부분 일치 (대소문자 무시)
            mask_name = df_in_search[in_prod_name_col].astype(str).str.contains(
                search_keyword, case=False, na=False
            )

            df_hit = df_in_search[mask_date & mask_name].copy()

            if df_hit.empty:
                st.info("최근 1개월 이내에 해당 제품명이 포함된 입고 데이터가 없습니다.")
            else:
                # 추가로 보여줄 컬럼들: 수주번호(B), 지시번호(C), 품번(M)
                in_suju_col = pick_col(df_hit, "B", ["수주번호"])
                in_jisi_col = pick_col(df_hit, "C", ["지시번호"])
                in_part_col = pick_col(df_hit, "M", ["품번"])

                show_cols = []
                for c in [
                    in_req_date_col,
                    in_suju_col,
                    in_jisi_col,
                    in_prod_name_col,
                    in_part_col,
                ]:
                    if c and c in df_hit.columns:
                        show_cols.append(c)

                df_show = df_hit[show_cols].copy()

                # 컬럼명 한글로 정리
                rename_map = {}
                rename_map[in_req_date_col] = "요청날짜"
                if in_suju_col:
                    rename_map[in_suju_col] = "수주번호"
                if in_jisi_col:
                    rename_map[in_jisi_col] = "지시번호"
                if in_prod_name_col:
                    rename_map[in_prod_name_col] = "제품명"
                if in_part_col:
                    rename_map[in_part_col] = "품번"

                df_show.rename(columns=rename_map, inplace=True)

                # 품번 제거 (검색용에서만 표시했다 지우기)
                if "품번" in df_show.columns:
                    df_show = df_show.drop(columns=["품번"])

                # 요청날짜는 중복 제거 기준 제외, 수주번호+지시번호 기준으로 유일하게
                uniq_cols = [c for c in ["수주번호", "지시번호"] if c in df_show.columns]
                df_show = df_show.drop_duplicates(subset=uniq_cols)

                st.dataframe(df_show, use_container_width=True)

                # 🔽 검색 결과에서 선택하면 아래 수주번호/지시번호 자동 채우기
                if "수주번호" in df_show.columns:
                    df_select = df_show.reset_index(drop=True)

                    option_labels = []
                    option_map = {}

                    for _, row in df_select.iterrows():
                        suju_val = str(row.get("수주번호", ""))
                        jisi_val = str(row.get("지시번호", ""))
                        prod_val = str(row.get("제품명", ""))

                        label = f"{prod_val} | 수주:{suju_val}"
                        if jisi_val:
                            label += f" / 지시:{jisi_val}"

                        option_labels.append(label)
                        option_map[label] = (suju_val, jisi_val)

                    selected_label = st.selectbox(
                        "👇 이 중 하나를 선택하면 아래 수주번호/지시번호가 자동으로 채워집니다.",
                        ["선택 안 함"] + option_labels,
                        key="return_suju_autofill",
                    )

                    if selected_label != "선택 안 함":
                        sel_suju, sel_jisi = option_map[selected_label]
                        st.session_state["return_suju_no"] = sel_suju
                        if sel_jisi:
                            st.session_state["return_jisi"] = sel_jisi

    # ----- 입력 1줄 (수주번호, 지시번호, 생산공정, 종료조건) -----
    col_suju, col_jisi, col_proc, col_reason = st.columns(4)
    with col_suju:
        suju_no = st.text_input("수주번호", key="return_suju_no")
    with col_jisi:
        selected_jisi = None  # 아래에서 selectbox로 채움
    with col_proc:
        process_options = [
            "4층 덕용",
            "4층 로터리",
            "4층 블리스터",
            "5층 덕용",
            "5층 기초",
            "6층 스틱",
            "6층 파우치",
            "6층 스킨팩",
        ]
        process_value = st.selectbox("생산공정", process_options, key="return_process")
    with col_reason:
        finish_reason = st.text_input("종료조건", key="return_finish_reason")

    # 수주번호 기반 지시번호/완성품번 후보 찾기
    jisi_options = []
    finished_part_selected = None

    # 🔹 작업지시 시트의 작업장 컬럼(X열) 찾기
    job_wc_col = pick_col(df_job_raw, "X", ["작업장"])

    if suju_no:
        if "수주번호" in df_job_raw.columns:
            # 1차: 수주번호 기준 필터
            df_job_suju = df_job_raw[df_job_raw["수주번호"] == suju_no].copy()

            # 🔹 2차: 작업장 WC501~WC504 조건 추가
            if job_wc_col and job_wc_col in df_job_suju.columns:
                df_job_suju = df_job_suju[
                    df_job_suju[job_wc_col].astype(str).isin(
                        ["WC501", "WC502", "WC503", "WC504"]
                    )
                ]

            # 👉 필터 후 아무 것도 없으면 안내
            if df_job_suju.empty:
                st.warning("해당 수주번호에 대해 작업장 WC401~WC404 작업지시가 없습니다.")
            else:
                # 완성품번 후보
                finished_parts = (
                    df_job_suju["품번"].dropna().unique().tolist()
                    if "품번" in df_job_suju.columns
                    else []
                )

                if len(finished_parts) > 1:
                    finished_part_selected = st.selectbox(
                        "완성품번", finished_parts, key="return_finished_part"
                    )
                    df_job_suju = df_job_suju[
                        df_job_suju["품번"] == finished_part_selected
                    ]
                elif len(finished_parts) == 1:
                    finished_part_selected = finished_parts[0]

                # 지시번호 후보
                if "지시번호" in df_job_suju.columns:
                    jisi_options = (
                        df_job_suju["지시번호"].dropna().unique().tolist()
                    )
                else:
                    st.error("작업지시 시트에 '지시번호' 컬럼이 없습니다.")
        else:
            st.error("작업지시 시트에 '수주번호' 컬럼이 없습니다.")


    # 지시번호 선택 (수주번호 입력 후)
    if jisi_options:
        selected_jisi = col_jisi.selectbox(
            "지시번호", jisi_options, key="return_jisi"
        )
    else:
        with col_jisi:
            st.write("지시번호: 선택 없음")

    # ----- 생산 시작/종료일 -----
    production_start_date = None
    production_end_date = None
    if (
        suju_no
        and "수주번호" in df_result_raw.columns
        and "생산일자" in df_result_raw.columns
    ):
        df_res_suju = df_result_raw[df_result_raw["수주번호"] == suju_no].copy()
        df_res_suju["생산일자"] = pd.to_datetime(
            df_res_suju["생산일자"], errors="coerce"
        )
        if not df_res_suju["생산일자"].isna().all():
            production_start_date = df_res_suju["생산일자"].min().date()
            production_end_date = df_res_suju["생산일자"].max().date()

    st.write(f"생산시작일: {production_start_date or '데이터 없음'}")
    st.write(f"생산종료일: {production_end_date or '데이터 없음'}")

    # ----- 환입일/환입주차 -----
    return_date = date.today()
    return_week = get_week_of_month(return_date)
    st.write(f"환입일: {return_date}")
    st.write(f"환입주차: {return_week}")

    # ----- 완성품번 / 완성품명 (BOM에서 품명 가져오기) -----
    finished_part = finished_part_selected
    finished_name = None

    # 1차: 지시번호에서 완성품번 유추 (없을 때만)
    if not finished_part and selected_jisi and "지시번호" in df_job_raw.columns:
        df_job_jisi = df_job_raw[df_job_raw["지시번호"] == selected_jisi]
        if not df_job_jisi.empty and "품번" in df_job_jisi.columns:
            finished_part = df_job_jisi["품번"].iloc[0]

    # BOM에서 완성품명 찾기 (품목코드=A열, 품명=B열)
    if finished_part is not None:
        bom_cols = list(df_bom_raw.columns)
        item_col = "품목코드" if "품목코드" in bom_cols else bom_cols[0]
        name_col = (
            "품명"
            if "품명" in bom_cols
            else (bom_cols[1] if len(bom_cols) > 1 else bom_cols[0])
        )

        df_bom_match = df_bom_raw[df_bom_raw[item_col] == finished_part]
        if not df_bom_match.empty:
            finished_name = df_bom_match[name_col].iloc[0]
        else:
            if (
                selected_jisi
                and "지시번호" in df_job_raw.columns
                and "품명" in df_job_raw.columns
            ):
                df_job_jisi = df_job_raw[df_job_raw["지시번호"] == selected_jisi]
                if not df_job_jisi.empty:
                    finished_name = df_job_jisi["품명"].iloc[0]

    st.write(f"완성품번: {finished_part or '데이터 없음'}")
    st.write(f"완성품명: {finished_name or '데이터 없음'}")

    # ----- BOM 자재 목록 -----
    bom_component_df = pd.DataFrame()
    if finished_part is not None:
        bom_cols = list(df_bom_raw.columns)
        item_col = "품목코드" if "품목코드" in bom_cols else bom_cols[0]
        bom_part_cols = [c for c in bom_cols if "품번" in c]
        bom_name_cols = [c for c in bom_cols if "품명" in c]

        bom_component_col2 = (
            bom_part_cols[1]
            if len(bom_part_cols) >= 2
            else (bom_part_cols[0] if bom_part_cols else None)
        )
        bom_name_col2 = (
            bom_name_cols[1]
            if len(bom_name_cols) >= 2
            else (bom_name_cols[0] if len(bom_name_cols) > 0 else None)
        )

        df_bom_finished = df_bom_raw[df_bom_raw[item_col] == finished_part].copy()
        if df_bom_finished.empty:
            st.warning("BOM에서 해당 완성품번(품목코드)을 사용하는 자재를 찾지 못했습니다.")
        else:
            subset_cols = []
            if bom_component_col2 and bom_component_col2 in df_bom_finished.columns:
                subset_cols.append(bom_component_col2)
            if bom_name_col2 and bom_name_col2 in df_bom_finished.columns:
                subset_cols.append(bom_name_col2)
            if "단위수량" in df_bom_finished.columns:
                subset_cols.append("단위수량")

            if subset_cols:
                df_bom_fin_uniq = df_bom_finished.drop_duplicates(subset=subset_cols)
            else:
                df_bom_fin_uniq = df_bom_finished.drop_duplicates()

            bom_component_df = pd.DataFrame(
                {
                    "선택": True,
                    "완성품번": df_bom_fin_uniq[item_col],
                    "품번": df_bom_fin_uniq[bom_component_col2]
                    if bom_component_col2 in df_bom_fin_uniq.columns
                    else "",
                    "품명": df_bom_fin_uniq[bom_name_col2]
                    if bom_name_col2 in df_bom_fin_uniq.columns
                    else "",
                    "단위수량": df_bom_fin_uniq["단위수량"]
                    if "단위수량" in df_bom_fin_uniq.columns
                    else "",
                }
            )

            st.markdown("BOM 자재 목록에서 환입 대상 자재를 선택하세요.")
            bom_component_df = st.data_editor(
                bom_component_df,
                use_container_width=True,
                num_rows="dynamic",
                key="bom_component_editor",
            )

            # ===============================
            # 🔘 (여기!) 환입 데이터 불러오기 / 초기화 버튼 (가운데 정렬)
            #  → BOM 자재 표가 뜬 뒤에만 보이도록
            # ===============================
            col_left, col_center, col_right = st.columns([1, 2, 1])

            with col_center:
                col_btn1, col_btn2 = st.columns([1, 1])

                with col_btn1:
                    load_clicked = st.button("✅ 환입 데이터 불러오기", key="btn_return_load")

                with col_btn2:
                    clear_clicked = st.button(
                        "🧹 환입 예상재고 초기화", key="btn_clear_expect"
                    )

            # 🔍 환입 데이터 불러오기 실행 로직
            if load_clicked:
                if not suju_no:
                    st.error("수주번호를 입력해주세요.")
                elif not selected_jisi:
                    st.error("지시번호를 선택해주세요.")
                elif bom_component_df.empty:
                    st.error("BOM 자재 목록이 없습니다.")
                else:
                    selected_rows = bom_component_df[
                        bom_component_df["선택"] == True
                    ].copy()
                    if selected_rows.empty:
                        st.warning("선택된 자재가 없습니다. 최소 1개 선택해주세요.")
                    else:
                        new_rows = []
                        for _, row in selected_rows.iterrows():
                            part = row["품번"]
                            name = row["품명"]
                            unit = row["단위수량"]

                            new_rows.append(
                                {
                                    "수주번호": suju_no,
                                    "지시번호": selected_jisi,
                                    "생산공정": process_value,
                                    "생산시작일": production_start_date,
                                    "생산종료일": production_end_date,
                                    "종료조건": finish_reason,
                                    "환입일": return_date,
                                    "환입주차": return_week,
                                    "완성품번": finished_part,
                                    "제품명": finished_name,
                                    "품번": part,
                                    "품명": name,
                                    "단위수량": unit,
                                    "ERP재고": None,
                                    "실재고예상": None,
                                    "환입결정수": None,
                                    "차이": None,
                                    "비고": "",
                                }
                            )

                        df_new = pd.DataFrame(new_rows)

                        # ✅ 이전 환입관리 내용은 버리고,
                        #    이번에 선택한 자재(df_new)만 환입관리로 사용
                        df_return = df_new.copy()
                        st.session_state["환입관리"] = df_return


                        # 집계 최초 생성
                        if st.session_state["aggregates"] is None:
                            st.session_state["aggregates"] = build_aggregates(
                                df_in_raw,
                                df_job_raw,
                                df_result_raw,
                                df_defect_raw,
                                df_stock_raw,
                            )

                        aggs = st.session_state["aggregates"]

                        # 예상재고 계산
                        df_full = recalc_return_expectation(df_return, aggs)
                        st.session_state["환입재고예상"] = df_full

                        # ERP재고 매칭
                        stock_part_col = pick_col(df_stock_raw, "D", ["품번"])
                        stock_qty_col = (
                            "실재고수량"
                            if "실재고수량" in df_stock_raw.columns
                            else pick_col(df_stock_raw, "N", ["실재고수량"])
                        )

                        if stock_part_col and stock_qty_col:
                            stock_map = dict(
                                zip(
                                    df_stock_raw[stock_part_col].astype(str),
                                    df_stock_raw[stock_qty_col].apply(safe_num),
                                )
                            )
                            df_full["ERP재고"] = (
                                df_full["품번"].astype(str).map(stock_map).fillna(0)
                            )
                        else:
                            st.warning(
                                "재고 시트에서 품번 또는 실재고수량 컬럼을 찾을 수 없습니다."
                            )

                        st.success(
                            f"선택된 자재 {len(df_new)}개에 대해 환입 예상재고 데이터가 갱신되었습니다."
                        )

            # 🧹 환입 예상재고 초기화 실행 로직
            if clear_clicked:
                # ✅ 환입관리도 함께 초기화
                st.session_state["환입관리"] = pd.DataFrame(columns=return_cols)
                df_return = st.session_state["환입관리"]

                st.session_state["환입재고예상"] = pd.DataFrame(columns=CSV_COLS)
                df_full = st.session_state["환입재고예상"]

                st.success("환입 관리 / 환입 예상재고 데이터가 모두 초기화되었습니다.")


    # ----- 환입 예상재고 데이터 표시 + CSV + PDF + 라벨 -----
    st.markdown("### 환입 예상재고 데이터")

    df_full = st.session_state.get(
        "환입재고예상", pd.DataFrame(columns=CSV_COLS)
    )

    if df_full.empty:
        st.write("환입 데이터 불러오기를 실행하면 이곳에 결과가 표시됩니다.")
    else:
        # -------------------------------------------------
        # 0) df_full 기본 세팅
        # -------------------------------------------------
        df_full = df_full.copy().reset_index(drop=True)

        col_defaults = {
            "추가수주": "",
            "라벨선택": False,
            "공통부자재": False,
        }
        for col, default in col_defaults.items():
            if col not in df_full.columns:
                df_full[col] = default

        for bcol in ["라벨선택", "공통부자재"]:
            df_full[bcol] = df_full[bcol].fillna(False).astype(bool)

        st.session_state["환입재고예상"] = df_full

        # -------------------------------------------------
        # 1) 추가수주 자동 채우기용 공통 입고기간 선택
        # -------------------------------------------------
        today = date.today()
        default_start = today - timedelta(days=30)
        date_range = st.date_input(
            "추가수주 자동생성용 입고기간 선택",
            (default_start, today),
            key="extra_order_range",
        )
        if isinstance(date_range, (tuple, list)):
            start_date, end_date = date_range
        else:
            start_date = end_date = date_range

        # -------------------------------------------------
        # 2) data_editor 에서 쓸 표시 컬럼 구성
        #    - 공통부자재: 맨 앞
        #    - 수주번호 뒤에 추가수주
        #    - 라벨선택: 여기서는 숨김
        # -------------------------------------------------
        base_cols = [c for c in VISIBLE_COLS if c in df_full.columns]

        display_cols = []

        # 맨 앞 공통부자재
        display_cols.append("공통부자재")

        # 수주번호 / 추가수주 / 나머지
        if "수주번호" in base_cols:
            display_cols.append("수주번호")
            display_cols.append("추가수주")
            for c in base_cols:
                if c != "수주번호":
                    display_cols.append(c)
        else:
            display_cols.extend(base_cols)
            if "추가수주" not in display_cols:
                display_cols.append("추가수주")

        # 라벨선택은 여기서는 숨김
        if "라벨선택" in display_cols:
            display_cols.remove("라벨선택")

        # 화면용 DF
        df_visible = pd.DataFrame(index=df_full.index)
        for c in display_cols:
            if c in df_full.columns:
                df_visible[c] = df_full[c]

        if "공통부자재" in df_visible.columns:
            df_visible["공통부자재"] = df_visible["공통부자재"].fillna(False).astype(bool)
        if "추가수주" in df_visible.columns:
            df_visible["추가수주"] = df_visible["추가수주"].astype(str)

        # -------------------------------------------------
        # 2-1) form 안에 data_editor + 두 개 버튼(저장 / 자동채우기)
        #      → 둘 중 하나만 눌러도 한 번에 처리
        # -------------------------------------------------
        with st.form("return_editor_form"):

            df_edit = st.data_editor(
                df_visible,
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
                column_config={
                    "공통부자재": st.column_config.CheckboxColumn(
                        "공통부자재",
                        default=False,
                    )
                },
                key="return_editor",
            )

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                save_clicked = st.form_submit_button("💾 공통부자재 / 추가수주 저장")
            with col_btn2:
                auto_clicked = st.form_submit_button("🔄 입고기간 기준으로 추가수주 자동 채우기")

        # 🔹 혹시 데이터가 비정상 타입으로 들어오는 경우 방지
        if not isinstance(df_edit, pd.DataFrame):
            df_edit = pd.DataFrame(df_edit)

        # -------------------------------------------------
        # 3) 폼이 제출되었을 때(df_edit → df_full 반영)
        #    - 저장 버튼만 눌렀을 때: 세션에만 저장
        #    - 자동채우기 버튼 눌렀을 때: 저장 + 자동채우기 + 재계산
        # -------------------------------------------------
        if save_clicked or auto_clicked:
            # 3-1) 에디터 값 → df_full 반영
            for col in ["공통부자재", "추가수주"]:
                if col in df_edit.columns:
                    df_full[col] = df_edit[col].reindex(df_full.index).values

            df_full["공통부자재"] = df_full["공통부자재"].fillna(False).astype(bool)
            st.session_state["환입재고예상"] = df_full

            # 3-2) 자동채우기 버튼이 눌린 경우에만 추가 작업
            if auto_clicked:
                df_full = df_full.copy()

                # 공통부자재 체크된 행만 대상
                if "공통부자재" in df_full.columns:
                    target_idx = df_full.index[df_full["공통부자재"] == True]
                else:
                    target_idx = df_full.index

                # ---------- (1) 추가수주 자동 채우기 ----------
                for idx in target_idx:
                    row = df_full.loc[idx]
                    part = row.get("품번", None)
                    base_suju = row.get("수주번호", None)

                    if part is None or pd.isna(part) or base_suju is None or pd.isna(base_suju):
                        continue

                    extra = get_extra_orders_by_period(
                        part_code=str(part),
                        base_suju=str(base_suju),
                        start_date=start_date,
                        end_date=end_date,
                    )

                    if not extra:
                        continue

                    current = str(row.get("추가수주", "")).strip()
                    if current:
                        current_list = [s.strip() for s in current.split(",") if s.strip()]
                        extra_list   = [s.strip() for s in extra.split(",") if s.strip()]
                        merged = sorted(set(current_list + extra_list))
                        df_full.at[idx, "추가수주"] = ", ".join(merged)
                    else:
                        df_full.at[idx, "추가수주"] = extra

                # ---------- (2) 공통부자재 행 재계산 ----------
                aggs = st.session_state.get("aggregates", None)

                if aggs is None:
                    st.warning("공통부자재 합산을 위해서는 먼저 '환입 데이터 불러오기' 버튼으로 집계를 만들어야 합니다.")
                else:
                    import re

                    def recompute_row_with_extra_orders(row):
                        part = str(row.get("품번", "")).strip()
                        base_suju = str(row.get("수주번호", "")).strip()
                        extra_text = str(row.get("추가수주", "")).strip()

                        if not part or not base_suju:
                            return row

                        suju_list = [base_suju]
                        if extra_text:
                            extra_ids = [
                                s.strip()
                                for s in re.split(r"[ ,;/]+", extra_text)
                                if s.strip()
                            ]
                            suju_list.extend(extra_ids)

                        in_tbl = aggs.get("in")
                        res_tbl = aggs.get("result")

                        # 1) 입고 합계 (품번 + 수주번호)
                        erp_out = 0.0
                        real_in = safe_num(row.get("현장실물입고", 0))
                        if isinstance(in_tbl, pd.DataFrame) and not in_tbl.empty:
                            mask_in = (
                                in_tbl["품번"].astype(str) == part
                            ) & (
                                in_tbl["수주번호"].astype(str).isin(suju_list)
                            )
                            tmp_in = in_tbl.loc[mask_in]
                            if not tmp_in.empty:
                                erp_out = tmp_in["ERP불출수량"].apply(safe_num).sum()
                                real_in = tmp_in["현장실물입고"].apply(safe_num).sum()

                        # 2) 생산/샘플 합계 (수주번호 기준)
                        prod = safe_num(row.get("생산수량", 0))
                        qc   = safe_num(row.get("QC샘플", 0))
                        etc  = safe_num(row.get("기타샘플", 0))

                        if (
                            isinstance(res_tbl, pd.DataFrame)
                            and not res_tbl.empty
                            and "수주번호" in res_tbl.columns
                        ):
                            mask_res = res_tbl["수주번호"].astype(str).isin(suju_list)
                            tmp_res = res_tbl.loc[mask_res]
                            if not tmp_res.empty:
                                if "생산수량" in tmp_res.columns:
                                    prod = tmp_res["생산수량"].apply(safe_num).sum()
                                if "QC샘플" in tmp_res.columns:
                                    qc = tmp_res["QC샘플"].apply(safe_num).sum()
                                if "기타샘플" in tmp_res.columns:
                                    etc = tmp_res["기타샘플"].apply(safe_num).sum()

                        orig_def = safe_num(row.get("원불", 0))
                        proc_def = safe_num(row.get("작불", 0))
                        unit = safe_num(row.get("단위수량", 0))

                        row["ERP불출수량"] = erp_out
                        row["현장실물입고"] = real_in
                        row["생산수량"] = prod
                        row["QC샘플"] = qc
                        row["기타샘플"] = etc

                        row["예상재고"] = (
                            real_in
                            - (prod + qc + etc) * unit
                            - orig_def
                            - proc_def
                        )

                        return row

                    df_full.loc[target_idx] = df_full.loc[target_idx].apply(
                        recompute_row_with_extra_orders, axis=1
                    )

                # 🔚 최종값 저장 후 즉시 다시 렌더 → 1번 클릭에도 결과 보이게
                st.session_state["환입재고예상"] = df_full
                import streamlit as st  # 이미 위에 있으면 생략
                st.rerun()

            else:
                # 저장 버튼만 눌렀을 때
                st.success("공통부자재 / 추가수주 변경 내용을 저장했습니다.")

        # -------------------------------------------------
        # 4) 계산 결과 (보기용) - 여기에서만 라벨선택 노출
        #    (여기 아래는 기존 코드 그대로 써도 됨)
        # -------------------------------------------------
        df_full = st.session_state["환입재고예상"].copy()

        visible_cols = [c for c in VISIBLE_COLS if c in df_full.columns]
        result_cols = visible_cols.copy()
        if "라벨선택" in df_full.columns:
            result_cols.append("라벨선택")

        df_result_view = df_full[result_cols].copy()
        if "라벨선택" in df_result_view.columns:
            df_result_view["라벨선택"] = (
                df_result_view["라벨선택"].fillna(False).astype(bool)
            )

        st.markdown("#### 계산 결과 (보기용)")
        df_result_edit = st.data_editor(
            df_result_view,
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            column_config={
                "라벨선택": st.column_config.CheckboxColumn("라벨선택", default=False)
            },
            key="return_result_editor",
        )

        if "라벨선택" in df_result_edit.columns:
            df_full["라벨선택"] = (
                df_result_edit["라벨선택"].fillna(False).astype(bool)
            )

        st.session_state["환입재고예상"] = df_full

        # ----------------------------------------------------
        # 🔽 여기부터는 기존 CSV / PDF / 라벨 로직 (df_full 기반)
        # ----------------------------------------------------
        
        # ---------- 품번별 수주번호 선택 (CSV 통합용) ----------
        merge_choices = {}
        work = df_full.copy()

        if "품번" in work.columns and "수주번호" in work.columns:
            suju_counts = work.groupby("품번")["수주번호"].nunique()
            dup_parts = suju_counts[suju_counts > 1].index.tolist()

            if dup_parts:
                st.markdown("#### 품번별 수주번호 선택 (CSV 통합용)")
                for part in dup_parts:
                    sub = work[work["품번"] == part]
                    combos = sub[["수주번호", "완성품명"]].drop_duplicates()

                    options = [
                        f"{str(row['수주번호'])} {str(row['완성품명'])}"
                        for _, row in combos.iterrows()
                    ]
                    if not options:
                        continue

                    key = f"merge_choice_{part}"
                    default = st.session_state.get(key, options[0])
                    try:
                        default_index = options.index(default)
                    except ValueError:
                        default_index = 0

                    choice = st.selectbox(
                        f"품번 {part} - 수주/완성품명 선택",
                        options,
                        index=default_index,
                        key=key,
                    )
                    merge_choices[part] = choice

        # ---------- 1단계: (수주번호, 지시번호, 품번) 동일한 행 먼저 통합 ----------
        key_cols = ["수주번호", "지시번호", "품번"]
        key_cols = [c for c in key_cols if c in work.columns]

        if key_cols:
            agg_dict_step1 = {}
            for col in work.columns:
                if col in key_cols:
                    continue
                if col in ["ERP불출수량", "현장실물입고"]:
                    agg_dict_step1[col] = "sum"
                else:
                    agg_dict_step1[col] = "first"

            work = work.groupby(key_cols, as_index=False).agg(agg_dict_step1)

        # ---------- 2단계: 품번 단위로 최종 통합 ----------
        result_rows = []

        header_cols = [
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
            "품명",
        ]

        sum_cols = [
            "ERP불출수량",
            "현장실물입고",
            "지시수량",
            "생산수량",
            "QC샘플",
            "기타샘플",
            "원불",
            "작불",
            "예상재고",
        ]

        unit_col = "단위수량"

        if "품번" in work.columns:
            for part, part_df in work.groupby("품번"):
                # 사용자가 선택한 대표 수주번호 적용
                if part in merge_choices:
                    sel_suju, _, _ = merge_choices[part].partition(" ")
                    base = part_df[part_df["수주번호"].astype(str) == sel_suju]
                    header_row = base.iloc[0] if not base.empty else part_df.iloc[0]
                else:
                    header_row = part_df.iloc[0]

                row = {}
                row["품번"] = part

                # 헤더 계열: 대표 수주/지시의 값 유지
                for col in header_cols:
                    row[col] = header_row.get(col, None)

                # 수량 계열: 모두 합계
                for col in sum_cols:
                    if col in part_df.columns:
                        row[col] = part_df[col].apply(safe_num).sum()
                    else:
                        row[col] = 0

                # 단위수량: 대표값만
                row[unit_col] = safe_num(header_row.get(unit_col, 0))

                # ERP재고: 같은 품번이면 동일 → 대표값만
                if "ERP재고" in part_df.columns:
                    non_na = part_df["ERP재고"].dropna()
                    row["ERP재고"] = (
                        safe_num(non_na.iloc[0]) if not non_na.empty else 0
                    )
                else:
                    row["ERP재고"] = 0

                result_rows.append(row)

        grouped = pd.DataFrame(result_rows) if result_rows else work.copy()

        # CSV 컬럼 정리
        for col in CSV_COLS:
            if col not in grouped.columns:
                grouped[col] = None

        csv_export_df = grouped[CSV_COLS].copy()

        # ---------- CSV 받기 버튼 ----------
        csv_data = csv_export_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 CSV 받기",
            data=csv_data,
            file_name="환입_예상재고_통합.csv",
            mime="text/csv",
        )

        # 🔹 PDF / 비고코멘트 / 바코드 라벨을 좌·우 2열 레이아웃으로 배치
        col_left, col_right = st.columns(2)

        # =========================
        # ⬅️ 왼쪽 컬럼: PDF + 입고 비고 코멘트
        # =========================
        with col_left:
            if REPORTLAB_AVAILABLE and not csv_export_df.empty:
                st.markdown("### 📑 PDF 상단 메모")

                pasted_text = st.text_area(
                    "PDF 메모",
                    height=100,
                    key="pdf_note_text",
                    placeholder="여기에 메모나 특이사항을 입력/붙여넣기 하세요.",
                )

                pdf_bytes = generate_pdf(csv_export_df, pasted_text=pasted_text)

                st.download_button(
                    "📄 PDF 받기",
                    data=pdf_bytes,
                    file_name="환입_예상재고.pdf",
                    mime="application/pdf",
                )
            elif not REPORTLAB_AVAILABLE:
                st.info("PDF 저장 기능을 쓰려면 `pip install reportlab` 설치가 필요합니다.")

            # ----- 입고 시트 비고 코멘트 -----
            st.markdown("### 📝 입고 비고 코멘트")

            in_suju_col = pick_col(df_in_raw, "B", ["수주번호"])
            in_jisi_col = pick_col(df_in_raw, "C", ["지시번호"])
            in_part_col = pick_col(df_in_raw, "M", ["품번"])
            in_cmt_col = pick_col(df_in_raw, "V", ["비고", "비고2"])

            if in_suju_col and in_jisi_col and in_part_col and in_cmt_col:
                df_in_comment = df_in_raw[
                    [in_suju_col, in_jisi_col, in_part_col, in_cmt_col]
                ].copy()
                df_in_comment.columns = ["수주번호", "지시번호", "품번", "비고2"]
                df_in_comment = df_in_comment.dropna(subset=["비고2"])

                if not df_in_comment.empty:
                    df_comment_merge = df_full.merge(
                        df_in_comment,
                        how="left",
                        on=["수주번호", "지시번호", "품번"],
                    )

                    df_comment_show = df_comment_merge.dropna(subset=["비고2"])[
                        ["품번", "품명", "비고2"]
                    ].drop_duplicates()

                    if not df_comment_show.empty:
                        for _, row in df_comment_show.iterrows():
                            st.markdown(
                                f"- **{row['품번']} / {row['품명']}** : {row['비고2']}"
                            )
                    else:
                        st.caption("표시할 비고 코멘트가 없습니다.")
                else:
                    st.caption("입고 시트에 비고 내용이 없습니다.")
            else:
                st.caption("입고 시트에서 비고 컬럼을 찾지 못했습니다.")

        # =========================
        # ➡️ 오른쪽 컬럼: 바코드 입력 + 라벨 PDF
        # =========================
        with col_right:
            st.markdown("### 🏷 부자재반입라벨 출력")

            col_bc, col_unit = st.columns([3, 1])

            with col_bc:
                barcode_value = st.text_input(
                    "부자재반입요청번호",
                    placeholder="예: B202511-00120001",
                    key="barcode_input",
                )

            with col_unit:
                unit_value = st.text_input(
                    "단위수량",
                    key="unit_input",
                )

            pdf_labels = None
            download_disabled = True
            download_help = ""

            if "라벨선택" not in df_full.columns:
                st.error("라벨선택 컬럼을 찾을 수 없습니다.")
            elif "품번" not in df_full.columns:
                st.error("품번 컬럼이 없어 라벨 데이터를 만들 수 없습니다.")
            else:
                selected_parts = (
                    df_full.loc[df_full["라벨선택"] == True, "품번"]
                    .astype(str)
                    .tolist()
                )

                required_cols = ["품명", "품번", "환입일"]
                if not all(col in df_full.columns for col in required_cols):
                    st.error("라벨 생성에 필요한 컬럼(품명, 품번, 환입일)이 부족합니다.")
                else:
                    if not barcode_value:
                        download_help = "부자재반입요청번호를 입력하면 버튼이 활성화됩니다."
                    elif not unit_value:
                        download_help = "단위수량을 입력하면 버튼이 활성화됩니다."
                    elif not selected_parts:
                        download_help = "라벨을 출력할 자재를 한 개 이상 선택하세요."
                    else:
                        df_labels = df_full[
                            df_full["품번"].astype(str).isin(selected_parts)
                        ][required_cols].copy()

                        if df_labels.empty:
                            download_help = "선택한 자재에서 라벨에 사용할 데이터를 찾지 못했습니다."
                        else:
                            try:
                                pdf_labels = generate_label_pdf(
                                    df_labels,
                                    barcode_value,
                                    unit_value,
                                )
                                download_disabled = False
                            except Exception as e:
                                st.error(f"라벨 PDF 생성 중 오류: {e}")
                                download_help = "라벨 PDF 생성 중 오류가 발생했습니다."

            if download_help:
                st.caption(download_help)

            st.download_button(
                "🏷 선택한 자재 바코드 라벨 PDF 만들기",
                data=pdf_labels if pdf_labels is not None else b"",
                file_name="부자재반입라벨.pdf",
                mime="application/pdf",
                disabled=download_disabled,
                key="btn_make_labels",
            )

# ============================================================
# 🧩 5. 공통자재 탭
# ============================================================
if menu == "🧩 공통자재":
    st.subheader("🧩 공통자재 확인")

    search_part = st.text_input(
        "찾을 자재 품번을 입력하세요",
        key="common_part_search",
        placeholder="예: 자재 품번 입력"
    )

    if search_part:
        df_bom = df_bom_raw.copy()

        bom_item_col = pick_col(df_bom, "A", ["품목코드"])
        bom_name_col = pick_col(df_bom, "B", ["품명"])
        bom_part_col = pick_col(df_bom, "C", ["품번"])

        if not all([bom_item_col, bom_name_col, bom_part_col]):
            st.error("BOM 시트에서 품목코드(A), 품명(B), 품번(C) 컬럼을 찾지 못했습니다.")
        else:
            df_bom_hit = df_bom[df_bom[bom_part_col] == search_part].copy()

            if df_bom_hit.empty:
                st.info("해당 자재 품번을 사용하는 품목코드를 BOM에서 찾지 못했습니다.")
            else:
                df_bom_hit = df_bom_hit[[bom_item_col, bom_name_col]].drop_duplicates()
                df_bom_hit.columns = ["완성품번", "품명"]

                df_in = df_in_raw.copy()
                in_fin_col = pick_col(df_in, "D", ["완성품번", "품목코드", "품번"])
                in_req_date_col = pick_col(df_in, "K", ["요청날짜", "요청일"])

                if in_fin_col is None or in_req_date_col is None:
                    st.error("입고 시트에서 완성품번(D열) 또는 요청날짜(K열) 컬럼을 찾지 못했습니다.")
                else:
                    df_in[in_req_date_col] = pd.to_datetime(
                        df_in[in_req_date_col], errors="coerce"
                    ).dt.date

                    today = date.today()
                    result_rows = []

                    for _, r in df_bom_hit.iterrows():
                        item_code = r["완성품번"]
                        name = r["품명"]

                        sub = df_in[df_in[in_fin_col] == item_code].copy()
                        sub = sub.dropna(subset=[in_req_date_col])

                        if sub.empty:
                            last_date = None
                            days_diff = None
                            mark_1w = ""
                            mark_2w = ""
                        else:
                            # 가장 마지막(맨 아래) 행 기준 요청날짜
                            sub = sub.sort_values(in_req_date_col)
                            last_date = sub[in_req_date_col].iloc[-1]
                            days_diff = (today - last_date).days

                            if days_diff <= 7:
                                mark_1w = "V"
                                mark_2w = ""
                            elif days_diff <= 14:
                                mark_1w = ""
                                mark_2w = "V"
                            else:
                                mark_1w = ""
                                mark_2w = ""

                        result_rows.append(
                            {
                                "완성품번": item_code,
                                "품명": name,
                                "불출요청일": last_date,
                                "1주 이내": mark_1w,
                                "2주 이내": mark_2w,
                            }
                        )

                    df_result = pd.DataFrame(result_rows)

                    if df_result.empty:
                        st.info("조건에 해당하는 데이터가 없습니다.")
                    else:
                        # 최신 불출요청일이 위로 오도록 정렬 (선택사항)
                        df_result = df_result.sort_values(
                            by="불출요청일", ascending=False, na_position="last"
                        ).reset_index(drop=True)

                        df_result_styled = df_result.style.set_properties(
                            subset=["1주 이내", "2주 이내"],
                            **{"text-align": "center"}
                        )

                        st.dataframe(df_result, use_container_width=True)

# ============================================================
# 🏷 6. 라벨 수량 계산 탭
# ============================================================
if menu == "🏷 라벨 수량 계산":
    st.subheader("🏷 라벨 수량 계산기")

    # -----------------------------
    # 0) S3 / 세션에서 라벨 DB 로딩
    # -----------------------------
    if "label_db" not in st.session_state:
        df_label_s3 = load_label_db_from_s3()
        if df_label_s3.empty:
            st.info("라벨 DB가 아직 없습니다. 아래에서 기존 라벨 엑셀 파일을 한 번 업로드해 초기화하세요.")

            label_file = st.file_uploader(
                "라벨 DB 초기 엑셀 업로드 (라벨 및 스티커 지관무게+수량 계산기_*.xlsx)",
                type=["xlsx", "xlsm"],
                key="label_db_init_upload",
            )

            if label_file is not None:
                df_init = parse_label_db(label_file)
                if df_init.empty:
                    st.error("엑셀에서 읽어온 라벨 데이터가 없습니다. 시트/헤더 위치를 다시 확인해주세요.")
                else:
                    save_label_db_to_s3(df_init)
                    st.session_state["label_db"] = df_init
                    st.success(
                        f"라벨 DB를 {len(df_init)}행으로 초기화했습니다. "
                        "(이제부터는 엑셀 업로드 없이 사용 가능합니다.)"
                    )
                    st.dataframe(
                        df_init[["샘플번호", "품번", "품명", "구분"]].head(20),
                        use_container_width=True,
                    )
            st.stop()
        else:
            st.session_state["label_db"] = df_label_s3

    # 여기까지 오면 df_label 존재
    df_label: pd.DataFrame = st.session_state["label_db"]
    df_label = normalize_label_df(df_label)  # 혹시 모를 컬럼 정리
    st.session_state["label_db"] = df_label

    # -----------------------------
    # 1) 라벨 수량 계산기
    # -----------------------------
    st.markdown("### 🔢 수량 계산")

    col_calc_left, col_calc_right = st.columns([2, 1])

    with col_calc_left:
        calc_search = st.text_input(
            "라벨 품번 검색 (부분일치, '-' 뒤 기준)",
            key="label_calc_search",
            placeholder="예: 027A14 → 2KKMMSK-027A14-xxx 등을 찾음",
        )

        selected_row = None

        if calc_search:
            search_key = calc_search.split("-")[-1].strip()
            if search_key:
                mask_label = df_label["품번"].astype(str).str.contains(search_key, na=False)
                df_hit_calc = df_label.loc[mask_label].copy()

                if df_hit_calc.empty:
                    st.info("해당 조건에 맞는 라벨 품목이 없습니다.")
                else:
                    df_hit_calc = df_hit_calc.reset_index().rename(columns={"index": "_orig_index"})
                    options = [
                        f"{row['품번']} | {row['품명']} ({row.get('구분', '')})"
                        for _, row in df_hit_calc.iterrows()
                    ]
                    selected_opt = st.selectbox(
                        "검색 결과에서 사용할 라벨 선택",
                        options=options,
                        key="label_calc_select",
                    )
                    sel_idx = options.index(selected_opt)
                    selected_row = df_hit_calc.iloc[sel_idx]

        film_weight = st.number_input(
            "필름무게 (g)",
            min_value=0.0,
            step=0.1,
            key="label_calc_film_weight",
        )

    with col_calc_right:
        core_weight_db = 0.0
        est_core_db = 0.0
        sample_weight_db = 0.0
        sample_count_db = 0.0
        label_info_text = "라벨 정보를 선택하면 여기에 표시됩니다."

        if selected_row is not None:
            part = str(selected_row.get("품번", ""))
            name = str(selected_row.get("품명", ""))
            gubun = str(selected_row.get("구분", ""))

            core_weight_db = safe_num(selected_row.get("지관무게", 0.0))
            est_core_db = safe_num(selected_row.get("추정값", 0.0))

            # ✅ 지관무게가 없으면 추정값 사용
            if core_weight_db <= 0 and est_core_db > 0:
                core_weight_default = est_core_db
                core_source = "추정값 사용"
            else:
                core_weight_default = core_weight_db
                core_source = "실측 지관무게 사용"

            sample_weight_db = safe_num(selected_row.get("샘플무게", 0.0))
            sample_count_db = parse_label_sample_count(selected_row.get("기준샘플", ""))

            label_info_text = (
                f"**품번**: {part}\n\n"
                f"**품명**: {name}\n\n"
                f"**구분**: {gubun}\n\n"
                f"**지관무게(실측)**: {core_weight_db:.2f} g\n"
                f"**지관무게(추정값)**: {est_core_db:.2f} g\n"
                f"→ 현재 계산에 사용할 값: **{core_weight_default:.2f} g** ({core_source})\n\n"
                f"**기준샘플**: {selected_row.get('기준샘플', '')} "
                f"(약 {sample_count_db:g} 매)\n"
                f"**샘플무게**: {sample_weight_db:.2f} g"
            )

        st.markdown(label_info_text)

    # 실제 계산 입력 (지관무게는 기본값 = DB의 실측 or 추정값)
    col_calc2_1, col_calc2_2, col_calc2_3 = st.columns(3)
    with col_calc2_1:
        core_weight_input = st.number_input(
            "지관무게 (g, 필요하면 수정)",
            min_value=0.0,
            step=0.1,
            value=float(core_weight_db if core_weight_db > 0 else est_core_db),
            key="label_calc_core_weight",
        )
    with col_calc2_2:
        sample_weight_input = st.number_input(
            "샘플무게 (g, 필요시 수정)",
            min_value=0.0,
            step=0.01,
            value=float(sample_weight_db),
            key="label_calc_sample_weight",
        )
    with col_calc2_3:
        sample_count_input = st.number_input(
            "기준샘플 매수 (숫자만)",
            min_value=0.0,
            step=1.0,
            value=float(sample_count_db),
            key="label_calc_sample_count",
        )

    # 결과 계산
    if film_weight > 0 and sample_weight_input > 0 and sample_count_input > 0:
        net_film = film_weight - core_weight_input
        if net_film <= 0:
            st.error("필름무게가 지관무게보다 작거나 같습니다. 값을 다시 확인해주세요.")
        else:
            qty = net_film / sample_weight_input * sample_count_input
            st.metric("계산 결과 (장수 기준)", f"{qty:,.1f} 매")
            st.caption(f"정수로 내리면: **{int(qty):,} 매**")
    else:
        st.caption("필름무게, 샘플무게, 기준샘플 매수를 모두 입력하면 결과가 계산됩니다.")

    # -----------------------------
    # 2) 새 라벨 품목 추가하기 (계산기 바로 아래)
    # -----------------------------
    with st.expander("➕ 새 라벨 품목 추가하기", expanded=False):
        st.caption("BOM 시트의 품번(C열)을 부분일치로 검색해서 품명을 확인한 뒤, 새 라벨 품목을 DB에 추가합니다.")

        # --- BOM 검색 (가능한 경우에만) ---
        selected_part_from_bom = None
        selected_name_from_bom = None

        if "df_bom_raw" in globals():
            df_bom_for_label = df_bom_raw.copy()

            # ✅ 품번은 C열 기준
            bom_part_col = pick_col(df_bom_for_label, "C", ["품번"])
            # BOM의 품명 컬럼 (D열 우선, 없으면 B열)
            bom_name_col = pick_col(df_bom_for_label, "D", ["품명"])
            if bom_name_col is None:
                bom_name_col = pick_col(df_bom_for_label, "B", ["품명"])

            new_bom_search = st.text_input(
                "BOM 자재 품번 검색 (부분일치, C열 기준)",
                key="label_new_bom_search",
                placeholder="예: 027A14, 038B12 등",
            )

            if new_bom_search and bom_part_col and bom_name_col:
                mask_part = df_bom_for_label[bom_part_col].astype(str).str.contains(
                    new_bom_search, na=False
                )

                # ✅ 품명의 끝부분에 '_라벨' 또는 '_엠블럼' 이 포함된 것만
                name_series = df_bom_for_label[bom_name_col].astype(str)
                mask_name = (
                    name_series.str.contains(r"_.*라벨", na=False)
                    | name_series.str.contains(r"_.*엠블럼", na=False)
                )

                mask_bom = mask_part & mask_name

                df_bom_hit = (
                    df_bom_for_label.loc[mask_bom, [bom_part_col, bom_name_col]]
                    .drop_duplicates()
                    .head(50)
                )
                if not df_bom_hit.empty:
                    df_bom_hit = df_bom_hit.rename(
                        columns={bom_part_col: "BOM_품번", bom_name_col: "BOM_품명"}
                    ).reset_index(drop=True)

                    st.dataframe(
                        df_bom_hit,
                        use_container_width=True,
                        height=200,
                    )

                    # 🔽 검색 결과에서 하나 선택 → 아래 입력칸에 자동 반영
                    options_bom = [
                        f"{row['BOM_품번']} | {row['BOM_품명']}"
                        for _, row in df_bom_hit.iterrows()
                    ]
                    selected_bom_opt = st.selectbox(
                        "검색 결과에서 라벨/엠블럼 품목 선택",
                        ["선택 안 함"] + options_bom,
                        key="label_new_bom_select",
                    )

                    if selected_bom_opt != "선택 안 함":
                        idx_sel = options_bom.index(selected_bom_opt)
                        row_sel = df_bom_hit.iloc[idx_sel]
                        selected_part_from_bom = str(row_sel["BOM_품번"])
                        selected_name_from_bom = str(row_sel["BOM_품명"])

                        # 👉 텍스트 입력 기본값으로 넣어주기
                        st.session_state["label_new_part"] = selected_part_from_bom
                        st.session_state["label_new_name"] = selected_name_from_bom
                else:
                    st.caption("검색 조건에 맞는 BOM 행이 없습니다. (라벨/엠블럼 품목만 표시합니다.)")
            elif not bom_part_col or not bom_name_col:
                st.warning("BOM 시트에서 품번(C열) 또는 품명(D열/B열) 컬럼을 찾지 못했습니다.")
        else:
            st.info("BOM 시트 검색은 메인 부자재 DB 업로드 후 사용 가능합니다.")

        st.markdown("#### 실제로 DB에 저장할 라벨 정보 입력")

        # 선택 가능한 구분 목록
        if "LABEL_TYPES" in globals():
            gubun_choices = LABEL_TYPES
        elif "구분" in df_label.columns:
            gubun_choices = sorted(df_label["구분"].dropna().unique().tolist())
        else:
            gubun_choices = []

        new_part = st.text_input(
            "라벨 품번 (DB에 저장할 실제 품번)",
            key="label_new_part",
            placeholder="예: 2KKMMSK-027A14-xxx",
        )
        new_name = st.text_input(
            "품명",
            key="label_new_name",
        )
        new_gubun = st.selectbox(
            "구분",
            options=gubun_choices if gubun_choices else ["(직접 입력)"],
            key="label_new_gubun",
        )

        col_dim1, col_dim2, col_dim3 = st.columns(3)
        with col_dim1:
            new_od = st.number_input(
                "외경 (mm)",
                min_value=0.0,
                step=0.1,
                key="label_new_od",
            )
        with col_dim2:
            new_id = st.number_input(
                "내경 (mm)",
                min_value=0.0,
                step=0.1,
                key="label_new_id",
            )
        with col_dim3:
            new_h = st.number_input(
                "높이 (mm)",
                min_value=0.0,
                step=0.1,
                key="label_new_h",
            )

        # 🔍 외경/내경/높이로 측정값(추정값) 미리 보기
        est_val_preview = 0.0
        if new_od > 0 and new_id > 0 and new_h > 0:
            est_val_preview = 3.14 * new_h * ((new_od ** 2 - new_id ** 2) / 4.0) * 0.78
            est_val_preview = round(est_val_preview, 2)
        st.metric("측정값 (추정 지관무게, g)", f"{est_val_preview:.2f}")

        col_sample1, col_sample2 = st.columns(2)
        with col_sample1:
            new_base_str = st.text_input(
                "기준샘플 (예: '4매', '2매(아이마크)')",
                key="label_new_base_str",
                placeholder="예: 4매",
            )
        with col_sample2:
            new_sample_weight = st.number_input(
                "샘플무게 (g)",
                min_value=0.0,
                step=0.01,
                key="label_new_sample_weight",
            )

        new_core_weight = st.number_input(
            "실측 지관무게 (g, 선택입력)",
            min_value=0.0,
            step=0.1,
            key="label_new_core_weight",
        )

        if st.button("✅ 입력 완료 (DB에 저장)", key="label_new_save_btn"):
            # 필수값 체크
            if not new_part or not new_name:
                st.error("품번과 품명은 반드시 입력해야 합니다.")
            elif new_od <= 0 or new_id <= 0 or new_h <= 0:
                st.error("외경, 내경, 높이는 모두 0보다 큰 값이어야 합니다.")
            elif new_sample_weight <= 0:
                st.error("샘플무게(g)는 0보다 큰 값이어야 합니다.")
            else:
                # 추정값 계산
                est_val = 3.14 * new_h * ((new_od ** 2 - new_id ** 2) / 4.0) * 0.78
                est_val = round(est_val, 2)

                # 오차: 실측 지관무게가 있으면 (추정값 - 실무게), 없으면 0
                if new_core_weight > 0:
                    err_val = est_val - new_core_weight
                else:
                    err_val = 0.0

                new_row = {
                    "샘플번호": None,
                    "품번": new_part,
                    "품명": new_name,
                    "구분": new_gubun if new_gubun != "(직접 입력)" else "",
                    "지관무게": new_core_weight if new_core_weight > 0 else 0.0,
                    "추정값": est_val,
                    "오차": err_val,
                    "외경": new_od,
                    "내경": new_id,
                    "높이": new_h,
                    "1R무게": None,
                    "기준샘플": new_base_str,
                    "샘플무게": new_sample_weight,
                }

                df_label_new = pd.concat(
                    [df_label, pd.DataFrame([new_row])],
                    ignore_index=True,
                )
                df_label_new = normalize_label_df(df_label_new)

                st.session_state["label_db"] = df_label_new
                save_label_db_to_s3(df_label_new)

                st.success(f"새 라벨 품목이 DB에 추가되었습니다. (품번: {new_part})")

    # -----------------------------
    # 3) 라벨 검색 + 삭제
    # -----------------------------
    st.markdown("### 🔍 라벨 검색 / 삭제")

    label_search = st.text_input(
        "품번 또는 품명 검색 (부분일치)",
        key="label_search",
        placeholder="예: 품번 일부 또는 품명 일부",
    )

    if label_search:
        mask_search = (
            df_label["품번"].astype(str).str.contains(label_search, na=False)
            | df_label["품명"].astype(str).str.contains(label_search, na=False)
        )
        df_search = df_label.loc[mask_search].copy()

        if df_search.empty:
            st.info("검색 조건에 맞는 라벨 품목이 없습니다.")
        else:
            df_search = df_search.reset_index().rename(columns={"index": "_orig_index"})
            df_search["삭제"] = False

            display_cols = [
                "삭제",
                "품번",
                "품명",
                "구분",
                "지관무게",
                "추정값",
                "기준샘플",
                "샘플무게",
            ]
            display_cols = [c for c in display_cols if c in df_search.columns]

            df_search_view = df_search[display_cols + ["_orig_index"]]

            df_search_edit = st.data_editor(
                df_search_view,
                use_container_width=True,
                num_rows="fixed",
                hide_index=True,
                column_config={
                    "삭제": st.column_config.CheckboxColumn("삭제", default=False)
                },
                key="label_search_editor",
            )

            if st.button("🗑 선택한 라벨 삭제", key="label_delete_btn"):
                to_delete_idx = df_search_edit.loc[
                    df_search_edit["삭제"] == True, "_orig_index"
                ].tolist()

                if not to_delete_idx:
                    st.warning("삭제할 라벨을 선택해주세요.")
                else:
                    df_label_after = df_label.drop(index=to_delete_idx).reset_index(drop=True)
                    df_label_after = normalize_label_df(df_label_after)
                    st.session_state["label_db"] = df_label_after
                    save_label_db_to_s3(df_label_after)
                    st.success(f"선택한 라벨 {len(to_delete_idx)}개를 삭제했습니다.")
                    st.experimental_rerun()

    # -----------------------------
    # 4) 라벨 DB 미리보기 / (선택적) 전체 편집
    # -----------------------------
    with st.expander("📋 라벨 DB 미리보기 / 저장", expanded=False):
        cols_preview = [
            c
            for c in ["샘플번호", "품번", "품명", "구분", "지관무게", "추정값", "기준샘플", "샘플무게"]
            if c in df_label.columns
        ]
        st.dataframe(df_label[cols_preview], use_container_width=True, height=300)

        edit_mode = st.checkbox(
            "✏️ 라벨 DB 전체 편집 모드 켜기 (느려질 수 있어요)",
            key="label_db_edit_mode",
            value=False,
        )

        if edit_mode:
            df_edit = st.data_editor(
                df_label,
                use_container_width=True,
                num_rows="dynamic",
                key="label_db_editor",
            )

            if st.button("💾 라벨 DB 저장 (S3 반영)", key="label_db_save_btn"):
                df_edit_norm = normalize_label_df(df_edit.copy())
                st.session_state["label_db"] = df_edit_norm
                save_label_db_to_s3(df_edit_norm)
                st.success("라벨 DB를 S3에 저장했습니다.")

        # 엑셀로 내보내기
        excel_buf = io.BytesIO()
        df_label.to_excel(excel_buf, index=False, sheet_name="라벨DB")
        excel_buf.seek(0)
        st.download_button(
            "📥 현재 라벨 DB 엑셀로 다운로드",
            data=excel_buf,
            file_name="라벨_DB_현재버전.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="label_db_download_btn",
        )

