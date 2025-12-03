import streamlit as st
import pandas as pd
from datetime import date, timedelta
import io
import os
from html import escape

# ============ S3 연동 ============

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = "rec-and-ship"
S3_KEY = "bulk-ledger.xlsx"  # 항상 이 이름으로 저장/불러오기

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
    """S3에 파일이 있으면 bytes로 읽어온다."""
    if s3_client is None:
        return None
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        return obj["Body"].read()
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return None
        st.error(f"S3에서 파일을 가져오는 중 오류가 발생했습니다: {e}")
        return None

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

    # === 3) 생산실적 집계: 수주번호별 양품 / QC샘플 / 기타샘플 합계 ===
    # 수주번호: 보통 "수주번호" 컬럼 사용
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

    # 최소한 수주번호는 있어야 집계 가능
    if res_suju_col:
        use_cols = [res_suju_col]
        if res_good_col:
            use_cols.append(res_good_col)
        if res_qc_col:
            use_cols.append(res_qc_col)
        if res_etc_col:
            use_cols.append(res_etc_col)

        df_res = df_result_raw[use_cols].copy()

        rename_map = {res_suju_col: "수주번호"}
        if res_good_col:
            rename_map[res_good_col] = "생산수량"
        if res_qc_col:
            rename_map[res_qc_col] = "QC샘플"
        if res_etc_col:
            rename_map[res_etc_col] = "기타샘플"

        df_res = df_res.rename(columns=rename_map)

        # NaN → 0 처리 후 수주번호 기준 합계
        for col in ["생산수량", "QC샘플", "기타샘플"]:
            if col in df_res.columns:
                df_res[col] = df_res[col].apply(safe_num)

        agg_res = df_res.groupby("수주번호", as_index=False).agg("sum")

        aggregates["result"] = agg_res
    else:
        aggregates["result"] = pd.DataFrame(
            columns=["수주번호", "생산수량", "QC샘플", "기타샘플"]
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

    # 3) 생산실적 집계 붙이기 (수주번호 기준: 생산수량 / QC / 기타샘플)
    df = df.merge(
        aggs["result"],
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
    from reportlab.platypus import PageBreak
    from reportlab.graphics.barcode import code128
    from reportlab.graphics.shapes import Drawing
    from reportlab.lib.units import mm

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
                ("MINROWHEIGHT",    (0, 0), (-1, -1), 35),
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

        # 🔥 행 높이 (헤더는 기본, 데이터 행만 높게)
        default_height = None        # 헤더
        data_height = 40             # 데이터 행
        row_heights = [default_height] + [data_height] * (len(table_data) - 1)

        # 🔥 컬럼 폭 설정
        #  - 앞의 5개 컬럼은 None(자동)
        #  - 1P~4P 4칸만 넓게(예: 80pt씩) → 필요하면 숫자 키워서 조절
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

    # 🔹 부자재반입 라벨 PDF 생성용
    def generate_label_pdf(df_labels: pd.DataFrame) -> bytes:
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        import io

        buffer = io.BytesIO()

        doc = SimpleDocTemplate(
            buffer,
            pagesize=A4,
            leftMargin=40,
            rightMargin=40,
            topMargin=40,
            bottomMargin=40,
        )

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            "LabelTitle",
            parent=styles["Heading1"],
            fontName=KOREAN_FONT_NAME,
            fontSize=32,
            alignment=1,
        )
        text_style = ParagraphStyle(
            "LabelText",
            parent=styles["Normal"],
            fontName=KOREAN_FONT_NAME,
            fontSize=14,
            leading=20,
            alignment=0,
        )

        story = []

        for idx, row in df_labels.iterrows():
            품명 = str(row.get("품명", ""))
            품번 = str(row.get("품번", ""))
            단위수량 = str(row.get("단위수량", ""))
            환입일 = row.get("환입일", "")

            try:
                if pd.notna(환입일):
                    환입일_str = pd.to_datetime(환입일).strftime("%Y-%m-%d")
                else:
                    환입일_str = ""
            except:
                환입일_str = str(환입일)

            date_for_barcode = (
                pd.to_datetime(환입일).strftime("%y%m%d")
                if pd.notna(환입일) else date.today().strftime("%y%m%d")
            )
            barcode_value = f"B{date_for_barcode}-{idx+1:07d}"

            story.append(Paragraph("부자재반입", title_style))
            story.append(Spacer(1, 30))

            lines = [
                f"품명      {escape(품명)}",
                f"품목코드  {escape(품번)}",
                f"단위수량  {escape(단위수량)}",
                f"반입일자  {escape(환입일_str)}",
            ]
            for line in lines:
                story.append(Paragraph(line, text_style))
                story.append(Spacer(1, 6))

            story.append(Spacer(1, 40))

            bc = code128.Code128(barcode_value, barHeight=20*mm, barWidth=0.5)
            drawing = Drawing(0, 0)
            drawing.add(bc)
            story.append(drawing)

            story.append(Spacer(1, 10))
            story.append(Paragraph(barcode_value, text_style))

            if idx != len(df_labels) - 1:
                story.append(PageBreak())

        doc.build(story)
        pdf_bytes = buffer.getvalue()
        buffer.close()
        return pdf_bytes

else:
    def generate_pdf(*args, **kwargs):
        raise RuntimeError("reportlab 패키지가 설치돼 있지 않습니다.")
    def generate_label_pdf(*args, **kwargs):
        raise RuntimeError("reportlab 패키지가 설치돼 있지 않습니다.")


# -----------------------------
# 메인 화면
# -----------------------------
st.title("부자재 관리 시스템")

menu = st.radio(
    "메뉴 선택",
    ["📤 파일 업로드", "📦 입고 조회", "↩️ 환입 관리", "🔍 수주 찾기", "🧩 공통자재"],
    horizontal=True,
)

# ==========================================
# 📤 1. 파일 업로드 탭 (S3에 저장)
# ==========================================
if menu == "📤 파일 업로드":
    st.subheader("📤 2025년 부자재 관리대장 업로드")

    uploaded_file = st.file_uploader("파일 업로드", type=["xlsm", "xlsx"])

    if uploaded_file and s3_client is not None:
        try:
            s3_client.upload_fileobj(uploaded_file, S3_BUCKET, S3_KEY)
            # 캐시 초기화
            load_file_from_s3.clear()
            load_excel.clear()
            st.success("S3 업로드 완료! 다른 탭에서 데이터 조회 가능합니다.")
        except Exception as e:
            st.error(f"S3 업로드 중 오류 발생: {e}")
    elif uploaded_file and s3_client is None:
        st.error("S3 클라이언트가 초기화되지 않았습니다. secrets 설정을 확인해주세요.")

    st.stop()  # 업로드 탭에서는 여기서 종료


# ==========================================
# 나머지 탭: S3에서 파일 로딩
# ==========================================
file_bytes = load_file_from_s3()
if file_bytes is None:
    st.warning("S3에 업로드된 관리대장 파일이 없습니다. 먼저 [📤 파일 업로드] 탭에서 파일을 올려주세요.")
    st.stop()

sheets = load_excel(file_bytes)

# 필수 시트 체크
required_sheets = ["입고", "작업지시", "수주", "BOM", "재고", "생산실적", "불량"]
missing = [s for s in required_sheets if s not in sheets]
if missing:
    st.error(f"다음 시트가 엑셀에 없습니다: {', '.join(missing)}")
    st.stop()

df_in_raw = sheets["입고"]
df_job_raw = sheets["작업지시"]
df_suju_raw = sheets["수주"]
df_bom_raw = sheets["BOM"]
df_stock_raw = sheets["재고"]
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

        start_date, end_date = st.date_input(
            "요청날짜 범위 선택",
            (default_start, today),
            key="in_date_range",
        )

        # Streamlit 버전에 따라 tuple 로 들어올 수 있어서 방어 코드
        if isinstance(start_date, (tuple, list)):
            start_date, end_date = start_date

        # 필터 마스크
        mask = (df_in[req_date_col] >= start_date) & (df_in[req_date_col] <= end_date)

        # 각 열 컬럼 찾기
        col_req_no   = pick_col(df_in, "L", ["요청번호"])
        col_part     = pick_col(df_in, "M", ["품번"])
        col_name     = pick_col(df_in, "O", ["품명"])
        col_req_qty  = pick_col(df_in, "P", ["요청수량"])
        col_erp_out  = pick_col(df_in, "Q", ["ERP불출수량", "불출수량"])
        col_real_in  = pick_col(df_in, "R", ["현장실물입고"])

        raw_cols = [c for c in [
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
            if col_req_no:  rename_map[col_req_no]  = "요청번호"
            if col_part:    rename_map[col_part]    = "품번"
            if col_name:    rename_map[col_name]    = "품명"
            if col_req_qty: rename_map[col_req_qty] = "요청수량"
            if col_erp_out: rename_map[col_erp_out] = "ERP불출수량"
            if col_real_in: rename_map[col_real_in] = "현장실물입고"

            df_filtered.rename(columns=rename_map, inplace=True)

            # 🔥 엑셀에서 "마지막(맨 아래) 행"이 위로 오도록: 인덱스 역순 정렬
            df_filtered = df_filtered.iloc[::-1].reset_index(drop=True)

            if df_filtered.empty:
                st.info("선택한 기간에 해당하는 입고 데이터가 없습니다.")
            else:
                st.dataframe(df_filtered, use_container_width=True)

                # CSV 다운로드
                csv_inbound = df_filtered.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "📥 이 조회 결과를 CSV로 받기",
                    data=csv_inbound,
                    file_name=f"입고조회_{start_date}_{end_date}.csv",
                    mime="text/csv",
                )


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

                # A열 = 품목코드, B열 = 품명
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
                                suju_cols = list(df_suju.columns)

                                suju_part_col = pick_col(df_suju, "J", ["품번"])
                                suju_due_col = pick_col(df_suju, "G", ["조정납기일자"])

                                df_suju[suju_due_col] = pd.to_datetime(
                                        df_suju[suju_due_col], errors="coerce"
                                ).dt.date

                                # 1차 품목코드로 검색
                                df_suju_hit = df_suju[df_suju[suju_part_col].isin(item_codes)].copy()

                                # 없으면 상위(2차) 품목코드로 재검색
                                if df_suju_hit.empty:
                                        fallback_item_codes = set()
                                        for code in item_codes:
                                                df_bom_lvl2 = df_bom[df_bom[bom_component_col] == code]
                                                if not df_bom_lvl2.empty:
                                                        lvl2 = df_bom_lvl2[bom_item_col].dropna().unique().tolist()
                                                        fallback_item_codes.update(lvl2)

                                        fallback_item_codes = list(fallback_item_codes)
                                        st.info("1차 품목코드로는 없어, 2차 상위 품목코드로 재검색합니다.")
                                        st.write("2차 품목코드:", fallback_item_codes)

                                        df_suju_hit = df_suju[df_suju[suju_part_col].isin(fallback_item_codes)].copy()

                                if df_suju_hit.empty:
                                        st.warning("해당 품목코드로 수주 시트에서 검색된 수주가 없습니다.")
                                else:
                                        # === 검색 범위 설정 ===
                                        one_month_after = today + timedelta(days=30)
                                        one_year_after  = today + timedelta(days=365)

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
                                                        df_suju_hit[suju_due_col].between(today, one_year_after)
                                                ].copy()

                                                if not df_1y.empty:
                                                        st.info("1개월 이내는 없고, 1년 이내 수주가 있습니다.")
                                                        df_1y.sort_values(by=suju_due_col, ascending=False, inplace=True)
                                                        df_show = df_1y

                                                else:
                                                        # 3) 과거 탐색: 3개월·6개월·12개월
                                                        back_3m  = today - timedelta(days=90)
                                                        back_6m  = today - timedelta(days=180)
                                                        back_12m = today - timedelta(days=365)

                                                        df_back3 = df_suju_hit[
                                                                df_suju_hit[suju_due_col].between(back_3m, today)
                                                        ].copy()

                                                        if not df_back3.empty:
                                                                st.info("1년 이내 수주는 없어서, 과거 3개월 수주를 보여줍니다.")
                                                                df_back3.sort_values(by=suju_due_col, ascending=False, inplace=True)
                                                                df_show = df_back3

                                                        else:
                                                                df_back6 = df_suju_hit[
                                                                        df_suju_hit[suju_due_col].between(back_6m, today)
                                                                ].copy()

                                                                if not df_back6.empty:
                                                                        st.info("3개월 이내 없음 → 과거 6개월 수주 표시.")
                                                                        df_back6.sort_values(by=suju_due_col, ascending=False, inplace=True)
                                                                        df_show = df_back6

                                                                else:
                                                                        df_back12 = df_suju_hit[
                                                                                df_suju_hit[suju_due_col].between(back_12m, today)
                                                                        ].copy()

                                                                        if not df_back12.empty:
                                                                                st.info("6개월 이내 없음 → 과거 12개월 수주 표시.")
                                                                                df_back12.sort_values(by=suju_due_col, ascending=False, inplace=True)
                                                                                df_show = df_back12
                                                                        else:
                                                                                st.warning("과거 12개월까지도 해당 품목코드의 수주가 없습니다.")
                                                                                df_show = pd.DataFrame()

                                        # ===== 결과 표시 =====
                                        if not df_show.empty:
                                                display_cols = []
                                                for c in [suju_part_col, "품명", "수주번호", suju_due_col, "수량", "매출처"]:
                                                        if c in df_show.columns:
                                                                display_cols.append(c)

                                                st.dataframe(df_show[display_cols], use_container_width=True)
                                            
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
                                            job_suju_col = pick_col(df_job_raw, "A", ["수주번호"])
                                            job_jisi_col = pick_col(df_job_raw, "B", ["지시번호"])
                                            job_name_col = pick_col(df_job_raw, "L", ["품명", "완성품명"])

                                            if not all([job_suju_col, job_jisi_col, job_name_col]):
                                                st.info("작업지시 시트에서 수주번호(A), 지시번호(B), 품명(L)을 모두 찾지 못했습니다.")
                                            else:
                                                # 3) 필요한 컬럼만 가져오기
                                                df_job_map = df_job_raw[
                                                    [job_suju_col, job_jisi_col, job_name_col]
                                                ].copy()
                                                df_job_map.columns = ["수주번호", "지시번호", "품명"]

                                                # 문자열 매칭을 위해 변환
                                                df_job_map["수주번호_str"] = df_job_map["수주번호"].astype(str)

                                                # 4) 수주찾기에서 나온 수주번호 목록과 일치하는 행 필터링
                                                df_job_filtered = df_job_map[
                                                    df_job_map["수주번호_str"].isin(suju_values)
                                                ].drop(columns=["수주번호_str"])

                                                if df_job_filtered.empty:
                                                    st.info("작업지시 시트에서 해당 수주번호의 지시번호/품명을 찾지 못했습니다.")
                                                else:
                                                    # 중복 제거
                                                    df_job_filtered = df_job_filtered.drop_duplicates(
                                                        subset=["수주번호", "지시번호", "품명"]
                                                    )

                                                    st.markdown("#### 수주번호별 지시번호 / 품명 (작업지시 기준)")
                                                    st.dataframe(
                                                        df_job_filtered[["수주번호", "지시번호", "품명"]],
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
        placeholder="예: 앰플, 크림, 마스크팩 등"
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
                if in_suju_col:      rename_map[in_suju_col] = "수주번호"
                if in_jisi_col:      rename_map[in_jisi_col] = "지시번호"
                if in_prod_name_col: rename_map[in_prod_name_col] = "제품명"
                if in_part_col:      rename_map[in_part_col] = "품번"

                df_show.rename(columns=rename_map, inplace=True)

                # 품번 제거
                if "품번" in df_show.columns:
                    df_show = df_show.drop(columns=["품번"])

                # 요청날짜는 중복 제거 기준에서 제외하고,
                # 수주번호 + 지시번호만 유일하도록 정리
                uniq_cols = [c for c in ["수주번호", "지시번호"] if c in df_show.columns]
                df_show = df_show.drop_duplicates(subset=uniq_cols)

                st.dataframe(df_show, use_container_width=True)

                # 🔽 검색 결과에서 한 행을 선택하면 아래 수주번호/지시번호 자동 채우기
                if "수주번호" in df_show.columns:
                    df_select = df_show.reset_index(drop=True)

                    option_labels = []
                    option_map = {}

                    for _, row in df_select.iterrows():
                        suju_val = str(row.get("수주번호", ""))
                        jisi_val = str(row.get("지시번호", ""))
                        prod_val = str(row.get("제품명", ""))

                        # 화면에 보여줄 라벨
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
                        # 아래 입력칸에 자동 반영
                        st.session_state["return_suju_no"] = sel_suju
                        if sel_jisi:
                            st.session_state["return_jisi"] = sel_jisi

    
    # ----- 입력 1줄 (수주번호, 지시번호, 생산공정, 종료조건) -----
    col_suju, col_jisi, col_proc, col_reason = st.columns(4)
    with col_suju:
        suju_no = st.text_input("수주번호", key="return_suju_no")
    with col_jisi:
        selected_jisi = None  # 옵션 생성 후 채움
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
        process_value = st.selectbox(
            "생산공정", process_options, key="return_process"
        )
    with col_reason:
        finish_reason = st.text_input("종료조건", key="return_finish_reason")

    # 수주번호 기반 지시번호/완성품번 후보 찾기
    jisi_options = []
    finished_part_selected = None

    if suju_no:
        if "수주번호" in df_job_raw.columns:
            df_job_suju = df_job_raw[df_job_raw["수주번호"] == suju_no].copy()

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

            if "지시번호" in df_job_suju.columns:
                jisi_options = df_job_suju["지시번호"].dropna().unique().tolist()
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
            else (bom_name_cols[0] if bom_name_cols else None)
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

    # ----- 환입 데이터 불러오기 버튼 -----
    if st.button(
        "✅ 환입 데이터 불러오기",
        key="btn_return_load",
    ):
        if not suju_no:
            st.error("수주번호를 입력해주세요.")
        elif not selected_jisi:
            st.error("지시번호를 선택해주세요.")
        elif bom_component_df.empty:
            st.error("BOM 자재 목록이 없습니다.")
        else:
            selected_rows = bom_component_df[bom_component_df["선택"] == True].copy()
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
                            "제품명": finished_name,  # 완성품명
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

                # 기존 + 신규 합쳐서 [수주번호, 지시번호, 품번] 기준 중복 제거
                df_return = pd.concat([df_return, df_new], ignore_index=True)
                df_return = df_return.drop_duplicates(
                    subset=["수주번호", "지시번호", "품번"], keep="last"
                ).reset_index(drop=True)
                st.session_state["환입관리"] = df_return

                # 집계가 아직 없으면 여기서 한 번만 계산
                if st.session_state["aggregates"] is None:
                    st.session_state["aggregates"] = build_aggregates(
                        df_in_raw,
                        df_job_raw,
                        df_result_raw,
                        df_defect_raw,
                        df_stock_raw,
                    )

                aggs = st.session_state["aggregates"]

                # 집계 사용해서 환입 예상재고 계산
                df_full = recalc_return_expectation(df_return, aggs)
                st.session_state["환입재고예상"] = df_full

                # ===== ERP재고 직접 매칭 패치 =====
                stock_part_col = pick_col(df_stock_raw, "D", ["품번"])
                stock_qty_col  = pick_col(df_stock_raw, "N", ["실재고수량"])

                if stock_part_col and stock_qty_col:
                    stock_map = dict(
                        zip(
                            df_stock_raw[stock_part_col].astype(str),
                            df_stock_raw[stock_qty_col].apply(safe_num)
                        )
                    )
                    df_full["ERP재고"] = df_full["품번"].astype(str).map(stock_map).fillna(0)
                else:
                    st.warning("재고 시트에서 품번(D) 또는 실재고수량(N) 컬럼을 찾을 수 없습니다.")

                st.success(
                    f"선택된 자재 {len(df_new)}개에 대해 환입 예상재고 데이터가 갱신되었습니다."
                )

    # ----- 환입 예상재고 초기화 -----
    if st.button("🧹 환입 예상재고 초기화", key="btn_clear_expect"):
        st.session_state["환입재고예상"] = pd.DataFrame(columns=CSV_COLS)
        df_full = st.session_state["환입재고예상"]
        st.success("환입 예상재고 데이터가 초기화되었습니다.")

    # ----- 환입 예상재고 데이터 표시 + CSV + PDF + 코멘트 -----
    st.markdown("### 환입 예상재고 데이터")

    df_full = st.session_state.get(
        "환입재고예상", pd.DataFrame(columns=CSV_COLS)
    )

    if df_full.empty:
        st.write("환입 데이터 불러오기를 실행하면 이곳에 결과가 표시됩니다.")
    else:
        # 화면용: 계산된 df_full 그대로 VISIBLE_COLS 기준으로 보여주기
        df_visible = df_full[[c for c in VISIBLE_COLS if c in df_full.columns]].copy()
        st.dataframe(df_visible, use_container_width=True)

         label_source_cols = ["품번", "품명", "단위수량", "환입일"]
        if all(col in df_full.columns for col in label_source_cols):
            st.markdown("#### 🏷 라벨 출력용 자재 선택")

            label_df = df_full[label_source_cols].copy()
            # 표시용: 선택 컬럼 맨 앞에 추가
            label_df.insert(0, "선택", False)

            label_df = st.data_editor(
                label_df,
                use_container_width=True,
                num_rows="dynamic",
                key="label_editor",
            )

            if st.button("🏷 선택한 자재 라벨 PDF 만들기", key="btn_make_labels"):
                selected_labels = label_df[label_df["선택"] == True].copy()

                if selected_labels.empty:
                    st.warning("라벨을 출력할 행을 하나 이상 선택하세요.")
                else:
                    try:
                        pdf_labels = generate_label_pdf(selected_labels)
                        st.download_button(
                            "📄 부자재반입 라벨 PDF 다운로드",
                            data=pdf_labels,
                            file_name="부자재_반입라벨.pdf",
                            mime="application/pdf",
                        )
                    except Exception as e:
                        st.error(f"라벨 PDF 생성 중 오류: {e}")       

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

                # 단위수량: 합치지 않고 대표값 only
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

        # CSV 받기 버튼
        csv_data = csv_export_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 CSV 받기",
            data=csv_data,
            file_name="환입_예상재고_통합.csv",
            mime="text/csv",
        )

        # PDF 받기 버튼 (최종 CSV용 데이터 기준)
        if REPORTLAB_AVAILABLE and not csv_export_df.empty:

            st.markdown("### 📎 PDF 상단에 들어갈 메모를 입력하거나 붙여넣기(Ctrl+V) 하세요")

            pasted_text = st.text_area(
                "PDF 메모",
                height=100,
                key="pdf_note_text",
                placeholder="여기에 메모나 특이사항을 입력/붙여넣기 하세요."
            )

            # 텍스트만 사용해서 PDF 생성 (이미지는 사용 안 함)
            pdf_bytes = generate_pdf(csv_export_df, pasted_text=pasted_text)

            st.download_button(
                "📄 PDF 받기",
                data=pdf_bytes,
                file_name="환입_예상재고.pdf",
                mime="application/pdf",
            )

        elif not REPORTLAB_AVAILABLE:
            st.info("PDF 저장 기능을 쓰려면 `pip install reportlab` 설치가 필요합니다.")

        # ---------- 입고 시트 비고(구 비고2) 코멘트 ----------
        in_suju_col = pick_col(df_in_raw, "B", ["수주번호"])
        in_jisi_col = pick_col(df_in_raw, "C", ["지시번호"])
        in_part_col = pick_col(df_in_raw, "M", ["품번"])
        # 이름을 "비고"로 바꿨으므로 우선 "비고"를 찾고, 없으면 V열/비고2도 허용
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
                    st.markdown("#### 입고 비고 코멘트")
                    for _, row in df_comment_show.iterrows():
                        st.markdown(
                            f"- **{row['품번']} / {row['품명']}** : {row['비고2']}"
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
