from fastapi import FastAPI, UploadFile, Form, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse
import pandas as pd
import io
import re
from rapidfuzz import process, fuzz


templates = Jinja2Templates(directory="templates")
app = FastAPI(
    title="Data Filtering",
    description="Testing Data Filtering",
    version="1.1"
)

@app.get("/", response_class=HTMLResponse)
def serve_frontend(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {"request": request}
    )


def normalize_value(value):
    if pd.isna(value):
        return ""
    value = str(value).upper()
    return re.sub(r"[^A-Z0-9]", "", value)

def build_composite_key(df, col_a, col_b):
    return (
        df[col_a].apply(normalize_value) + "|" + df[col_b].apply(normalize_value)
    )


def reconcile_data(df1, df2, column, mode="exact", threshold=80):
    df1 = df1.copy()
    df2 = df2.copy()

    if column == "Company+NPP":
        df1["_key"] = build_composite_key(df1, "Company", "NPP")
        df2["_key"] = build_composite_key(df2, "Company", "NPP")
    else:
        df1["_key"] = df1[column].apply(normalize_value)
        df2["_key"] = df2[column].apply(normalize_value)

    if mode == "exact":
        if column == "Company":
            occ_col = "_occ_idx"
            df1[occ_col] = df1.groupby("_key").cumcount()
            df2[occ_col] = df2.groupby("_key").cumcount()

            merged = df1.merge(
                df2,
                on=["_key", occ_col],
                how="outer",
                indicator=True,
                suffixes=("_sheet1", "_sheet2")
            ).drop(columns=[occ_col], errors="ignore")
        else:
            merged = df1.merge(
                df2,
                on="_key",
                how="outer",
                indicator=True,
                suffixes=("_sheet1", "_sheet2")
            )

    elif mode == "fuzzy":
        choices = df2["_key"].tolist()

        def fuzzy_match(val):
            if not val:
                return None
            match = process.extractOne(
                val, choices, scorer=fuzz.token_sort_ratio
            )
            if match and match[1] >= threshold:
                return match[0]
            return None

        df1["_matched_key"] = df1["_key"].apply(fuzzy_match)

        merged = df1.merge(
            df2,
            left_on="_matched_key",
            right_on="_key",
            how="outer",
            indicator=True,
            suffixes=("_sheet1", "_sheet2")
        )
    else:
        raise ValueError("Mode harus 'exact' atau 'fuzzy'")

    data_cocok = merged[merged["_merge"] == "both"]
    anomali_sheet1 = merged[merged["_merge"] == "left_only"]
    anomali_sheet2 = merged[merged["_merge"] == "right_only"]

    return data_cocok, anomali_sheet1, anomali_sheet2

def write_large_df(writer, df, sheet_base_name):
    max_rows = 1_000_000
    for i in range(0, len(df), max_rows):
        chunk = df.iloc[i:i + max_rows]
        sheet_name = f"{sheet_base_name}_{i//max_rows + 1}"
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)

def get_filter_columns(df1, df2):
    common_columns = [col for col in df1.columns if col in df2.columns]
    columns = [str(col) for col in common_columns]

    required = {"Company", "NPP"}
    if required.issubset(df1.columns) and required.issubset(df2.columns):
        columns.append("Company+NPP")

    return columns

def build_summary(df1, df2, cocok, anomali_s1, anomali_s2):
    total_sheet1 = len(df1)
    total_sheet2 = len(df2)
    total_cocok = len(cocok)
    total_anomali_s1 = len(anomali_s1)
    total_anomali_s2 = len(anomali_s2)
    denominator = max(total_sheet1, total_sheet2)
    matching_rate = (total_cocok / denominator * 100) if denominator else 0

    return pd.DataFrame(
        [
            {"Metric": "Total data Sheet1", "Value": total_sheet1},
            {"Metric": "Total data Sheet2", "Value": total_sheet2},
            {"Metric": "Total data_cocok", "Value": total_cocok},
            {"Metric": "Total anomali_sheet1", "Value": total_anomali_s1},
            {"Metric": "Total anomali_sheet2", "Value": total_anomali_s2},
            {"Metric": "Matching rate (%)", "Value": round(matching_rate, 2)},
        ]
    )


@app.post("/detect-columns")
async def detect_columns(
    file: UploadFile,
    sheet1: str = Form(...),
    sheet2: str = Form(...)
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File harus .xlsx")

    try:
        contents = await file.read()
        excel = pd.ExcelFile(io.BytesIO(contents))
    except Exception:
        raise HTTPException(status_code=400, detail="Gagal membaca file Excel")

    if sheet1 not in excel.sheet_names or sheet2 not in excel.sheet_names:
        raise HTTPException(status_code=400, detail="Nama sheet tidak ditemukan")

    df1 = excel.parse(sheet1)
    df2 = excel.parse(sheet2)
    columns = get_filter_columns(df1, df2)

    return JSONResponse(content={"columns": columns})



@app.post("/process-download")
async def process_and_download(
    file: UploadFile,
    sheet1: str = Form(...),
    sheet2: str = Form(...),
    column: str = Form(...),
    mode: str = Form("exact")
):
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="File harus .xlsx")

    try:
        contents = await file.read()
        excel = pd.ExcelFile(io.BytesIO(contents))
    except Exception:
        raise HTTPException(status_code=400, detail="Gagal membaca file Excel")

    if sheet1 not in excel.sheet_names or sheet2 not in excel.sheet_names:
        raise HTTPException(status_code=400, detail="Nama sheet tidak ditemukan")

    df1 = excel.parse(sheet1)
    df2 = excel.parse(sheet2)

    if column == "Company+NPP":
        required = {"Company", "NPP"}
        if not required.issubset(df1.columns) or not required.issubset(df2.columns):
            raise HTTPException(status_code=400, detail="Kolom Company/NPP tidak ditemukan")
    else:
        if column not in df1.columns or column not in df2.columns:
            raise HTTPException(status_code=400, detail="Kolom tidak ditemukan")

    cocok, anomali_s1, anomali_s2 = reconcile_data(
        df1, df2, column, mode
    )

    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        summary = build_summary(df1, df2, cocok, anomali_s1, anomali_s2)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        write_large_df(writer, cocok, "Data_Cocok")
        anomali_s1.to_excel(writer, sheet_name="Anomali_Sheet1", index=False)
        anomali_s2.to_excel(writer, sheet_name="Anomali_Sheet2", index=False)

    output.seek(0)

    safe_column = column.lower().replace("+", "_")
    filename = f"hasil_filtering_{safe_column}_{mode}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
