from fastapi import FastAPI, UploadFile, Form, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse, HTMLResponse
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



def reconcile_data(df1, df2, column, mode="exact", threshold=80):
    df1 = df1.copy()
    df2 = df2.copy()

    df1["_key"] = df1[column].apply(normalize_value)
    df2["_key"] = df2[column].apply(normalize_value)

    if mode == "exact":
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

    if column not in df1.columns or column not in df2.columns:
        raise HTTPException(status_code=400, detail="Kolom tidak ditemukan")

    cocok, anomali_s1, anomali_s2 = reconcile_data(
        df1, df2, column, mode
    )

    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        cocok.to_excel(writer, sheet_name="Data_Cocok", index=False)
        anomali_s1.to_excel(writer, sheet_name="Anomali_Sheet1", index=False)
        anomali_s2.to_excel(writer, sheet_name="Anomali_Sheet2", index=False)

    output.seek(0)

    filename = f"hasil_filtering_{column.lower()}_{mode}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )
