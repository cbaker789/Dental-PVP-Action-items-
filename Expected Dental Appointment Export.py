import os
import re
import pandas as pd
from datetime import datetime, date
from MAIN import (
    format_sheet_as_table,
    export_to_excel_simple,
    clean__df,
    run_query_and_return,
    prompt_date,
)

# ---------- Name parsing helper ----------
def _split_full_name(full_name: str):
    """
    Split 'Last, First [Middle...]' or 'First [Middle...] Last'
    Returns (last, first, middle_or_None)
    """
    if not isinstance(full_name, str) or not full_name.strip():
        return (None, None, None)
    name = re.sub(r"\s+", " ", full_name.strip())
    if "," in name:
        # Format: Last, First Middle
        last, rest = [s.strip() for s in name.split(",", 1)]
        parts = rest.split(" ")
        first = parts[0] if parts else None
        middle = " ".join(parts[1:]) if len(parts) > 1 else None
        return (last or None, first or None, middle or None)
    # Fallback: First Middle Last
    parts = name.split(" ")
    if len(parts) == 1:
        return (parts[0], None, None)
    first = parts[0]
    last = parts[-1]
    middle = " ".join(parts[1:-1]) if len(parts) > 2 else None
    return (last or None, first or None, middle or None)

# ---------- Outreach CSV (single file, no location) ----------
def generate_outreach_file(
    df: pd.DataFrame,
    output_dir: str,
    campaign_name: str = "Mobile_Dental_Outreach",
    current_date_str: str | None = None,
    digits_only_phone: bool = False,
):
    if df is None or df.empty:
        print("⚠️ No data to create outreach file.")
        return None

    df = df.copy()

    # 🔹 Filter for Goleta Neighborhood Clinic
    if "Location Name" in df.columns:
        df = df[df["Location Name"].str.contains("Goleta Dental", case=False, na=False)]
        if df.empty:
            print("⚠️ No records found.")
            return None

    # Recode Language
    if "Language" in df.columns:
        df["Language"] = df["Language"].replace({"Spanish; Castilian": "Spanish"})

    # Split names from "Full Patient Name" if needed
    if "Full Patient Name" in df.columns:
        split_names = df["Full Patient Name"].apply(_split_full_name)
        df["Last Name"] = split_names.apply(lambda t: t[0])
        df["First Name"] = split_names.apply(lambda t: t[1])
        df["Middle Name"] = split_names.apply(lambda t: t[2])
    else:
        missing = [c for c in ["First Name", "Last Name"] if c not in df.columns]
        if missing:
            raise KeyError(
                f"Missing name columns: {missing} and no 'Full Patient Name' to derive them."
            )

    # Format DOB as YYYYMMDD
    if "DOB" in df.columns:
        dob_series = pd.to_datetime(df["DOB"], errors="coerce")
        df["dob_fmt"] = dob_series.dt.strftime("%Y%m%d")
    else:
        df["dob_fmt"] = None

    # Clean phone/email strings
    for col in ["Phone Number", "Email"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"None": "", "nan": ""})

    # Outreach frame
    cleaned = pd.DataFrame(
        {
            "personLastName": df.get("Last Name"),
            "personMidName": df.get("Middle Name"),
            "personFirstName": df.get("First Name"),
            "personCellPhone": df.get("Phone Number"),
            "personHomePhone": None,
            "personWorkPhone": None,
            "personPrefLanguage": df.get("Language"),
            "dob": df.get("dob_fmt"),
            "gender": df.get("Sex at Birth"),
            "personID": df.get("MRN"),
            "PersonEmail": df.get("Email"),
        }
    )

    # Remove duplicate personIDs
    if "personID" in cleaned.columns:
        cleaned = cleaned.drop_duplicates(subset=["personID"])

    # Optional: keep only digits in phone
    if digits_only_phone and "personCellPhone" in cleaned.columns:
        cleaned["personCellPhone"] = cleaned["personCellPhone"].astype(str).str.replace(
            r"\D+", "", regex=True
        )

    # Drop rows with missing/blank cell phone
    cleaned["personCellPhone"] = (
        cleaned["personCellPhone"].fillna("").astype(str).str.strip()
    )
    cleaned = cleaned.loc[cleaned["personCellPhone"] != ""].copy()

    # Output file path
    if current_date_str is None:
        current_date_str = date.today().strftime("%Y-%m-%d")

    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"{campaign_name}_{current_date_str}.csv")
    cleaned.to_csv(out_path, index=False)
    print(f"📤 Outreach file written: {out_path}")
    return out_path

# ---------- Main query function ----------
def run_main_template_query(
    output_dir: str | None = None,
    outreach_dir: str | None = None,
    sheet_name: str = "Dental Bookings",
):
    """
    Prompts for a Mobile Dental event date, queries bookings for that exact date,
    cleans the result, exports Excel to `output_dir`, and writes a single outreach
    CSV to `outreach_dir`.
    Returns the cleaned DataFrame.
    """
    # --- Get date from user ---
    event_dt = prompt_date("Enter the Date of the Mobile Dental Event")  # datetime.date
    event_str_sql = event_dt.strftime("%Y%m%d")     # For SQL format
    event_str_file = event_dt.strftime("%Y-%m-%d")  # For file naming

    # --- Default directories ---
    if output_dir is None:
        output_dir = r"C:\Reports\Dental Booking Analysis"
    if outreach_dir is None:
        outreach_dir = os.path.join(output_dir, "Outreach")  # separate folder

    # --- SQL Query ---
    sql_query_dental = f"""
        SELECT 
            x.description AS [Provider Name],
            m.event AS [Appointment Name], 
            l.location_name AS [Location Name], 
            z.appt_date AS [Appointment Date], 
            z.begintime, 
            z.appt_kept_ind AS [Kept Status?],
            z.description AS [Full Patient Name],
            CAST(pp.med_rec_nbr AS INT) AS [MRN], 
            CAST(q.date_of_birth AS DATE) AS [DOB],
            q.cell_phone AS [Phone Number], 
            q.email_address AS [Email],    
            q.language AS [Language], 
            q.sex AS [Sex at Birth], 
            z.workflow_status, 
            z.cancel_ind, 
            z.delete_ind
        FROM appointments z
        INNER JOIN location_mstr l ON l.location_id = z.location_id
        INNER JOIN provider_mstr x ON x.provider_id = z.rendering_provider_id
        INNER JOIN events m ON m.event_id = z.event_id
        INNER JOIN person q ON q.person_id = z.person_id
        FULL JOIN patient pp ON pp.person_id = q.person_id
        FULL JOIN patient_encounter pe ON pe.person_id = z.appt_id
        WHERE 
            z.appt_date = '{event_str_sql}' AND 
            l.location_name LIKE '%Dental%' AND 
            z.cancel_ind = 'N' 
        ORDER BY z.appt_date ASC;
    """

    # --- Run Query ---
    df_raw = run_query_and_return(sql_query_dental)
    if df_raw is None or df_raw.empty:
        print(f"⚠️ No dental bookings found for {event_str_file}.")
        return df_raw

    # --- Clean Data ---
    df = clean__df(df_raw).copy()

    # Convert date columns
    for col in ["Appointment Date", "DOB"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # Normalize/rename time column if present
    if "begintime" in df.columns and "Begin Time" not in df.columns:
        df.rename(columns={"begintime": "Begin Time"}, inplace=True)

    # Strip strings
    for col in ["Phone Number", "Email"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"None": "", "nan": ""})

    # Sort data (only using columns that exist)
    sort_cols = [
        c
        for c in ["Appointment Date", "Begin Time", "Location Name", "Provider Name", "Full Patient Name"]
        if c in df.columns
    ]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # --- Export Excel (to output_dir) ---
    os.makedirs(output_dir, exist_ok=True)
    out_path_xlsx = os.path.join(output_dir, f"Mobile_Dental_Bookings_{event_str_file}.xlsx")
    try:
        export_to_excel_simple({sheet_name: df}, out_path_xlsx)
    except TypeError:
        export_to_excel_simple(df, out_path_xlsx, sheet_name=sheet_name)

    print(f"✅ Retrieved & exported Dental bookings for {event_str_file} → {out_path_xlsx}")

    # --- Outreach CSV (to outreach_dir) ---
    try:
        out_csv = generate_outreach_file(
            df=df,
            output_dir=outreach_dir,              # <— separate folder
            campaign_name="Mobile_Dental_Event",
            current_date_str=event_str_file,
            digits_only_phone=False,              # set True if you want digits-only phone
        )
    except Exception as e:
        print(f"⚠️ Outreach file generation skipped due to error: {e}")
        out_csv = None

    return df

# ---------- Run ----------
if __name__ == "__main__":
    run_main_template_query()