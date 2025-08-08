import os
import pandas as pd
from datetime import datetime
from MAIN import (
    format_sheet_as_table,
    export_to_excel_simple,
    prompt_date,
    clean__df,
    run_query_and_return,
)

def run_main_template_query(output_dir=None, sheet_name="Dental Bookings"):
    """
    Prompts for a Mobile Dental event date, queries bookings for that exact date,
    cleans the result, and exports to an Excel file. Returns the cleaned DataFrame.
    """
    # --- Get date from user ---
    event_dt = prompt_date("Enter the Date of the Mobile Dental Event")   # should return a datetime.date/datetime
    event_str_sql = event_dt.strftime("%Y%m%d")
    event_str_file = event_dt.strftime("%Y-%m-%d")

    # --- SQL (SQL Server) ---
    sql_query_dental = f"""
                SELECT 
            x.description AS [Provider Name],
            m.event AS [Appointment Name], 
            l.location_name AS [Location Name], 
            z.appt_date AS [Appointment Date], 
            z.begintime, 
            z.appt_kept_ind AS [Kept Status?],
            z.description AS [Full Patient Name],
            cast(pp.med_rec_nbr as [int]) as [MRN], 
            cast(q.date_of_birth as [Date]) [DOB],
            q.cell_phone as [Phone Number], 
            q.email_address as [Email],    
            q.language as [Language], 
            q.sex as [Sex at Birth], 
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
            z.appt_date = '{lower_str_sql}' AND 
            l.location_name LIKE '%Dental%' AND 
            z.cancel_ind = 'N' 
        ORDER BY z.appt_date ASC;
    """

    # --- Run ---
    df_raw = run_query_and_return(sql_query_dental)
    if df_raw is None or df_raw.empty:
        print(f"⚠️ No dental bookings found for {event_str_file}.")
        return df_raw

    # --- Clean / normalize ---
    df = clean__df(df_raw).copy()

    # enforce date types (safe even if already correct)
    for col in ["Appointment Date", "DOB"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # tidy strings
    for col in ["Phone Number", "Email"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace({"None": "", "nan": ""})

    # sort
    sort_cols = [c for c in ["Appointment Date", "Begin Time", "Location Name", "Provider Name", "Full Patient Name"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    # --- Export ---
    if output_dir is None:
        output_dir = os.path.join(os.getcwd(), "Reports", "Dental")
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(output_dir, f"Mobile_Dental_Bookings_{event_str_file}.xlsx")

    # If your export helper expects a dict of {sheet_name: df}, use this:
    try:
        export_to_excel_simple({sheet_name: df}, out_path)
    except TypeError:
        # Fallback in case your helper signature is (df, path, sheet_name=...)
        export_to_excel_simple(df, out_path, sheet_name=sheet_name)

    print(f"✅ Retrieved & exported Dental bookings for {event_str_file} → {out_path}")
    return df
