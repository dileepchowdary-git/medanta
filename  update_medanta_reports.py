#!/usr/bin/env python3
"""
Prefect Flow: Update Medanta-only reports in Google Sheets
Runs every hour on VM using Prefect deployment.
"""

import os
import pandas as pd
from jinja2 import Template
from clickhouse_connect import get_client
from gspread_dataframe import set_with_dataframe
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from credentials import CLICKHOUSE_CONFIG
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta

# --- CONFIGURATION ---
SERVICE_ACCOUNT_FILE = "/Users/dileep/medanta/credentials.json"
SHEET_ID = "1nyi9r6D9Wjqo6SXmM6ZKcOB43-T4gEDc3qNualSn9go"
SQL_DIR = os.path.join(os.path.dirname(__file__), "sql")
MEDANTA_FILTER = "AND lower(c.client_name) LIKE '%medanta%'"


# --- AUTH FUNCTIONS ---
def init_gspread_client():
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scopes)
    return gspread.authorize(creds)


def open_sheet():
    client = init_gspread_client()
    return client.open_by_key(SHEET_ID)


# --- PREFECT TASKS ---
@task(retries=2, retry_delay_seconds=10, cache_policy=None)  # disable caching
def run_query_and_write(tab_name: str, sql_file: str):
    ch_client = get_client(**CLICKHOUSE_CONFIG)
    sheet = open_sheet()

    sql_path = os.path.join(SQL_DIR, sql_file)
    with open(sql_path) as f:
        sql_template = Template(f.read())

    sql = sql_template.render(medanta_filter=MEDANTA_FILTER)

    try:
        result = ch_client.query(sql)
        df = pd.DataFrame(data=result.result_rows, columns=result.column_names)
    except Exception as e:
        print(f"❌ Error executing query for {tab_name}: {e}")
        return

    try:
        worksheet = sheet.worksheet(tab_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sheet.add_worksheet(title=tab_name, rows="1000", cols="50")

    set_with_dataframe(worksheet, df)
    print(f"✅ Updated: {tab_name} ({len(df)} rows)")


@flow(name="Medanta-GSheet-Update")
def medanta_flow():
    print("🔁 Starting Medanta report update...")

    tasks = [
        ("medanta_branch_case_detail_base", "medanta_branch_case_detail_base.sql"),
        ("medanta_branch_eqc_reject_base", "medanta_branch_eqc_reject_base.sql"),
        ("medanta_branch_pending_cases", "medanta_branch_pending_cases.sql"),
        ("medanta_branch_bionic_inout", "medanta_branch_bionic_inout.sql"),
    ]

    for tab_name, sql_file in tasks:
        run_query_and_write(tab_name, sql_file)

    print("✅ All Medanta tabs processed.")


if __name__ == "__main__":
    medanta_flow()
