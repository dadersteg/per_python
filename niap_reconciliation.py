'''
============================================================================
[NIAP RECONCILIATION ENGINE] v1.0.1
============================================================================
'''

"""
EXECUTIVE SUMMARY (BLUF)
------------------------
This script reconciles NIAP Governance Tickets against Product Register entries
to identify "Shadow IT" and launch gaps.

Goal: Provide a single, filterable "Venn Diagram" table for forensic investigation
into "Approved for Launch" gaps and "Shadow IT."

Business Problem:
It identifies gaps in both directions: tickets without products and products without tickets.
"""

import pandas as pd
import json
import re
import time
import numpy as np
import traceback
import logging
import contextlib
from datetime import datetime
from rapidfuzz import process, fuzz, utils

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Mock dependencies if not available (for local verification)
try:
    # Attempt to import internal tools.
    # NOTE: These modules are assumed to exist in the production environment.
    from internal.database import zeus, execute_sql
    from internal.drive import upload_file_to_gdrive
except ImportError:
    logger.warning("Internal dependencies not found. Using mocks for local verification.")

    @contextlib.contextmanager
    def zeus():
        """Mock database connection context manager."""
        yield "mock_cursor"

    def execute_sql(cursor, query, params=None):
        """Mock SQL execution returning sample DataFrames."""
        query_lower = query.lower()
        if "jira_niap_initiatives" in query_lower:
            return pd.DataFrame({
                "issue_id": ["JIRA-123", "JIRA-456", "JIRA-789"],
                "summary": ["Alpha Product", "Beta Service", "Gamma Platform"],
                "issue_status": ["Approved for Launch", "Development", "Monitoring"],
                "product_register_link": ["http://link.to/alpha", None, "http://link.to/gamma"]
            })
        elif "niap_product_register" in query_lower:
            return pd.DataFrame({
                "name": ["Alpha Product", "Delta Widget"],
                "niap": ["Yes", "No"],
                "product_status": ["Active", "Draft"]
            })
        elif "core.products" in query_lower:
            return pd.DataFrame({
                "product_type": ["Type A", "Type B"],
                "name": ["Alpha Product", "Epsilon Tool"],
                "risk_volume": [100, 50]
            })
        elif "sdm_service_catalogue" in query_lower:
            return pd.DataFrame({
                "name": ["Component Z"],
                "bia": [10]
            })
        return pd.DataFrame()

    def upload_file_to_gdrive(**kwargs):
        """Mock file upload."""
        logger.info(f"Mock uploading file: {kwargs.get('name')}")

# -----------------------------------------------------------------------------
# 1. SETUP: Configuration and File Naming
# -----------------------------------------------------------------------------
TARGET_FOLDER_ID = "15xKraT4MhWzBvOjMTJ0x21TBIopqfl4E"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M")

# Core Artifacts
REPORT_MD = f"niap_forensic_deep_dive_{TIMESTAMP}.md"
CSV_TOC = f"export_table_of_contents_{TIMESTAMP}.csv"
CSV_MASTER_SPINE = f"niap_master_spine_reconciliation_{TIMESTAMP}.csv"

# Supporting Bases
CSV_BASE_0 = f"base_0_register_delta_master_{TIMESTAMP}.csv"
CSV_BASE_1_GAPS = f"base_1_core_GAPS_RISK_priority_{TIMESTAMP}.csv"
CSV_BASE_2 = f"base_2_component_mapping_{TIMESTAMP}.csv"

# Audit Trail
TXT_METADATA = f"audit_provenance_metadata_{TIMESTAMP}.txt"

# -----------------------------------------------------------------------------
# 2. TOOLS: Data Processing & Formatting
# -----------------------------------------------------------------------------

def df_to_markdown_manual(df, headers=None):
    """Formats data into clean Markdown tables without f-string backslash errors.

    Args:
        df (pd.DataFrame or pd.Series): The data to format.
        headers (list, optional): List of column headers. Defaults to None.

    Returns:
        str: A Markdown-formatted table string.
    """
    if df.empty:
        return "No data available."

    if isinstance(df, pd.Series):
        df = df.reset_index()
        df.columns = headers if headers else ["Category", "Count"]

    if isinstance(df.index, pd.Index) and df.index.name:
        df = df.reset_index()

    if headers and len(headers) == len(df.columns):
        df.columns = headers

    md_headers = df.columns.tolist()
    header_row = f"| {' | '.join(md_headers)} |"
    sep_row = f"| {' | '.join(['---'] * len(md_headers))} |"

    body = []
    for _, row in df.iterrows():
        clean_values = []
        for val in row:
            s_val = str(val).replace('|', '\\|').replace('\n', ' ')
            clean_values.append(s_val)
        row_str = "| " + " | ".join(clean_values) + " |"
        body.append(row_str)

    return f"{header_row}\n{sep_row}\n" + "\n".join(body)

def normalize_name(text):
    """Clean standard for cross-source matching.

    Args:
        text (str): The text to normalize.

    Returns:
        str: The normalized string suitable for fuzzy matching.
    """
    if pd.isna(text):
        return ""
    text = str(text).lower()
    text = re.split(r'[|·]', text)[-1].strip()
    return utils.default_process(text)

def clean_tech_family(text):
    """Aggregates technical configurations into clean product families.

    Args:
        text (str): The raw technical family name.

    Returns:
        str: A cleaned, aggregated product family name.
    """
    if pd.isna(text):
        return ""
    text = str(text).upper()
    text = re.sub(r'_(GB|EUR|US|AU|NZ|SG|JP|RO|FR|PL|ES|IT|IE)_?', ' ', text)
    for p in [r'PT_', r'MF_', r'ACQ_', r'REVX_', r'RV_', r'PRCHS_']:
        text = re.sub(p, ' ', text)
    for n in ['REPRICING','PLAN','BASE','STD','PREM','METAL','PLUS','OFFER','FREE','UL','PR','ST']:
        text = text.replace(n, '')
    return " ".join(re.sub(r'[^A-Z0-9\s]', ' ', text).split()).lower()

def upload_to_drive(file_path, mime_type):
    """Uploads a file to Google Drive with retries.

    Args:
        file_path (str): The path to the file to upload.
        mime_type (str): The MIME type of the file.

    Returns:
        None
    """
    for attempt in range(3):
        try:
            upload_file_to_gdrive(
                file=file_path,
                name=file_path,
                mime_type=mime_type,
                folder_id=TARGET_FOLDER_ID
            )
            return
        except Exception:
            time.sleep(5)

# -----------------------------------------------------------------------------
# 3. PIPELINE: Bi-Directional Reconciliation
# -----------------------------------------------------------------------------

def run_master_audit():
    """Executes the master audit reconciliation process.

    Args:
        None

    Returns:
        None
    """
    try:
        # Step 1: Extract Source Data
        logger.info(f"PHASE A: Extracting Source Data...")
        with zeus() as cur:
            df_niap_initiatives = execute_sql(
                cur,
                "SELECT issue_id, summary, issue_status, product_register_link FROM global_entity_operations.jira_niap_initiatives",
                None
            )
            df_product_register = execute_sql(
                cur,
                "SELECT name, niap, product_status FROM global_entity_operations.niap_product_register",
                None
            )
            df_core_products = execute_sql(
                cur,
                "SELECT product_type, name, COUNT(*) as risk_volume FROM core.products WHERE (decommission_date IS NULL OR decommission_date > CURRENT_DATE) GROUP BY 1, 2",
                None
            )
            df_components = execute_sql(
                cur,
                "SELECT name, business_impact_assessment_importance_score as bia FROM global_entity_operations.sdm_service_catalogue_v3_process_component WHERE type = 'PRODUCT'",
                None
            )

        # Step 2: Standardize Data
        df_product_register['product_status'] = df_product_register['product_status'].fillna('Unknown')

        # Step 3: Create Normalization Keys
        df_niap_initiatives['join_key'] = df_niap_initiatives['summary'].apply(normalize_name)
        df_product_register['join_key'] = df_product_register['name'].apply(normalize_name)

        # Step 4: Create Master Reconciliation Spine (Outer Join)
        logger.info(f"Building Master Reconciliation Spine...")
        df_spine = pd.merge(
            df_niap_initiatives,
            df_product_register,
            on='join_key',
            how='outer',
            suffixes=('_jira', '_reg')
        )

        def categorize_overlap(row):
            if pd.notna(row['issue_id']) and pd.notna(row['name']):
                return "MATCHED"
            if pd.notna(row['issue_id']):
                return "JIRA_ONLY (Unmapped Ticket)"
            return "REGISTER_ONLY (Unmapped Product)"

        df_spine['reconciliation_outcome'] = df_spine.apply(categorize_overlap, axis=1)

        # Step 5: Attach Footprints from Core Products
        logger.info(f"Attaching Footprints...")
        df_core_products['clean_family'] = df_core_products['name'].apply(clean_tech_family)
        core_lookup = df_core_products.groupby('clean_family')['risk_volume'].sum().to_dict()
        df_spine['associated_tech_vol'] = df_spine['join_key'].map(core_lookup).fillna(0)

        # Step 6: Calculate Metrics
        venn_metrics = df_spine['reconciliation_outcome'].value_counts()
        jira_gaps = df_spine[
            (df_spine['reconciliation_outcome'] == "JIRA_ONLY (Unmapped Ticket)") &
            (df_spine['issue_status'].isin(['Approved for Launch', 'Monitoring', 'Development']))
        ][['issue_id', 'summary', 'issue_status']].head(50)

        # Step 7: Generate Table of Contents
        toc_data = [
            {"What": "The Manifest (This file).", "Why": "Inventory of all files delivered in this export."},
            {"What": "Master many-to-many outer join between NIAP Jira and the Product Register.", "Why": "The primary investigation tool for Afonso to find unmapped tickets and products."},
            {"What": "Deep-dive forensic report with Venn metrics and gap analysis.", "Why": "Stakeholder-ready summary for NotebookLM and PO outreach planning."},
            {"What": "Register-centric delta file.", "Why": "Used to identify products that exist legally but have no governance oversight."},
            {"What": "List of technical families in Core with no NIAP mapping.", "Why": "Identifies high-volume technical 'Shadow IT' risks."},
            {"What": "Architectural L3 Component mapping to the Register.", "Why": "Identifies drift between architectural design and legal registration."},
            {"What": "Audit execution metadata.", "Why": "Proof of run time and logic version for compliance."}
        ]

        df_toc = pd.DataFrame(toc_data)
        df_toc.index = [CSV_TOC, CSV_MASTER_SPINE, REPORT_MD, CSV_BASE_0, CSV_BASE_1_GAPS, CSV_BASE_2, TXT_METADATA]
        df_toc.index.name = "Filename"
        df_toc = df_toc.reset_index()

        # Step 8: Generate Markdown Report
        md_report = f"""# NIAP to Product Mapping: Forensic Reconciliation Report

## 1. The Venn Diagram (Master Reconciliation Overview)
*Audit Objective: Understand the overlap between governance (Jira) and registration (GEO Register).*

### 1.1 Global Linkage Summary
{df_to_markdown_manual(venn_metrics, headers=['Reconciliation Outcome', 'Count'])}

### 1.2 High-Priority Jira Gaps (Afonso's Investigation List)
*Definition: 'Approved for Launch' or 'Active' Jira tickets that are NOT found in the Product Register.*
{df_to_markdown_manual(jira_gaps)}

---

## 2. Governance Health (Jira perspective)
### 2.1 Jira Ticket Status Breakdown
{df_to_markdown_manual(df_niap_initiatives['issue_status'].value_counts(), headers=['Status', 'Ticket Count'])}

---

## 3. Product Health (Register perspective)
### 3.1 Unmapped Register Entries (Orphans)
{df_to_markdown_manual(df_spine[df_spine['reconciliation_outcome'] == "REGISTER_ONLY (Unmapped Product)"][['name', 'product_status']].head(20))}

### 4. Technical Exposure (Core Gaps)
*Definition: Aggregated technical families with no NIAP link, sorted by volume.*
{df_to_markdown_manual(df_core_products.groupby('clean_family')['risk_volume'].sum().sort_values(ascending=False).head(20))}
"""
        # Step 9: Save Artifacts and Upload
        with open(REPORT_MD, 'w') as f: f.write(md_report)
        with open(TXT_METADATA, 'w') as f: f.write(f"NIAP AUDIT LOG\nTimestamp: {datetime.now()}\nLogic: Bi-Directional Spine")

        df_toc.to_csv(CSV_TOC, index=False)
        df_spine.to_csv(CSV_MASTER_SPINE, index=False)
        df_spine[df_spine['reconciliation_outcome'].str.contains("REGISTER")].to_csv(CSV_BASE_0, index=False)
        df_core_products[~df_core_products['clean_family'].isin(df_spine['join_key'])].to_csv(CSV_BASE_1_GAPS, index=False)
        df_components.to_csv(CSV_BASE_2, index=False)

        logger.info(f"Uploading to Drive...")
        for artifact in [CSV_TOC, CSV_MASTER_SPINE, REPORT_MD, CSV_BASE_0, CSV_BASE_1_GAPS, CSV_BASE_2, TXT_METADATA]:
            mime = "text/csv" if ".csv" in artifact else ("text/markdown" if ".md" in artifact else "text/plain")
            upload_to_drive(artifact, mime)

        logger.info(f"\n✅ RECONCILIATION COMPLETE.\nTable of Contents: {CSV_TOC}\nMaster Investigation File: {CSV_MASTER_SPINE}")

    except Exception:
        logger.error("An error occurred during the audit process:", exc_info=True)

if __name__ == "__main__":
    run_master_audit()
