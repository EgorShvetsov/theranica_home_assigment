import requests
import pandas as pd
from google.cloud import bigquery
import logging
import sys


# ============================================================
#                      LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# ============================================================
#                      CONFIG
# ============================================================
STATES_LIST = ["AL", "SD"]
SPECIALTY_FILTER = ["ORTHOPEDIC SURGERY", "DIAGNOSTIC RADIOLOGY"]
DATASET_ID = "d86e116d-ef83-54c5-a14f-9a7bf5a76eba"
CMS_URL = "https://data.cms.gov/provider-data/api/1/datastore/sql"
CHUNK_SIZE = 1000

SERVICE_ACCOUNT_FILE = "home-assignment-478617-7b741a2005e1.json"
BQ_PROJECT = "home-assignment-478617"
BQ_DATASET = "doctors_and_clinicians"


# ============================================================
#                    EXTRACT FUNCTION
# ============================================================
def extract_cms_data() -> list:
    """Fetch selected CMS dataset rows"""
    logger.info("Starting data extraction from CMS...")

    result = []

    for state in STATES_LIST:
        offset = 0
        logger.info(f"Fetching data for state: {state}")

        while True:
            query = f"""[SELECT * FROM {DATASET_ID}][WHERE state = "{state}"][LIMIT {CHUNK_SIZE} OFFSET {offset}]"""

            try:
                response = requests.get(CMS_URL, params={"query": query, "show_db_columns": "false"})
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                logger.error(f"Request failed for state={state}, offset={offset}: {e}")
                break

            if not data:
                break

            filtered = [x for x in data if x.get("pri_spec") in SPECIALTY_FILTER]
            result.extend(filtered)

            offset += CHUNK_SIZE
            if len(data) < CHUNK_SIZE:
                break

    logger.info(f"Extraction completed. Retrieved {len(result)} records.")
    return result


# ============================================================
#                    TRANSFORM FUNCTION
# ============================================================
def transform_data(raw_list: list) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rename fields, convert data types, split into 2 tables"""

    logger.info("Starting transformation...")

    df_raw = pd.DataFrame(raw_list)

    rename_map = {
        "npi": "npi",
        "ind_pac_id": "individual_pac_id",
        "ind_enrl_id": "individual_enrollment_id",
        "provider_last_name": "provider_last_name",
        "provider_first_name": "provider_first_name",
        "provider_middle_name": "provider_middle_name",
        "suff": "provider_suffix",
        "gndr": "gender",

        "cred": "credential",
        "med_sch": "medical_school_name",
        "grd_yr": "graduation_year",
        "pri_spec": "primary_specialty",
        "sec_spec_1": "secondary_specialty_1",
        "sec_spec_2": "secondary_specialty_2",
        "sec_spec_3": "secondary_specialty_3",
        "sec_spec_4": "secondary_specialty_4",
        "sec_spec_all": "secondary_specialties_all",

        "telehlth": "offers_telehealth",
        "facility_name": "facility_name",
        "org_pac_id": "organization_pac_id",
        "num_org_mem": "number_of_group_members",
        "adr_ln_1": "address_line_1",
        "adr_ln_2": "address_line_2",
        "ln_2_sprs": "line2_suppression_flag",
        "citytown": "city",
        "state": "state",
        "zip_code": "zip_code",
        "telephone_number": "telephone_number",
        "ind_assgn": "individual_accepts_medicare_assignment",
        "grp_assgn": "group_accepts_medicare_assignment",
        "adrs_id": "address_id",
        "record_number": "record_number",
        "bq_load_dttm": "bq_load_dttm",
    }

    df_raw = df_raw.rename(columns=rename_map).replace({"": None})

    # type conversions
    df_raw["graduation_year"] = pd.to_numeric(df_raw["graduation_year"], errors="coerce")
    df_raw["number_of_group_members"] = pd.to_numeric(df_raw["number_of_group_members"], errors="coerce")

    df_raw["bq_load_dttm"] = pd.Timestamp.utcnow()

    doctor_cols = [
        "npi",
        "individual_pac_id",
        "individual_enrollment_id",
        "provider_last_name",
        "provider_first_name",
        "provider_middle_name",
        "provider_suffix",
        "gender",
        "bq_load_dttm",
    ]

    df_doctors = df_raw[doctor_cols].drop_duplicates(subset=["npi"])
    df_specialty_and_locations = df_raw.drop(
        columns=[x for x in doctor_cols if x not in ["npi", "bq_load_dttm"]]
    )

    logger.info("Transformation completed.")
    return df_doctors, df_specialty_and_locations


# ============================================================
#                       LOAD FUNCTION
# ============================================================
def load_to_bigquery(df: pd.DataFrame, table_name: str):
    """Loads dataframe to BigQuery"""
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

    logger.info(f"Loading dataframe into BigQuery table: {table_id}")

    try:
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        logger.info(f"Loaded {job.output_rows} rows into {table_id}")

    except Exception as e:
        logger.error(f"Failed to load table {table_name}: {e}")
        raise


# ============================================================
#                       MAIN PIPELINE
# ============================================================
def main():
    raw = extract_cms_data()
    df_doctors, df_locations = transform_data(raw)

    load_to_bigquery(df_doctors, "doctors")
    load_to_bigquery(df_locations, "specialty_and_locations")

    logger.info("ETL pipeline completed successfully.")


# ============================================================
#                      ENTRY POINT
# ============================================================
if __name__ == "__main__":
    main()