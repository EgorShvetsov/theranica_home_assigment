# Doctors & Clinicians ETL Pipeline

This project extracts, transforms, and loads clinician data from the CMS "Doctors and Clinicians – National Downloadable File" API into Google BigQuery.

## Steps

1. **Extract**  
   - Pull data from CMS API using the SQL method, which allows filtering by a single field.  
   - In this project, the filter used at the API level is **state**.  
   - Data for the selected specialties (configured in the script) is filtered **after** fetching from the API.  
   - Handles pagination to fetch all records.  

2. **Transform**  
   - Clean and standardize column names to `snake_case`.  
   - Convert numeric fields (`graduation_year`, `number_of_group_members`).  
   - Remove duplicates and add a `bq_load_dttm` timestamp.  
   - Split data into two tables: `doctors` and `specialty_and_locations`.  

3. **Load**  
   - Load cleaned tables into BigQuery.  
   - `doctors` table: professional identification info.  
   - `specialty_and_locations` table: practice locations, specialties, and group info.  

## Requirements

- Python 3  
- `pandas`, `requests`, `google-cloud-bigquery`  

## Usage

```bash
python etl.py