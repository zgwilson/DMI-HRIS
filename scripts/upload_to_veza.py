#!/usr/bin/env python3

import base64
import os
import sys
import logging

from oaaclient.client import OAAClient, OAAClientError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def parse_ids_from_filename(csv_path):
    filename = os.path.basename(csv_path)
    if "_" not in filename or not filename.endswith(".csv"):
        log.error("❌ Invalid filename format. Expected format: <provider_id>_<data_source_id>.csv")
        sys.exit(6)

    name_part = filename[:-4]  # remove .csv
    parts = name_part.split("_", 1)
    if len(parts) != 2:
        log.error("❌ Could not split provider and data source ID from filename.")
        sys.exit(6)

    return parts[0], parts[1]

def upload_csv_to_veza(csv_path):
    veza_url = os.getenv("VEZA_URL")
    veza_api_key = os.getenv("VEZA_API_KEY")

    if not all([veza_url, veza_api_key]):
        log.error("❌ Missing VEZA_URL or VEZA_API_KEY in environment variables.")
        sys.exit(1)

    provider_id, data_source_id = parse_ids_from_filename(csv_path)

    log.info("Connecting to Veza")
    try:
        veza_con = OAAClient(veza_url, veza_api_key)
    except Exception as e:
        log.error("❌ Error connecting to Veza tenant")
        log.error(str(e))
        sys.exit(2)

    log.info(f"Reading CSV: {csv_path}")
    try:
        with open(csv_path, "rb") as f:
            encoded_csv = base64.b64encode(f.read())
    except Exception as e:
        log.error(f"❌ Failed to read CSV file: {e}")
        sys.exit(3)

    log.info(f"Pushing CSV to provider={provider_id}, datasource={data_source_id}")
    try:
        push_request = {
            "id": provider_id,
            "data_source_id": data_source_id,
            "csv_data": encoded_csv.decode()
        }
        veza_con.api_post(
            f"/api/v1/providers/custom/{provider_id}/datasources/{data_source_id}:push_csv",
            push_request
        )
        log.info("✅ Push succeeded")
    except Exception as e:
        log.error("❌ Failed to push data to Veza")
        log.error(str(e))
        sys.exit(4)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        log.error("Usage: python upload_to_veza.py <csv_file_path>")
        sys.exit(5)
    upload_csv_to_veza(sys.argv[1])
