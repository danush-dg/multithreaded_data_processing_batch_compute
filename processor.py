from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import pandas as pd
import logging
from config import ERROR_FILE

logger = logging.getLogger(__name__)


def split_dataframe(df, chunks):

    return np.array_split(df, chunks)


def process_chunk(chunk):

    processed_rows = []
    errors = []

    logger.info(f"Processing chunk size {len(chunk)}")

    for _, row in chunk.iterrows():

        try:

            row["first_name"] = str(row.get("first_name")).title()
            row["last_name"] = str(row.get("last_name")).title()

            row["location"] = str(row.get("location")).upper()

            row["valid_salary"] = row["salary"] > 0

            if row["salary"] < 40000:
                row["salary_band"] = "Low"

            elif row["salary"] < 80000:
                row["salary_band"] = "Medium"

            else:
                row["salary_band"] = "High"

            row["join_date"] = pd.to_datetime(row["join_date"]).date()

            processed_rows.append(row)

        except Exception as e:

            emp_id = row.get("emp_id")

            logger.error(f"Error processing emp_id {emp_id}: {e}")

            errors.append((emp_id, str(e)))

    return pd.DataFrame(processed_rows), errors


def parallel_process(df, threads):

    chunks = split_dataframe(df, threads)

    results = []
    all_errors = []

    with ThreadPoolExecutor(max_workers=threads) as executor:

        futures = [
            executor.submit(process_chunk, chunk)
            for chunk in chunks
        ]

        for future in as_completed(futures):

            processed, errors = future.result()

            results.append(processed)

            all_errors.extend(errors)

    if all_errors:

        logger.warning(f"{len(all_errors)} rows failed processing")

        pd.DataFrame(
            all_errors,
            columns=["emp_id", "error"]
        ).to_csv(ERROR_FILE, index=False)

    return pd.concat(results, ignore_index=True)