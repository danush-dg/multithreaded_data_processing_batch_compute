import boto3
import logging
import time

from db import fetch_data
from processor import parallel_process
from config import THREAD_COUNT, OUTPUT_FILE, S3_BUCKET, S3_KEY

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def upload_to_s3(file_name):

    s3 = boto3.client("s3")

    s3.upload_file(
        file_name,
        S3_BUCKET,
        S3_KEY
    )

    logger.info("File uploaded to S3")


def main():

    start_time = time.time()

    logger.info("Pipeline started")

    df = fetch_data()

    logger.info("Starting multithread processing")

    processed_df = parallel_process(df, THREAD_COUNT)

    processed_df.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Processed rows: {len(processed_df)}")

    upload_to_s3(OUTPUT_FILE)

    runtime = round(time.time() - start_time, 2)

    logger.info(f"Pipeline completed in {runtime} seconds")


if __name__ == "__main__":

    main()