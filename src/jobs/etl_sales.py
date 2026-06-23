import io
import json
import sys
from urllib.parse import urlparse

import boto3
import pandas as pd

try:
    from awsglue.utils import getResolvedOptions
except ImportError:  # pragma: no cover
    import argparse

    def getResolvedOptions(argv, options):  # NOSONAR
        parser = argparse.ArgumentParser()
        for opt in options:
            parser.add_argument(f"--{opt}", required=False)
        args, _ = parser.parse_known_args(argv[1:])
        return {opt: getattr(args, opt) for opt in options if getattr(args, opt) is not None}


def run_job():
    args = getResolvedOptions(sys.argv, ["CONFIG_PATH"])
    config_path = args["CONFIG_PATH"]

    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)  # NOSONAR
    config = json.loads(response["Body"].read().decode("utf-8"))

    output_bucket = config["OUTPUT_BUCKET_NAME"]

    df = pd.DataFrame({"number": range(1, 21)})
    buf = io.StringIO()
    df.to_csv(buf, index=False)

    s3.put_object(  # NOSONAR
        Bucket=output_bucket,
        Key="output/sales_data.csv",
        Body=buf.getvalue().encode("utf-8"),
    )
    print(f"Data written to s3://{output_bucket}/output/sales_data.csv")


if __name__ == "__main__":  # pragma: no cover
    run_job()
