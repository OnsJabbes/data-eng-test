import sys
from unittest.mock import patch, MagicMock

from src.jobs.etl_sales import run_job


@patch("boto3.client")
def test_run_job_logic(mock_boto):
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3

    config_content = '{"OUTPUT_BUCKET_NAME": "test-output-bucket"}'
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: config_content.encode("utf-8"))
    }

    test_args = ["job.py", "--CONFIG_PATH", "s3://fake-bucket/config.json"]
    with patch.object(sys, "argv", test_args):
        run_job()

    mock_s3.get_object.assert_called_with(Bucket="fake-bucket", Key="config.json")
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "test-output-bucket"
    assert call_kwargs["Key"] == "output/sales_data.csv"
