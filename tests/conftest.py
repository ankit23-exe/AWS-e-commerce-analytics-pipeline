import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "glue"))
sys.path.insert(0, str(ROOT / "src" / "lambda"))


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]").appName("olist-tests")
        .config("spark.sql.shuffle.partitions", "4").config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def sample_raw():
    return str(ROOT / "data" / "sample" / "raw")
