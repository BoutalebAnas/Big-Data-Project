import pytest

from src.imdb_loader import spark_session


@pytest.fixture(scope="session")
def spark():
    with pytest.MonkeyPatch.context() as env:
        env.setenv("IMDB_SPARK_THREADS", "2")
        env.setenv("IMDB_SPARK_PARTITIONS", "2")
        env.setenv("IMDB_SPARK_MEMORY", "1g")
        session = spark_session("IMDb tests")
        yield session
        session.stop()
