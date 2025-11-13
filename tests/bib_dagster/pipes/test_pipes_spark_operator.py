from bib_dagster.pipes.pipes_spark_operator import PipesSparkApplicationClient


def test_file_path_to_module():
    assert (
        PipesSparkApplicationClient._file_path_to_module(
            "local:///app/src/transform/raw_tss/main.py"
        )
        == "src.transform.raw_tss.main"
    )
    assert (
        PipesSparkApplicationClient._file_path_to_module(
            "local:///app/src/transform/main.py"
        )
        == "src.transform.main"
    )
