import pytest

from surquest.GCP.bq.grid import Grid


class TestGrid:

    DATASET = "sample_data"

    def test__exist(self):

        grid = Grid(
            dataset=self.DATASET,
            name="users"
        )

        assert grid.exist() in [True, False]


    def test__invalid_yaml(self):

        with pytest.raises(Exception):

            Grid.from_yaml(
                path="../data/sample/invalid.grid.yaml",
                dataset=self.DATASET
            )

    def test__basic_table(self):
        """Method tests the creation of a simple table"""

        grid = Grid.from_yaml(
            path="../data/sample/basic.grid.yaml",
            dataset=self.DATASET
        )

        exist = grid.exist()

        if exist is True:

            grid.drop()

        grid.create()
        assert grid.exist() is True

        grid.load_jsonl(
            blob_uri="gs://sample-asset-data/BQ/grid/basic.data.jsonl"
        )

        assert grid.table.num_rows == 3

        grid.truncate()

        assert grid.table.num_rows == 0

        grid.drop()

        assert grid.exist() is False