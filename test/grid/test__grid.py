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

    # def test__basic_table(self):
    #     """Method tests the creation of a simple table"""
    #
    #     grid = Grid.from_yaml(
    #         path="../data/sample/basic.grid.yaml",
    #         dataset=self.DATASET
    #     )
    #
    #     exist = grid.exist()
    #
    #     if exist is True:
    #
    #         grid.drop()
    #
    #     grid.create()
    #     assert grid.exist() is True
    #
    #     grid.load_jsonl(
    #         blob_uri="gs://sample-asset-data/BQ/grid/basic.data.jsonl"
    #     )
    #
    #     assert grid.table.num_rows == 3
    #
    #     grid.truncate()
    #
    #     assert grid.table.num_rows == 0
    #
    #     grid.drop()
    #
    #     assert grid.exist() is False

    def test__get_markdown_schema_table(self):
        """Method tests the creation of a simple table"""

        grid = Grid.from_yaml(
            path="../data/sample/basic.grid.yaml",
            dataset=self.DATASET
        )

        markdown = grid.get_markdown_table(
            schema=grid.config.get("schema")
        )
        table_arch = markdown.strip("\n").split("\n")
        assert table_arch[0] == "| name | type | mode | desc | fields | defaultValueExpression |"
        assert table_arch[1] == "| --- | --- | --- | --- | --- | --- |"
        assert table_arch[2] == "| id | INTEGER | required | ID of the user |  |  |"
        assert table_arch[3] == "| name | STRING | required | First name and last name of the user |  |  |"
        assert table_arch[4] == "| department | STRING | NULLABLE | Description of the user |  |  |"
        assert table_arch[5] == "| height | FLOAT | NULLABLE | Height of the user in centimeters |  |  |"