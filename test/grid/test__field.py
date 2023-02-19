import pytest

from google.cloud import bigquery
from surquest.GCP.bq.grid import Field


class TestField:

    FIELDS = {
        "id": {
            "name": "id",
            "type": "INTEGER",
            "mode": "REQUIRED"
        },
        "name": {
            "name": "name"
        },
        "roles": {
            "name": "roles",
            "type": "RECORD",
            "mode": "REPEATED",
            "fields": [
                {
                    "name": "name",
                    "mode": "REQUIRED",
                },
                {
                    "name": "is_active",
                    "type": "BOOLEAN"
                }
            ]
        }
    }

    @pytest.mark.parametrize(
        "spec,field",
        [
            (
                FIELDS.get("id"),
                bigquery.SchemaField(
                    name="id",
                    field_type="INTEGER",
                    mode="REQUIRED")
            ),
            (
                    FIELDS.get("name"),
                    bigquery.SchemaField(
                        name="name",
                        field_type="STRING")
            ),
            (
                    FIELDS.get("roles"),
                    bigquery.SchemaField(
                        name="roles",
                        field_type="RECORD",
                        mode="REPEATED",
                    )
            )
        ]
    )
    def test__adjust_dict(self, spec, field):

        assert Field.from_dict(spec).name == field.name
        assert Field.from_dict(spec).field_type == field.field_type
        assert Field.from_dict(spec).mode == field.mode
        assert Field.from_dict(spec).description == field.description

    def test__adjust_dict__missing_required_attribute(self):

        spec = self.FIELDS.get("id")
        del spec["name"]

        try:
            Field.adjust_dict(spec)

        except KeyError as e:
            assert True is isinstance(e, KeyError)