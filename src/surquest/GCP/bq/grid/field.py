from google.cloud import bigquery


class Field:

    ATTRIBUTES = {
        "required": ["name"]
    }

    @classmethod
    def from_dict(cls, spec: dict):
        """Method creates instance of SchemaField class from dictionary.

        :param spec: dictionary with field specification
        :type spec: dict

        :return: instance of SchemaField class
        :rtype: SchemaField
        """

        spec = cls.adjust_dict(spec)

        return bigquery.SchemaField.from_api_repr(spec)
    @classmethod
    def adjust_dict(cls, spec: dict):
        """Method adjusts dictionary to be compatible with SchemaField class.

        :param spec: dictionary with field specification
        :type spec: dict

        :return: dictionary with adjusted field specification
        :rtype: dict
        """

        for attr in cls.ATTRIBUTES["required"]:
            if attr not in spec:
                raise ValueError(
                    f"Missing required attribute {attr} for field: `{spec['name']}`"
                )

        if "desc" in spec:
            spec["description"] = spec.get("desc")

        if "type" not in spec:
            spec["type"] = "STRING"

        if "fields" in spec:
            fields = []
            for field in spec["fields"]:
                fields.append(cls.adjust_dict(field))

            spec["fields"] = fields

        return spec
