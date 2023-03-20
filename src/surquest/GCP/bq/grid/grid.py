import yaml
from google.cloud import bigquery

# import internal modules
from .field import Field


class Grid:
    ATTRIBUTES = {
        "required": ("name", "schema")
    }

    def __init__(
            self,
            dataset,
            name=None,
            config=None,
            client=None
    ):

        self.name = name
        self.config = config
        self.dataset = dataset
        self._client = client

        if self.config is None:
            self.config = {}

        if self.name is None:
            self.name = config['name']

        if self._client is None:
            self._client = bigquery.Client()

        self.dataset_ref = bigquery.dataset.DatasetReference(
            project=self._client.project,
            dataset_id=self.dataset
        )

        self.table_ref = bigquery.table.TableReference(
            dataset_ref=self.dataset_ref,
            table_id=self.name
        )

    @property
    def table(self):

        return self._client.get_table(self.table_ref)

    @classmethod
    def from_dict(cls, config: dict, dataset, client=None):
        """Method creates instance of Grid class from dictionary.

        :param config: dictionary with grid specification
        :type config: dict
        :param dataset: BigQuery dataset
        :type dataset: str
        :param client: instance of BigQuery client
        :type client: google.cloud.bigquery.client.Client
        :return: instance of Grid class
        :rtype: Grid

        :return: instance of Grid class
        :rtype: Grid
        """

        for attr in cls.ATTRIBUTES['required']:

            if attr not in config:
                raise ValueError(f"{attr} is not specified in the YAML file.")

        return cls(
            name=config['name'],
            dataset=dataset,
            config=config,
            client=client
        )

    @classmethod
    def from_yaml(cls, path, dataset, client=None):
        """Create Grid instance from YAML file specification.

        :param path: path to YAML file
        :type path: str
        :param dataset: BigQuery dataset
        :type dataset: str
        :param client: instance of BigQuery client
        :type client: google.cloud.bigquery.client.Client
        :return: instance of Grid class
        :rtype: Grid
        """

        config = cls._load_yaml(path)

        return cls.from_dict(
            config=config,
            dataset=dataset,
            client=client
        )

    def drop(self, **kwargs):
        """Drop the BigQuery table from the database."""

        self._client.delete_table(
            table=self.table_ref,
            **kwargs
        )

    def create(self):
        """Create the BigQuery table in the dataset

        :return: instance of BigQuery table
        :rtype: google.cloud.bigquery.table.Table
        """

        table = self.set_table(config=self.config)
        return self._client.create_table(
            table=table
        )

    def exist(self):
        """Check if the BigQuery table exists in the dataset.

        :return: True if the table exists, False otherwise
        :rtype: bool
        """

        try:
            self._client.get_table(self.table_ref)
            return True
        except:
            return False

    def truncate(self):
        """Truncate the BigQuery table in the dataset.

        :return: instance of BigQuery job
        :rtype: google.cloud.bigquery.job.QueryJob
        """

        query = f"TRUNCATE TABLE {self.table_ref}"
        job_config = bigquery.QueryJobConfig(
            use_legacy_sql=False
        )
        query_job = self._client.query(
            query=query,
            job_config=job_config
        )

        query_job.result()


    def import_data(self, blob_uri, mode=bigquery.WriteDisposition.WRITE_APPEND):
        """Import data from blob to the BigQuery table.

        :param blob_uri: URI of the blob
        :type blob_uri: str
        :param mode: mode of the import
        :type mode: google.cloud.bigquery.job.WriteDisposition
        :return: instance of BigQuery job
        :rtype: google.cloud.bigquery.job.QueryJob
        """

        if blob_uri.endswith(".csv"):
            source_format=bigquery.SourceFormat.CSV
        elif blob_uri.endswith(".jsonl"):
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        else:
            raise ValueError("File format not supported.")

        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=mode
        )

        load_job = self._client.load_table_from_uri(
            source_uris=blob_uri,
            destination=self.table_ref,
            job_config=job_config
        )

        try:
            load_job.result()
        except BaseException:
            pass

        return load_job


    def set_table(self, config):
        """Returns instance of BigQuery table from specification.

        :param config: specification of the table in dictionary
        :type config: dict

        :return: BigQuery table
        :rtype: google.cloud.bigquery.table.Table
        """

        table = bigquery.Table(
            table_ref=self.table_ref,
            schema=self.get_schema(
                config=config['schema']
            )
        )

        if 'desc' in config:
            table.description = config['desc']

        if 'labels' in config:
            table.labels = config['labels']

        if 'clustering_fields' in config:
            table.clustering_fields = config['clustering_fields']

        if 'time_partitioning' in config:
            table.time_partitioning = bigquery.table.TimePartitioning.from_api_repr(
                config['time_partitioning']
            )

        if 'range_partitioning' in config:
            table.range_partitioning = bigquery.table.RangePartitioning(
                range_=bigquery.table.PartitionRange(
                    start=config['range_partitioning']['start'],
                    end=config['range_partitioning']['end'],
                    interval=config['range_partitioning']['interval']
                ),
                field=config['range_partitioning']['field']
            )

        return table

    @staticmethod
    def get_schema(config):
        """Returns BigQuery schema from specification.

        :param config: specification of the schema as list of dictionaries
        :type config: list

        :return: BigQuery schema
        :rtype: list
        """

        schema = []
        for field in config:
            schema.append(
                Field.from_dict(
                    spec=field
                )
            )

        return schema

    @staticmethod
    def _load_yaml(path):
        """Load YAML file and return it as dictionary.

        :param path: path to YAML file
        :type path: str
        :return: YAML file content as dictionary
        :rtype: dict
        """

        with open(path, 'r') as f:
            config = yaml.load(f, Loader=yaml.FullLoader)

        return config

    @staticmethod
    def get_markdown_table(
            schema,
            columns={
            "name": None,
            "type": "STRING",
            "mode": "NULLABLE",
            "desc": None,
            "fields": None,
            "defaultValueExpression": None,
        }
    ):
        """Returns Markdown table from BigQuery schema.

        :param schema: BigQuery schema
        :type schema: list
        :param columns: dictionary with column names as keys and default values as values
        :type columns: dict
        :return: Markdown table
        :rtype: str
        """

        values = []

        for field in schema:

            record = []
            for attr in columns.keys():
                val = field.get(attr)
                if isinstance(val, (int, bool, float, str)):
                    record.append(str(val))
                elif isinstance(val, type(None)):
                    if columns.get(attr) is None:
                        record.append("")
                    else:
                        record.append(columns.get(attr))
                else:
                    record.append(F"```{yaml.dump(val)}```")
            values.append(record)

        table_header = F"| {' | '.join(columns.keys())} |"
        table_line = F"| {' | '.join(['---'] * len(columns.keys()))} |"
        table_rows = [F"| {' | '.join(row)} |" for row in values]
        table = F"\n{table_header}\n{table_line}\n" + "\n".join(table_rows)

        return table




