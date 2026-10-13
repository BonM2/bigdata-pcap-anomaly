import unittest

from processing.config import ProcessingConfig, create_spark_session, load_contract, raw_schema


class SparkCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.config = ProcessingConfig(batch_id="unit-tests")
        cls.contract = load_contract()
        cls.spark = create_spark_session(cls.config, enable_minio=False)
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.spark.conf.set("spark.sql.ansi.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def raw(self, overrides):
        from pyspark.sql.types import StringType, StructField, StructType
        names = [f["name"] for f in self.contract["fields"]]
        schema = StructType(raw_schema(self.contract).fields + [
            StructField("source_file", StringType()), StructField("_corrupt_record", StringType())])
        rows = []
        for changes in overrides:
            record = dict.fromkeys(names, "0")
            record.update(label="BENIGN", source_file="test.csv", _corrupt_record=None)
            record.update(changes)
            rows.append(tuple(record.get(n) for n in schema.fieldNames()))
        return self.spark.createDataFrame(rows, schema)
