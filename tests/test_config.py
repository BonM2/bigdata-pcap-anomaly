import unittest

from processing.config import ProcessingConfig, load_contract


class ConfigTests(unittest.TestCase):
    def test_env_paths_and_secret_redaction(self):
        config = ProcessingConfig.from_env({"MINIO_BUCKET": "test-bucket", "MINIO_ACCESS_KEY": "access-example",
                                            "MINIO_SECRET_KEY": "secret-example", "BATCH_ID": "batch-1"})
        config.validate()
        self.assertEqual(config.input_path, "s3a://test-bucket/raw/cicids2017/*.csv")
        self.assertNotIn("secret-example", repr(config))
        self.assertNotIn("access-example", repr(config))
        self.assertEqual(len(load_contract()["fields"]), 79)

    def test_credentials_and_positive_resources_required(self):
        with self.assertRaisesRegex(ValueError, "MINIO_ACCESS_KEY"):
            ProcessingConfig.from_env({}).validate()
        with self.assertRaisesRegex(ValueError, "positive"):
            ProcessingConfig(shuffle_partitions=0).validate(require_minio=False)
        with self.assertRaisesRegex(ValueError, "positive"):
            ProcessingConfig(cache_batch_size=0).validate(require_minio=False)
        with self.assertRaisesRegex(ValueError, "SPARK_STORAGE_LEVEL"):
            ProcessingConfig(storage_level="INVALID").validate(require_minio=False)
