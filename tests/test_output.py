import unittest
from dataclasses import replace

from processing.config import ProcessingConfig
from processing.output import output_plan


class OutputPathTests(unittest.TestCase):
    def test_batch_isolated_and_unsafe_paths_rejected(self):
        config = ProcessingConfig(batch_id="test")
        self.assertTrue(all(path.endswith("/test") for path in output_plan(config).values()))
        for path in ("s3a://bigdata-network/raw/elsewhere", "s3a://bigdata-network/",
                     "s3a://bigdata-network/raw/cicids2017/result", "s3a://bigdata-network/processed/../raw/x",
                     "s3a://bigdata-network/processed/%2e%2e/raw/x", "s3a://bigdata-network/out/*"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                output_plan(replace(config, cleaned_path=path))
        with self.assertRaisesRegex(ValueError, "overlap"):
            output_plan(replace(config, cleaned_path=config.aggregates_path))
        with self.assertRaisesRegex(ValueError, "overlap"):
            output_plan(replace(config, input_path="s3a://bigdata-network/cleaned/*.csv"))
