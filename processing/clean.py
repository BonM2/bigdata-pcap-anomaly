"""Clean soft errors and split records into three disjoint quality outputs."""

from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F

from processing.config import ProcessingConfig, RATES, WINDOW_FLAGS, load_contract
from processing.transform import transform_flows
from processing.validation import validate_flows


@dataclass
class CleanResult:
    annotated: DataFrame
    valid: DataFrame
    quarantined: DataFrame
    rejected: DataFrame

    def counts(self):
        counts = {r["quality_status"]: r["count"] for r in self.annotated.groupBy("quality_status").count().collect()}
        return {
            "input_records": sum(counts.values()),
            "valid_records": counts.get("VALID", 0),
            "quarantined_records": counts.get("QUARANTINED", 0),
            "rejected_records": counts.get("REJECTED", 0),
        }


def clean_flows(raw, config=None, contract=None):
    """Preserve duplicates: no stable observation key exists in these CSVs.

    All annotated/quarantined/rejected rows retain raw evidence. The valid output
    has exactly 22 core fields plus the documented lineage and quality metadata.
    """
    config = config or ProcessingConfig.from_env()
    contract = contract or load_contract(config.schema_path)
    frame = validate_flows(transform_flows(raw, config, contract), contract)
    return clean_classified_flows(frame, contract)


def clean_classified_flows(classified, contract=None):
    """Clean an already transformed/validated frame without reclassifying rows."""
    contract = contract or load_contract()
    frame = classified.withColumn("quality_status_before", F.col("quality_status"))
    for name in RATES:
        value = F.col(name)
        invalid = value.isNull() | F.isnan(value) | (F.abs(value) == float("inf")) | (value < 0)
        frame = (frame.withColumn(f"{name}_invalid", invalid)
                 .withColumn(name, F.when(~F.col(f"{name}_invalid"), value)))
    for name, flag in WINDOW_FLAGS.items():
        frame = (frame.withColumn(flag, F.coalesce(F.col(name) == -1, F.lit(False)))
                 .withColumn(name, F.when(~F.col(flag), F.col(name))))
    metadata = [
        "source_file", "source_dataset", "batch_id", "pipeline_version", "schema_version",
        "processed_at", "quality_status", "quality_reason_codes",
        "flow_bytes_s_invalid", "flow_packets_s_invalid", "init_win_fwd_missing", "init_win_bwd_missing",
    ]
    return CleanResult(
        annotated=frame,
        valid=frame.filter(F.col("quality_status") == "VALID").select(*contract["cleaned_core_field_names"], *metadata),
        quarantined=frame.filter(F.col("quality_status") == "QUARANTINED"),
        rejected=frame.filter(F.col("quality_status") == "REJECTED"),
    )
